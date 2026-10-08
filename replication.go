package main

// Replication: one leader, any number of followers.
//
// LEADER SIDE
//   A follower connects like a normal client and sends:
//     PING                         -> +PONG        ("are you alive?")
//     REPLCONF listening-port 6381 -> +OK          ("this is my port")
//     PSYNC <replid> <offset>      ("I have history <replid> up to byte <offset>")
//   The leader answers one of two ways:
//     +CONTINUE <replid>            PARTIAL resync: "I still have the bytes you
//                                   missed in my backlog, here they are"
//     +FULLRESYNC <replid> <offset> FULL resync: "start over", followed by a
//                                   snapshot of the whole database
//   then streams every new write, plus a PING every heartbeatInterval so the
//   follower can tell "no writes happening" apart from "leader is dead".
//
// FOLLOWER SIDE
//   Connects, does the handshake, applies the snapshot (if full) and then the
//   live stream. It keeps its own backlog of the stream too, so that if it is
//   ever promoted to leader, other followers can partial-resync from it.
//   If the link breaks: reconnect. If the leader stays gone: failover.go.
//
// OFFSETS
//   Leader and followers count the same stream bytes. Equal offsets = fully
//   caught up. Offsets are also how a reconnecting follower says exactly
//   what it's missing.

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Timings. They're variables (not constants) so tests can shrink them.
var (
	heartbeatInterval   = time.Second
	leaderReadTimeout   = 5 * time.Second // follower: no data for this long = link is dead
	replicaWriteTimeout = 10 * time.Second
	retryInterval       = time.Second

	// quorumTimeout: a follower counts as "reachable" if we got an ACK from it
	// within this long. It MUST be shorter than failoverAfter (3s): a leader
	// cut off from the majority has to stop accepting writes BEFORE the other
	// side can elect a new leader, so the two never accept writes at once.
	quorumTimeout = 2 * time.Second
)

const replicaBufferSize = 10_000 // queued writes per follower before we give up on it

func newReplID() string {
	b := make([]byte, 20)
	rand.Read(b)
	return hex.EncodeToString(b) // 40 hex characters, like real Redis
}

// ======================= LEADER SIDE =======================

// replica is one connected follower, as seen by the leader.
type replica struct {
	conn net.Conn
	addr string // ip:listening-port, for INFO

	// out is a queue of bytes to send. A dedicated goroutine (writeLoop)
	// sends them, so a slow follower never makes the leader itself wait.
	out       chan []byte
	closeOnce sync.Once

	// Updated every time the follower sends "REPLCONF ACK <offset>".
	lastAck   atomic.Int64 // unix nanos
	ackOffset atomic.Int64 // how much of the stream the follower has applied
}

// serveReplica turns a client connection into a replication stream.
func (srv *Server) serveReplica(conn net.Conn, reader *RespReader, port string, reqID string, reqOffset int64) {
	r, partial := srv.attachReplica(conn, port, reqID, reqOffset)
	if partial {
		log.Printf("replica %s connected: partial resync from offset %d", r.addr, reqOffset)
	} else {
		log.Printf("replica %s connected: full resync", r.addr)
	}

	// The follower sends "REPLCONF ACK <offset>" regularly. Each one proves
	// it's alive and reachable, and says how far it has got.
	// Reading also tells us when it disconnects (Read returns an error).
	for {
		v, err := reader.Read()
		if err != nil {
			break
		}
		if commandName(v) == "REPLCONF" && len(v.Array) == 3 && strings.EqualFold(v.Array[1].Str, "ACK") {
			if off, err := strconv.ParseInt(v.Array[2].Str, 10, 64); err == nil {
				r.ackOffset.Store(off)
			}
			r.lastAck.Store(time.Now().UnixNano())
		}
	}
	srv.removeReplica(r)
	log.Printf("replica %s disconnected", r.addr)
}

// attachReplica answers PSYNC (partial or full) and registers the follower
// for live writes. Returns true if it was a partial resync.
//
// Everything happens under writeMu, so no write can sneak in between
// "decide what the follower is missing" and "join the live stream".
func (srv *Server) attachReplica(conn net.Conn, port, reqID string, reqOffset int64) (*replica, bool) {
	srv.writeMu.Lock()
	defer srv.writeMu.Unlock()

	host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	r := &replica{
		conn: conn,
		addr: net.JoinHostPort(host, port),
		out:  make(chan []byte, replicaBufferSize),
	}
	r.lastAck.Store(time.Now().UnixNano()) // it just talked to us: counts as reachable

	missing, partial := srv.continueFromLocked(reqID, reqOffset)
	if partial {
		r.out <- []byte(fmt.Sprintf("+CONTINUE %s\r\n", srv.getReplID()))
		if len(missing) > 0 {
			r.out <- missing
		}
		srv.syncPartial.Add(1)
	} else {
		r.out <- []byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", srv.getReplID(), srv.replOffset.Load()))
		r.out <- Bulk(string(srv.store.SnapshotRESP())).Marshal()
		srv.syncFull.Add(1)
	}

	srv.replicas[r] = struct{}{}
	go r.writeLoop(srv)
	return r, partial
}

// continueFromLocked decides whether a follower can do a partial resync.
// It can if (1) it's on our history (current, or the one we inherited at
// failover), and (2) the bytes it's missing are still in our backlog.
// Caller must hold writeMu.
func (srv *Server) continueFromLocked(reqID string, reqOffset int64) ([]byte, bool) {
	if reqOffset < 0 {
		return nil, false
	}
	sameHistory := reqID == srv.getReplID() ||
		(srv.replID2 != "" && reqID == srv.replID2 && reqOffset <= srv.secondOffset)
	if !sameHistory {
		return nil, false
	}
	return srv.backlog.readFrom(reqOffset)
}

// hasQuorumLocked reports whether this leader can currently reach a majority
// of the cluster (itself + followers that ACKed recently). Caller must hold writeMu.
//
// A leader that can't reach a majority may have been cut off by a network
// split, and the other side may be electing a new leader right now. If we
// kept accepting writes, both sides would accept different writes and one
// side's writes would be thrown away when the network heals. So we refuse
// writes instead: unavailable for a while, but never losing acknowledged data.
func (srv *Server) hasQuorumLocked() bool {
	if len(srv.peers) == 0 {
		return true // standalone (no cluster): nothing to split from
	}
	reachable := 1 // ourselves
	for r := range srv.replicas {
		if time.Since(time.Unix(0, r.lastAck.Load())) < quorumTimeout {
			reachable++
		}
	}
	return reachable >= srv.majority()
}

// writeLoop sends queued bytes to the follower until the queue is closed.
func (r *replica) writeLoop(srv *Server) {
	for b := range r.out {
		r.conn.SetWriteDeadline(time.Now().Add(replicaWriteTimeout))
		if _, err := r.conn.Write(b); err != nil {
			srv.removeReplica(r)
			return
		}
	}
}

// replicateLocked adds a write to the backlog and queues it for every
// follower. Caller must hold writeMu.
func (srv *Server) replicateLocked(cmd []byte) {
	srv.backlog.write(cmd)
	srv.replOffset.Add(int64(len(cmd)))
	for r := range srv.replicas {
		select {
		case r.out <- cmd:
		default:
			// Queue full: this follower can't keep up. Waiting for it would
			// slow down every client, so we drop it. It will reconnect and
			// resync. (Real Redis: client-output-buffer-limit.)
			log.Printf("replica %s too slow, disconnecting", r.addr)
			srv.removeReplicaLocked(r)
		}
	}
}

func (srv *Server) removeReplica(r *replica) {
	srv.writeMu.Lock()
	defer srv.writeMu.Unlock()
	srv.removeReplicaLocked(r)
}

// removeReplicaLocked unregisters a follower and closes its connection.
// Caller must hold writeMu. Safe to call more than once.
func (srv *Server) removeReplicaLocked(r *replica) {
	delete(srv.replicas, r)
	r.closeOnce.Do(func() {
		close(r.out) // stops writeLoop
		r.conn.Close()
	})
}

// startLeaderLoops starts the leader's background work (heartbeats, and
// watching for a newer leader) if it isn't already running.
func (srv *Server) startLeaderLoops() {
	if srv.leaderLoopOn.CompareAndSwap(false, true) {
		go func() {
			srv.heartbeatLoop()
			srv.leaderLoopOn.Store(false)
		}()
	}
	if len(srv.peers) > 0 && srv.watchLoopOn.CompareAndSwap(false, true) {
		go func() {
			srv.leaderWatchLoop() // in failover.go
			srv.watchLoopOn.Store(false)
		}()
	}
}

// heartbeatLoop sends PING to all followers regularly, while we're leader.
func (srv *Server) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	ping := ArrayOf(Bulk("PING")).Marshal()
	for range ticker.C {
		if srv.stopped.Load() || srv.isReplica.Load() {
			return // we stopped being leader
		}
		srv.writeMu.Lock()
		if len(srv.replicas) > 0 {
			srv.replicateLocked(ping)
		}
		srv.writeMu.Unlock()
	}
}

// ======================= FOLLOWER SIDE =======================

// startReplicaLink starts the follower's connection loop if it isn't running.
func (srv *Server) startReplicaLink() {
	if !srv.linkRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		srv.runReplicaLink()
		srv.linkRunning.Store(false)
		// If we were turned back into a follower while exiting, start again.
		if srv.isReplica.Load() && !srv.stopped.Load() {
			srv.startReplicaLink()
		}
	}()
}

// runReplicaLink keeps this follower connected to its leader. If the leader
// stays unreachable and we know our peers, it triggers a failover.
func (srv *Server) runReplicaLink() {
	srv.markContact()
	for srv.isReplica.Load() && !srv.stopped.Load() {
		err := srv.syncWithLeader()
		srv.linkUp.Store(false)
		if !srv.isReplica.Load() || srv.stopped.Load() {
			return
		}
		log.Printf("link to leader %s lost: %v", srv.getLeaderAddr(), err)

		if len(srv.peers) > 0 && srv.sinceContact() >= failoverAfter {
			srv.handleLeaderDown() // failover.go: find a new leader, or become it
			if !srv.isReplica.Load() {
				return // we were promoted
			}
		}
		time.Sleep(retryInterval)
	}
}

func (srv *Server) setLeaderConn(c net.Conn) {
	srv.leaderConnMu.Lock()
	srv.leaderConn = c
	srv.leaderConnMu.Unlock()
}

// closeLeaderConn breaks the current link (e.g. to switch to a new leader).
func (srv *Server) closeLeaderConn() {
	srv.leaderConnMu.Lock()
	if srv.leaderConn != nil {
		srv.leaderConn.Close()
	}
	srv.leaderConnMu.Unlock()
}

// syncWithLeader runs one connection to the leader: handshake, resync,
// then apply the live stream until something breaks.
func (srv *Server) syncWithLeader() error {
	addr := srv.getLeaderAddr()
	conn, err := net.DialTimeout("tcp", addr, peerTimeout)
	if err != nil {
		return err
	}
	srv.setLeaderConn(conn)
	defer func() {
		srv.setLeaderConn(nil)
		conn.Close()
	}()

	reader := NewRespReader(conn)

	// send writes one command; expect reads one reply and checks it.
	send := func(parts ...string) error {
		vals := make([]Value, len(parts))
		for i, p := range parts {
			vals[i] = Bulk(p)
		}
		conn.SetWriteDeadline(time.Now().Add(leaderReadTimeout))
		_, err := conn.Write(ArrayOf(vals...).Marshal())
		return err
	}
	expect := func(prefixes ...string) (string, error) {
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		v, err := reader.Read()
		if err != nil {
			return "", err
		}
		for _, p := range prefixes {
			if v.Type == SimpleString && strings.HasPrefix(v.Str, p) {
				return v.Str, nil
			}
		}
		return "", fmt.Errorf("handshake: expected %v, got %q", prefixes, v.Str)
	}

	// ---- 1. Handshake ----
	if err := send("PING"); err != nil {
		return err
	}
	if _, err := expect("PONG"); err != nil {
		return err
	}
	if err := send("REPLCONF", "listening-port", strconv.Itoa(srv.port)); err != nil {
		return err
	}
	if _, err := expect("OK"); err != nil {
		return err
	}
	// Tell the leader exactly what we already have.
	myID, myOffset := srv.getReplID(), srv.replOffset.Load()
	if err := send("PSYNC", myID, strconv.FormatInt(myOffset, 10)); err != nil {
		return err
	}
	line, err := expect("FULLRESYNC", "CONTINUE")
	if err != nil {
		return err
	}
	fields := strings.Fields(line)

	// ---- 2. Resync ----
	if fields[0] == "CONTINUE" {
		// Partial: keep our data, offset and backlog; the missing bytes
		// arrive as part of the stream below. The leader may have a new
		// history id (after a failover) that continues ours -- adopt it.
		if len(fields) == 2 {
			srv.setReplID(fields[1])
		}
		log.Printf("partial resync with %s from offset %d", addr, myOffset)
	} else {
		// Full: "FULLRESYNC <replid> <offset>", then the snapshot.
		if len(fields) != 3 {
			return fmt.Errorf("bad FULLRESYNC line %q", line)
		}
		leaderOffset, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("bad offset in %q", line)
		}
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		snap, err := reader.Read()
		if err != nil {
			return err
		}
		if snap.Type != BulkString {
			return errors.New("expected snapshot as a bulk string")
		}
		n, err := srv.loadSnapshot(snap.Str, leaderOffset)
		if err != nil {
			return err
		}
		srv.setReplID(fields[1])
		log.Printf("full resync with %s: %d keys loaded, offset %d", addr, n, leaderOffset)
	}
	srv.linkUp.Store(true)
	srv.markContact()

	// Tell the leader regularly how far we've got ("REPLCONF ACK <offset>").
	// This is how the leader knows we're reachable (its quorum check).
	// Only this goroutine writes to conn now; the loop below only reads.
	done := make(chan struct{})
	defer close(done)
	go srv.sendAcks(conn, done)

	// ---- 3. Apply the live stream ----
	for {
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		cmd, err := reader.Read()
		if err != nil {
			return err // includes the timeout: no heartbeat for a while = leader gone
		}
		srv.markContact()

		// internal=true: bypasses the READONLY rule; the leader is allowed to write.
		if reply := execute(srv, cmd, true); reply.Type == Error {
			log.Printf("replication: command from leader failed: %s", reply.Str)
		}

		// Record it in our own backlog and offset. Re-encoding gives exactly
		// the bytes the leader sent, so our offset matches the leader's.
		raw := cmd.Marshal()
		srv.writeMu.Lock()
		srv.backlog.write(raw)
		srv.replOffset.Add(int64(len(raw)))
		srv.writeMu.Unlock()
	}
}

// sendAcks sends "REPLCONF ACK <offset>" every heartbeatInterval until done.
func (srv *Server) sendAcks(conn net.Conn, done chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		ack := ArrayOf(Bulk("REPLCONF"), Bulk("ACK"), Bulk(strconv.FormatInt(srv.replOffset.Load(), 10))).Marshal()
		conn.SetWriteDeadline(time.Now().Add(leaderReadTimeout))
		if _, err := conn.Write(ack); err != nil {
			return
		}
		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}

// loadSnapshot replaces all our data with the leader's snapshot and resets
// our replication position to the leader's offset. Returns how many keys
// were loaded.
func (srv *Server) loadSnapshot(payload string, offset int64) (int, error) {
	srv.writeMu.Lock()
	srv.store.Flush()
	srv.backlog.reset(offset)
	srv.replOffset.Store(offset)
	if srv.aof != nil {
		// Our old history is irrelevant now; the snapshot (written to the
		// AOF below as it's applied) becomes the new history.
		if err := srv.aof.Reset(); err != nil {
			srv.writeMu.Unlock()
			return 0, err
		}
	}
	srv.writeMu.Unlock()

	reader := NewRespReader(strings.NewReader(payload))
	count := 0
	for {
		cmd, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		if err != nil {
			return count, fmt.Errorf("bad snapshot: %w", err)
		}
		if reply := execute(srv, cmd, true); reply.Type == Error {
			return count, fmt.Errorf("snapshot command failed: %s", reply.Str)
		}
		count++
	}
}

// ======================= INFO =======================

// replicationInfo builds the text for "INFO replication".
func (srv *Server) replicationInfo() string {
	var b strings.Builder
	b.WriteString("# Replication\r\n")
	if srv.isReplica.Load() {
		host, port, _ := net.SplitHostPort(srv.getLeaderAddr())
		status := "down"
		if srv.linkUp.Load() {
			status = "up"
		}
		fmt.Fprintf(&b, "role:slave\r\nmaster_host:%s\r\nmaster_port:%s\r\n", host, port)
		fmt.Fprintf(&b, "master_link_status:%s\r\n", status)
		fmt.Fprintf(&b, "slave_repl_offset:%d\r\n", srv.replOffset.Load())
	} else {
		srv.writeMu.Lock()
		fmt.Fprintf(&b, "role:master\r\nconnected_slaves:%d\r\n", len(srv.replicas))
		i := 0
		for r := range srv.replicas {
			host, port, _ := net.SplitHostPort(r.addr)
			lag := int(time.Since(time.Unix(0, r.lastAck.Load())).Seconds())
			fmt.Fprintf(&b, "slave%d:ip=%s,port=%s,offset=%d,lag=%d\r\n", i, host, port, r.ackOffset.Load(), lag)
			i++
		}
		quorum := srv.hasQuorumLocked()
		srv.writeMu.Unlock()
		if len(srv.peers) > 0 {
			fmt.Fprintf(&b, "cluster_nodes:%d\r\nwrites_allowed:%v\r\n", len(srv.peers)+1, quorum)
		}
		fmt.Fprintf(&b, "sync_full:%d\r\nsync_partial_ok:%d\r\n", srv.syncFull.Load(), srv.syncPartial.Load())
	}
	fmt.Fprintf(&b, "master_replid:%s\r\n", srv.getReplID())
	srv.writeMu.Lock()
	id2 := srv.replID2
	off2 := srv.secondOffset
	srv.writeMu.Unlock()
	if id2 != "" {
		fmt.Fprintf(&b, "master_replid2:%s\r\nsecond_repl_offset:%d\r\n", id2, off2)
	}
	fmt.Fprintf(&b, "master_repl_offset:%d\r\n", srv.replOffset.Load())
	fmt.Fprintf(&b, "failover_epoch:%d\r\n", srv.epoch.Load())
	return b.String()
}
