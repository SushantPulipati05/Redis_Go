package main

// Replication: one leader, any number of followers.
//
// LEADER SIDE
//   A follower connects like a normal client and sends:
//     PING                         -> +PONG        ("are you alive?")
//     REPLCONF listening-port 6381 -> +OK          ("this is my port")
//     PSYNC ? -1                   -> +FULLRESYNC <replid> <offset>
//   then the leader sends:
//     1. a SNAPSHOT: the whole database as SET commands, wrapped in one bulk string
//     2. a live STREAM: every write from now on, the same bytes that go to the AOF
//   plus a PING every second as a heartbeat, so the follower can tell
//   "no writes happening" apart from "leader is dead".
//
// FOLLOWER SIDE
//   Connects, does the handshake, wipes its data, loads the snapshot, then
//   applies the stream forever. If the connection breaks, it waits a second
//   and starts over. Normal clients may read from a follower, but writes are
//   refused with READONLY -- only the leader decides what the data is.
//
// OFFSETS
//   The leader counts every byte it streams (replOffset). The follower counts
//   every byte it applies. If the two numbers match, the follower is fully
//   caught up. Day 5 uses this to resume after a disconnect without a full copy.

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
	"time"
)

const (
	heartbeatInterval   = time.Second
	leaderReadTimeout   = 5 * time.Second // follower: no data for this long = leader is gone
	replicaWriteTimeout = 10 * time.Second
	replicaBufferSize   = 10_000 // queued writes per follower before we give up on it
)

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
}

// serveReplica turns a client connection into a replication stream.
// Called from handleConn when the connection sends PSYNC.
func (srv *Server) serveReplica(conn net.Conn, reader *RespReader, port string) {
	r := srv.attachReplica(conn, port)
	log.Printf("replica %s connected, full sync sent", r.addr)

	// Keep reading so we notice when the follower disconnects.
	// (Day 5: followers will send "REPLCONF ACK <offset>" here.)
	for {
		if _, err := reader.Read(); err != nil {
			break
		}
	}
	srv.removeReplica(r)
	log.Printf("replica %s disconnected", r.addr)
}

// attachReplica sends the full sync and registers the follower for live writes.
//
// Everything happens under writeMu, so no write can sneak in between taking
// the snapshot and joining the stream. Every write is therefore either IN the
// snapshot or IN the stream -- never missing, never duplicated.
func (srv *Server) attachReplica(conn net.Conn, port string) *replica {
	srv.writeMu.Lock()
	defer srv.writeMu.Unlock()

	host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	r := &replica{
		conn: conn,
		addr: net.JoinHostPort(host, port),
		out:  make(chan []byte, replicaBufferSize),
	}

	header := fmt.Sprintf("+FULLRESYNC %s %d\r\n", srv.getReplID(), srv.replOffset.Load())
	snapshot := Bulk(string(srv.store.SnapshotRESP())).Marshal()
	r.out <- []byte(header)
	r.out <- snapshot

	srv.replicas[r] = struct{}{}
	go r.writeLoop(srv)
	return r
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

// replicateLocked queues a write for every follower and advances the offset.
// Caller must hold writeMu.
func (srv *Server) replicateLocked(cmd []byte) {
	srv.replOffset.Add(int64(len(cmd)))
	for r := range srv.replicas {
		select {
		case r.out <- cmd:
		default:
			// Queue full: this follower can't keep up. Waiting for it would
			// slow down every client, so we drop it. It will reconnect and
			// get a fresh full sync. (Real Redis: client-output-buffer-limit.)
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

// heartbeatLoop sends PING to all followers every second (leaders only).
// Followers use it to detect a dead leader even when there are no writes.
func (srv *Server) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	ping := ArrayOf(Bulk("PING")).Marshal()
	for range ticker.C {
		srv.writeMu.Lock()
		if !srv.isReplica.Load() && len(srv.replicas) > 0 {
			srv.replicateLocked(ping)
		}
		srv.writeMu.Unlock()
	}
}

// ======================= FOLLOWER SIDE =======================

// runReplicaLink keeps this follower connected to its leader, forever.
func (srv *Server) runReplicaLink() {
	for {
		err := srv.syncWithLeader()
		srv.linkUp.Store(false)
		log.Printf("link to leader %s lost: %v (retrying in 1s)", srv.leaderAddr, err)
		time.Sleep(time.Second)
	}
}

// countingReader counts every byte read through it, so the follower can
// work out its replication offset precisely.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// syncWithLeader runs one connection to the leader: handshake, full sync,
// then apply the live stream until something breaks.
func (srv *Server) syncWithLeader() error {
	conn, err := net.DialTimeout("tcp", srv.leaderAddr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	counter := &countingReader{r: conn}
	reader := NewRespReader(counter)

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
	expect := func(prefix string) (string, error) {
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		v, err := reader.Read()
		if err != nil {
			return "", err
		}
		if v.Type != SimpleString || !strings.HasPrefix(v.Str, prefix) {
			return "", fmt.Errorf("handshake: expected %q, got %q", prefix, v.Str)
		}
		return v.Str, nil
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
	if err := send("PSYNC", "?", "-1"); err != nil {
		return err
	}
	line, err := expect("FULLRESYNC")
	if err != nil {
		return err
	}
	// line = "FULLRESYNC <replid> <offset>"
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return fmt.Errorf("bad FULLRESYNC line %q", line)
	}
	leaderOffset, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return fmt.Errorf("bad offset in %q", line)
	}

	// ---- 2. Full sync: wipe our data and load the snapshot ----
	conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
	snap, err := reader.Read()
	if err != nil {
		return err
	}
	if snap.Type != BulkString {
		return errors.New("expected snapshot as a bulk string")
	}
	n, err := srv.loadSnapshot(snap.Str)
	if err != nil {
		return err
	}

	srv.setReplID(fields[1])
	srv.replOffset.Store(leaderOffset)
	srv.linkUp.Store(true)
	log.Printf("synced with leader %s: %d keys loaded, offset %d", srv.leaderAddr, n, leaderOffset)

	// ---- 3. Apply the live stream ----
	// Bytes consumed so far = the handshake + snapshot (not part of the offset).
	start := counter.n - int64(reader.Buffered())
	for {
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		cmd, err := reader.Read()
		if err != nil {
			return err // includes the timeout: no heartbeat for 5s = leader gone
		}
		// internal=true: bypasses the READONLY rule; the leader is allowed to write.
		if reply := execute(srv, cmd, true); reply.Type == Error {
			log.Printf("replication: command from leader failed: %s", reply.Str)
		}
		consumed := counter.n - int64(reader.Buffered()) - start
		srv.replOffset.Store(leaderOffset + consumed)
	}
}

// loadSnapshot replaces all our data with the leader's snapshot.
// Returns how many keys were loaded.
func (srv *Server) loadSnapshot(payload string) (int, error) {
	srv.writeMu.Lock()
	srv.store.Flush()
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
		host, port, _ := net.SplitHostPort(srv.leaderAddr)
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
			fmt.Fprintf(&b, "slave%d:%s\r\n", i, r.addr)
			i++
		}
		srv.writeMu.Unlock()
	}
	fmt.Fprintf(&b, "master_replid:%s\r\n", srv.getReplID())
	fmt.Fprintf(&b, "master_repl_offset:%d\r\n", srv.replOffset.Load())
	return b.String()
}
