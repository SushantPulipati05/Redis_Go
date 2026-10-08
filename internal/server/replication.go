package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

// Timings are variables so tests can shorten them.
var (
	heartbeatInterval   = time.Second
	leaderReadTimeout   = 5 * time.Second
	replicaWriteTimeout = 10 * time.Second
	retryInterval       = time.Second

	// Must stay below failoverAfter, so that a leader cut off from the
	// majority stops accepting writes before a new leader can be elected.
	quorumTimeout = 2 * time.Second
)

const replicaBufferSize = 10_000

func newReplID() string {
	b := make([]byte, 20)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// replica is a connected follower, as seen by the leader. Writes are queued
// on out and sent by writeLoop, so a slow follower never blocks the leader.
type replica struct {
	conn      net.Conn
	addr      string
	out       chan []byte
	closeOnce sync.Once
	lastAck   atomic.Int64 // unix nanos
	ackOffset atomic.Int64
}

// serveReplica runs for the lifetime of a follower's connection: it answers
// PSYNC, then reads the follower's ACKs until it disconnects.
func (srv *Server) serveReplica(conn net.Conn, reader *resp.Reader, port string, reqID string, reqOffset int64) {
	r, partial := srv.attachReplica(conn, port, reqID, reqOffset)
	if partial {
		log.Printf("replica %s connected: partial resync from offset %d", r.addr, reqOffset)
	} else {
		log.Printf("replica %s connected: full resync", r.addr)
	}

	for {
		v, err := reader.Read()
		if err != nil {
			break
		}
		if v.Name() == "REPLCONF" && len(v.Array) == 3 && strings.EqualFold(v.Array[1].Str, "ACK") {
			if off, err := strconv.ParseInt(v.Array[2].Str, 10, 64); err == nil {
				r.ackOffset.Store(off)
			}
			r.lastAck.Store(time.Now().UnixNano())
			srv.notifyAck()
		}
	}
	srv.removeReplica(r)
	log.Printf("replica %s disconnected", r.addr)
}

// attachReplica sends either the missing part of the stream (CONTINUE) or a
// full snapshot (FULLRESYNC), then registers the follower for live writes.
// Holding writeMu throughout means no write can fall between the two.
func (srv *Server) attachReplica(conn net.Conn, port, reqID string, reqOffset int64) (*replica, bool) {
	srv.writeMu.Lock()
	defer srv.writeMu.Unlock()

	host, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	r := &replica{
		conn: conn,
		addr: net.JoinHostPort(host, port),
		out:  make(chan []byte, replicaBufferSize),
	}
	r.lastAck.Store(time.Now().UnixNano())

	missing, partial := srv.continueFromLocked(reqID, reqOffset)
	if partial {
		r.out <- []byte(fmt.Sprintf("+CONTINUE %s\r\n", srv.getReplID()))
		if len(missing) > 0 {
			r.out <- missing
		}
		srv.syncPartial.Add(1)
	} else {
		r.out <- []byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", srv.getReplID(), srv.replOffset.Load()))
		r.out <- resp.Bulk(string(srv.snapshot())).Marshal()
		srv.syncFull.Add(1)
	}

	srv.replicas[r] = struct{}{}
	go r.writeLoop(srv)
	return r, partial
}

// continueFromLocked returns what a follower is missing, if it is on our
// history (or the one we took over at failover) and the gap is still in the
// backlog. Caller must hold writeMu.
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

// hasQuorumLocked reports whether this node plus the followers that ACKed
// within quorumTimeout form a majority of the cluster. Caller must hold writeMu.
func (srv *Server) hasQuorumLocked() bool {
	if len(srv.peers) == 0 {
		return true
	}
	reachable := 1
	for r := range srv.replicas {
		if time.Since(time.Unix(0, r.lastAck.Load())) < quorumTimeout {
			reachable++
		}
	}
	return reachable >= srv.majority()
}

func (r *replica) writeLoop(srv *Server) {
	for b := range r.out {
		r.conn.SetWriteDeadline(time.Now().Add(replicaWriteTimeout))
		if _, err := r.conn.Write(b); err != nil {
			srv.removeReplica(r)
			return
		}
	}
}

// replicateLocked appends cmd to the backlog and queues it for every
// follower. A follower whose queue is full is dropped rather than allowed to
// block writers; it will reconnect and resync. Caller must hold writeMu.
func (srv *Server) replicateLocked(cmd []byte) {
	srv.backlog.write(cmd)
	srv.replOffset.Add(int64(len(cmd)))
	for r := range srv.replicas {
		select {
		case r.out <- cmd:
		default:
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

// Caller must hold writeMu.
func (srv *Server) removeReplicaLocked(r *replica) {
	delete(srv.replicas, r)
	r.closeOnce.Do(func() {
		close(r.out)
		r.conn.Close()
	})
}

func (srv *Server) startLeaderLoops() {
	if srv.leaderLoopOn.CompareAndSwap(false, true) {
		go func() {
			srv.heartbeatLoop()
			srv.leaderLoopOn.Store(false)
		}()
	}
	if len(srv.peers) > 0 && srv.watchLoopOn.CompareAndSwap(false, true) {
		go func() {
			srv.leaderWatchLoop()
			srv.watchLoopOn.Store(false)
		}()
	}
}

func (srv *Server) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	ping := resp.Command("PING")
	for range ticker.C {
		if srv.stopped.Load() || srv.isReplica.Load() {
			return
		}
		srv.writeMu.Lock()
		if len(srv.replicas) > 0 {
			srv.replicateLocked(ping)
		}
		srv.writeMu.Unlock()
	}
}

// snapshot encodes the whole keyspace as SET commands.
func (srv *Server) snapshot() []byte {
	var out []byte
	for _, e := range srv.store.Snapshot() {
		if e.ExpireAt.IsZero() {
			out = append(out, resp.Command("SET", e.Key, e.Value)...)
		} else {
			out = append(out, resp.Command("SET", e.Key, e.Value, "PXAT", unixMs(e.ExpireAt))...)
		}
	}
	return out
}
