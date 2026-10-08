package main

// WAIT numreplicas timeout-ms
//
// Normally the leader replies OK to a write BEFORE followers have it
// (asynchronous replication: fast, but the last few writes can be lost if the
// leader crashes at the wrong moment). For writes that really matter, a
// client can follow up with WAIT:
//
//	SET balance 500   -> OK
//	WAIT 2 1000       -> 2
//
// "Don't answer until at least 2 followers have everything written so far,
// but give up after 1000 ms." The reply is how many followers have it.
//
// Why it's safe: if a write reached a MAJORITY of the cluster, any future
// leader must have it too. A candidate needs a majority of votes, any two
// majorities share a node, and that node refuses to vote for a candidate
// with less data than itself (handleVote, rule 5).

import (
	"strconv"
	"time"
)

// cmdWait implements WAIT.
func cmdWait(srv *Server, args []string) Value {
	if len(args) != 2 {
		return wrongArgs("wait")
	}
	want, err1 := strconv.Atoi(args[0])
	timeoutMs, err2 := strconv.ParseInt(args[1], 10, 64)
	if err1 != nil || err2 != nil || want < 0 || timeoutMs < 0 {
		return errNotInt
	}
	if srv.isReplica.Load() {
		return Err("ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.")
	}

	// Everything written up to now must reach the followers.
	target := srv.replOffset.Load()

	if n := srv.countAcked(target); n >= want {
		return Int(int64(n))
	}

	// Followers normally ACK once per heartbeat. Ask them to ACK right now,
	// so WAIT takes one network round trip instead of up to a second.
	srv.writeMu.Lock()
	if len(srv.replicas) > 0 && !srv.isReplica.Load() {
		srv.replicateLocked(ArrayOf(Bulk("REPLCONF"), Bulk("GETACK"), Bulk("*")).Marshal())
	}
	srv.writeMu.Unlock()

	// timeout 0 = wait forever (same as Redis).
	var deadline <-chan time.Time
	if timeoutMs > 0 {
		timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
		defer timer.Stop()
		deadline = timer.C
	}

	for {
		// Grab the channel BEFORE counting, so an ACK that arrives right
		// after the count still wakes us up (no missed wake-ups).
		ch := srv.ackChan()
		n := srv.countAcked(target)
		if n >= want {
			return Int(int64(n))
		}
		select {
		case <-ch: // some follower ACKed: count again
		case <-deadline:
			return Int(int64(srv.countAcked(target)))
		}
	}
}

// countAcked returns how many followers have confirmed (via ACK) that they
// have applied the stream up to at least offset.
func (srv *Server) countAcked(offset int64) int {
	srv.writeMu.Lock()
	defer srv.writeMu.Unlock()
	n := 0
	for r := range srv.replicas {
		if r.ackOffset.Load() >= offset {
			n++
		}
	}
	return n
}

// ackChan returns a channel that will be closed at the next follower ACK.
func (srv *Server) ackChan() chan struct{} {
	srv.ackMu.Lock()
	defer srv.ackMu.Unlock()
	return srv.ackCh
}

// notifyAck wakes up everyone waiting in WAIT. Closing a channel wakes ALL
// goroutines reading from it at once; then we swap in a fresh one for next time.
func (srv *Server) notifyAck() {
	srv.ackMu.Lock()
	defer srv.ackMu.Unlock()
	close(srv.ackCh)
	srv.ackCh = make(chan struct{})
}
