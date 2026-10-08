package server

import (
	"strconv"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

// WAIT numreplicas timeout blocks until numreplicas followers have
// acknowledged every write made so far, or the timeout (ms, 0 = forever)
// expires, and returns how many did. A write acknowledged by a majority
// survives any failover, since the winner of an election must have it.
func cmdWait(srv *Server, args []string) resp.Value {
	if len(args) != 2 {
		return wrongArgs("wait")
	}
	want, err1 := strconv.Atoi(args[0])
	timeoutMs, err2 := strconv.ParseInt(args[1], 10, 64)
	if err1 != nil || err2 != nil || want < 0 || timeoutMs < 0 {
		return errNotInt
	}
	if srv.isReplica.Load() {
		return resp.Err("ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.")
	}

	target := srv.replOffset.Load()

	if n := srv.countAcked(target); n >= want {
		return resp.Int(int64(n))
	}

	// Ask followers to ACK now rather than at their next heartbeat.
	srv.writeMu.Lock()
	if len(srv.replicas) > 0 && !srv.isReplica.Load() {
		srv.replicateLocked(resp.Command("REPLCONF", "GETACK", "*"))
	}
	srv.writeMu.Unlock()

	var deadline <-chan time.Time
	if timeoutMs > 0 {
		timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
		defer timer.Stop()
		deadline = timer.C
	}

	for {
		ch := srv.ackChan() // taken before counting so no ACK is missed
		n := srv.countAcked(target)
		if n >= want {
			return resp.Int(int64(n))
		}
		select {
		case <-ch:
		case <-deadline:
			return resp.Int(int64(srv.countAcked(target)))
		}
	}
}

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

func (srv *Server) ackChan() chan struct{} {
	srv.ackMu.Lock()
	defer srv.ackMu.Unlock()
	return srv.ackCh
}

// notifyAck wakes every WAIT by closing the current channel.
func (srv *Server) notifyAck() {
	srv.ackMu.Lock()
	defer srv.ackMu.Unlock()
	close(srv.ackCh)
	srv.ackCh = make(chan struct{})
}
