package main

import (
	"log"
	"sync"
)

// Server ties everything together: the data (Store), the persistence log (AOF),
// and the lock that keeps writes in order.
type Server struct {
	store *Store
	aof   *AOF // nil when persistence is turned off (and while loading the AOF)

	// writeMu makes write commands run one at a time.
	//
	// Why? The AOF must record writes in the SAME order they were applied to
	// the store. Without this, two clients could do:
	//
	//   A: SET k 1  (applied first)      B: SET k 2  (applied second)
	//   B: logs "SET k 2"                A: logs "SET k 1"   <- logged in the wrong order
	//
	// The store ends with k=2, but replaying the file after a restart gives k=1.
	// Holding writeMu across "apply + log" makes each write one atomic step.
	// Reads (GET, TTL, EXISTS...) don't take writeMu, so they still run in parallel.
	// Real Redis gets the same guarantee by running every command on one thread.
	writeMu sync.Mutex
}

func NewServer(store *Store) *Server {
	return &Server{store: store}
}

// propagate records a write that just changed the data.
// Today it goes to the AOF; on Day 4 the same call will also send it to replicas.
//
// Commands pass the *normalised* form of the write, e.g. "SET k v EX 10"
// becomes "SET k v PXAT <absolute ms>", so replaying it later gives the
// exact same result.
func (srv *Server) propagate(args ...string) {
	if srv.aof == nil {
		return
	}
	if err := srv.aof.Append(args); err != nil {
		// Real Redis stops accepting writes when it can't persist them.
		// We keep it simple and log loudly.
		log.Printf("AOF write failed: %v", err)
	}
}
