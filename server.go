package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// Server ties everything together: the data (Store), persistence (AOF),
// replication, and the lock that keeps writes in order.
type Server struct {
	store *Store
	aof   *AOF // nil when persistence is turned off (and while loading the AOF)
	port  int

	// writeMu makes write commands run one at a time, so the AOF and the
	// replicas receive writes in exactly the order they were applied.
	// (See Day 3 notes.) It also protects the replicas map below.
	writeMu sync.Mutex

	// ----- Replication -----

	// isReplica is true when this server is a follower. Followers reject
	// writes from normal clients and only apply writes sent by their leader.
	isReplica  atomic.Bool
	leaderAddr string // "host:port" of our leader (followers only)

	// replID identifies a leader's history (a random 40-char id), and
	// replOffset counts how many bytes of writes that history contains.
	// Together they let a follower say "I have everything up to byte N of
	// history X" -- the basis of catch-up after a disconnect (Day 5).
	replID     atomic.Value // string; read by INFO while the replication link may change it
	replOffset atomic.Int64

	replicas map[*replica]struct{} // connected followers (leaders only), guarded by writeMu
	linkUp   atomic.Bool           // follower: is the connection to the leader healthy?
}

func NewServer(store *Store) *Server {
	srv := &Server{
		store:    store,
		replicas: make(map[*replica]struct{}),
	}
	srv.setReplID(newReplID())
	return srv
}

func (srv *Server) getReplID() string   { return srv.replID.Load().(string) }
func (srv *Server) setReplID(id string) { srv.replID.Store(id) }

// propagate records a write that just changed the data: it goes to the AOF
// and, if we're a leader, to every connected follower.
//
// It's always called while writeMu is held (from inside a write command),
// so every destination receives writes in the same order.
//
// Commands pass the *normalised* form of the write, e.g. "SET k v EX 10"
// becomes "SET k v PXAT <absolute ms>", so replaying it anywhere, at any
// time, gives the exact same result.
func (srv *Server) propagate(args ...string) {
	vals := make([]Value, len(args))
	for i, a := range args {
		vals[i] = Bulk(a)
	}
	cmd := ArrayOf(vals...).Marshal() // encode once, send everywhere

	if srv.aof != nil {
		if err := srv.aof.Append(cmd); err != nil {
			// Real Redis stops accepting writes when it can't persist them.
			// We keep it simple and log loudly.
			log.Printf("AOF write failed: %v", err)
		}
	}
	if !srv.isReplica.Load() {
		srv.replicateLocked(cmd)
	}
}

// Serve accepts clients on ln until ln is closed.
func (srv *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			fmt.Println("accept error:", err)
			continue // one bad connection shouldn't kill the server
		}
		// Serve each client in its own goroutine so many can connect at once.
		go srv.handleConn(conn)
	}
}

// handleConn serves one client until it disconnects.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := NewRespReader(conn)
	replicaPort := "" // set if this connection says it's a follower (REPLCONF listening-port)

	for {
		// Read exactly one full command, however the bytes arrived over TCP.
		cmd, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// Malformed input: tell the client, then drop the connection
				// (we can't know where the next command starts).
				conn.Write(Err("ERR Protocol error: " + err.Error()).Marshal())
			}
			return
		}

		// Two replication commands need the connection itself, so they're
		// handled here instead of in the normal command table.
		switch commandName(cmd) {
		case "REPLCONF":
			// A follower introducing itself: "REPLCONF listening-port 6381".
			if len(cmd.Array) == 3 && strings.EqualFold(cmd.Array[1].Str, "listening-port") {
				replicaPort = cmd.Array[2].Str
			}
			conn.Write(OK().Marshal())
			continue
		case "PSYNC":
			// A follower asking for the data. From now on this connection
			// is a replication stream, not a normal client.
			if srv.isReplica.Load() {
				conn.Write(Err("ERR this server is a replica and cannot serve PSYNC").Marshal())
				continue
			}
			srv.serveReplica(conn, reader, replicaPort)
			return
		}

		reply := dispatch(srv, cmd)

		if _, err := conn.Write(reply.Marshal()); err != nil {
			return
		}
	}
}

// commandName returns the upper-cased first word of a command, or "".
func commandName(v Value) string {
	if v.Type != Array || len(v.Array) == 0 || v.Array[0].Type != BulkString {
		return ""
	}
	return strings.ToUpper(v.Array[0].Str)
}
