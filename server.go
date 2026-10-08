package main

import (
	"bufio"
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

// Server ties everything together: the data (Store), persistence (AOF),
// replication, failover, and the lock that keeps writes in order.
type Server struct {
	store *Store
	aof   *AOF // nil when persistence is turned off (and while loading the AOF)
	port  int
	addr  string // how other nodes reach us, e.g. "localhost:6380"

	// writeMu makes write commands run one at a time, so the AOF and the
	// replicas receive writes in exactly the order they were applied.
	// It also protects: replicas, backlog, replID2, secondOffset.
	writeMu sync.Mutex

	// ----- Replication -----

	// isReplica is true when this server is a follower. Followers reject
	// writes from normal clients and only apply writes sent by their leader.
	isReplica atomic.Bool
	leader    atomic.Value // string "host:port" of our leader (followers only)

	// replID identifies a history of writes (a random 40-char id) and
	// replOffset counts how many bytes of that history we have.
	// "I have everything up to byte N of history X."
	replID     atomic.Value // string
	replOffset atomic.Int64

	// After a failover, the new leader starts a NEW history (new replID), but
	// remembers the old one: "my new history continues old history X, which
	// ended at byte secondOffset". That lets the other followers of the old
	// leader catch up from the new leader with a partial resync.
	replID2      string
	secondOffset int64

	backlog  *backlog              // recent stream bytes, for partial resyncs
	replicas map[*replica]struct{} // connected followers (leaders only)

	linkUp      atomic.Bool  // follower: is the connection to the leader healthy?
	lastContact atomic.Int64 // follower: unix nanos when we last heard from the leader

	leaderConnMu sync.Mutex
	leaderConn   net.Conn // follower: current connection to the leader

	syncFull    atomic.Int64 // leader stats, shown in INFO
	syncPartial atomic.Int64

	// ackCh is closed (and replaced) every time a follower ACKs, waking up
	// every WAIT command at once. See notifyAck / ackChan.
	ackMu sync.Mutex
	ackCh chan struct{}

	// ----- Failover (see failover.go) -----
	peers      []string // the other nodes in the cluster
	epoch      atomic.Int64
	electionMu sync.Mutex
	votedEpoch int64
	votedFor   string

	// Guards so each background loop runs at most once at a time.
	linkRunning  atomic.Bool
	leaderLoopOn atomic.Bool
	watchLoopOn  atomic.Bool
	stopped      atomic.Bool // set by Stop(); all loops exit
}

func NewServer(store *Store) *Server {
	srv := &Server{
		store:    store,
		replicas: make(map[*replica]struct{}),
		backlog:  newBacklog(0),
		ackCh:    make(chan struct{}),
	}
	srv.setReplID(newReplID())
	srv.setLeaderAddr("")
	return srv
}

func (srv *Server) getReplID() string      { return srv.replID.Load().(string) }
func (srv *Server) setReplID(id string)    { srv.replID.Store(id) }
func (srv *Server) getLeaderAddr() string  { return srv.leader.Load().(string) }
func (srv *Server) setLeaderAddr(a string) { srv.leader.Store(a) }
func (srv *Server) markContact()           { srv.lastContact.Store(time.Now().UnixNano()) }
func (srv *Server) sinceContact() time.Duration {
	return time.Since(time.Unix(0, srv.lastContact.Load()))
}

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

// Stop halts all replication and failover activity and drops replication
// connections. Together with closing the listener, tests use it to simulate
// a server crashing.
func (srv *Server) Stop() {
	srv.stopped.Store(true)
	srv.writeMu.Lock()
	for r := range srv.replicas {
		srv.removeReplicaLocked(r)
	}
	srv.writeMu.Unlock()
	srv.closeLeaderConn()
}

// handleConn serves one client until it disconnects.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := NewRespReader(conn)
	replicaPort := "" // set if this connection says it's a follower (REPLCONF listening-port)

	// Replies go into a buffer instead of straight to the network. If the
	// client sent several commands at once (pipelining), we answer them all
	// and send the replies in ONE network write instead of one per command.
	// We flush as soon as there are no more commands waiting to be read, so
	// a client sending one command at a time still gets its answer at once.
	// (Measured: 2-3x more throughput with pipelining. See README.)
	w := bufio.NewWriter(conn)
	send := func(v Value) error {
		if _, err := w.Write(v.Marshal()); err != nil {
			return err
		}
		if reader.Buffered() == 0 {
			return w.Flush()
		}
		return nil
	}

	for {
		// Read exactly one full command, however the bytes arrived over TCP.
		cmd, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// Malformed input: tell the client, then drop the connection
				// (we can't know where the next command starts).
				w.Write(Err("ERR Protocol error: " + err.Error()).Marshal())
				w.Flush()
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
			if send(OK()) != nil {
				return
			}
			continue
		case "PSYNC":
			// "PSYNC <replid> <offset>": a follower asking for data, saying
			// what it already has. From now on this connection is a
			// replication stream, not a normal client.
			if srv.isReplica.Load() {
				send(Err("ERR this server is a replica and cannot serve PSYNC"))
				continue
			}
			if len(cmd.Array) != 3 {
				send(wrongArgs("psync"))
				continue
			}
			reqID := cmd.Array[1].Str
			reqOffset, err := strconv.ParseInt(cmd.Array[2].Str, 10, 64)
			if err != nil {
				reqOffset = -1 // "PSYNC ? -1" = "I have nothing"
			}
			if w.Flush() != nil { // send anything still buffered first
				return
			}
			srv.serveReplica(conn, reader, replicaPort, reqID, reqOffset)
			return
		}

		if err := send(dispatch(srv, cmd)); err != nil {
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
