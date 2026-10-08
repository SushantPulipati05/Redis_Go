// Package server implements a Redis-compatible server with AOF persistence,
// leader-follower replication and automatic failover.
package server

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

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
	"github.com/SushantPulipati05/Redis_Go/internal/store"
)

type Config struct {
	Port      int
	Addr      string   // address peers use to reach this node; defaults to localhost:<Port>
	Peers     []string // other cluster members; enables automatic failover
	ReplicaOf string   // start as a follower of this address
	AOFPath   string   // empty disables persistence
	Fsync     FsyncPolicy
}

type Server struct {
	store *store.Store
	aof   *AOF
	port  int
	addr  string
	peers []string

	// writeMu serialises write commands so that the store, the AOF and the
	// replication stream all see writes in the same order. It also guards
	// replicas, backlog, replID2 and secondOffset.
	writeMu sync.Mutex

	isReplica atomic.Bool
	leader    atomic.Value // string

	// A replication history is identified by replID; replOffset is how many
	// bytes of it this node has. After a failover, replID2/secondOffset
	// remember the history this node continued from.
	replID       atomic.Value // string
	replOffset   atomic.Int64
	replID2      string
	secondOffset int64

	backlog  *backlog
	replicas map[*replica]struct{}

	linkUp       atomic.Bool
	lastContact  atomic.Int64 // unix nanos
	leaderConnMu sync.Mutex
	leaderConn   net.Conn

	syncFull    atomic.Int64
	syncPartial atomic.Int64

	ackMu sync.Mutex
	ackCh chan struct{} // closed and replaced on every replica ACK

	epoch      atomic.Int64
	electionMu sync.Mutex
	votedEpoch int64
	votedFor   string

	linkRunning  atomic.Bool
	leaderLoopOn atomic.Bool
	watchLoopOn  atomic.Bool
	stopped      atomic.Bool
	done         chan struct{}
	stopOnce     sync.Once

	lnMu sync.Mutex
	ln   net.Listener
}

func newServer(st *store.Store) *Server {
	srv := &Server{
		store:    st,
		replicas: make(map[*replica]struct{}),
		backlog:  newBacklog(0),
		ackCh:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	srv.setReplID(newReplID())
	srv.setLeaderAddr("")
	return srv
}

// New creates a server from cfg, replaying the AOF if one exists.
func New(cfg Config) (*Server, error) {
	srv := newServer(store.New())
	srv.port = cfg.Port
	srv.addr = cfg.Addr
	if srv.addr == "" {
		srv.addr = fmt.Sprintf("localhost:%d", cfg.Port)
	}
	srv.peers = cfg.Peers

	if cfg.AOFPath != "" {
		start := time.Now()
		n, err := LoadAOF(cfg.AOFPath, srv)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", cfg.AOFPath, err)
		}
		log.Printf("loaded %d commands from %s in %v", n, cfg.AOFPath, time.Since(start).Round(time.Millisecond))

		aof, err := OpenAOF(cfg.AOFPath, cfg.Fsync)
		if err != nil {
			return nil, err
		}
		srv.aof = aof
	}

	if cfg.ReplicaOf != "" {
		srv.setLeaderAddr(cfg.ReplicaOf)
		srv.isReplica.Store(true)
	}
	return srv, nil
}

// Start launches background work. A node configured as leader first checks
// whether the cluster already elected someone else while it was down.
func (srv *Server) Start() {
	go srv.store.RunActiveExpiry(srv.done)

	if !srv.isReplica.Load() && len(srv.peers) > 0 {
		srv.discoverLeaderAtStartup()
	}
	if srv.isReplica.Load() {
		srv.startReplicaLink()
	} else {
		srv.startLeaderLoops()
	}
}

func (srv *Server) IsLeader() bool     { return !srv.isReplica.Load() }
func (srv *Server) LeaderAddr() string { return srv.getLeaderAddr() }
func (srv *Server) Addr() string       { return srv.addr }

func (srv *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", srv.port))
	if err != nil {
		return err
	}
	return srv.Serve(ln)
}

func (srv *Server) Serve(ln net.Listener) error {
	srv.lnMu.Lock()
	srv.ln = ln
	srv.lnMu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("accept: %v", err)
			continue
		}
		go srv.handleConn(conn)
	}
}

// Stop halts replication and background work and drops replication links.
func (srv *Server) Stop() {
	srv.stopped.Store(true)
	srv.stopOnce.Do(func() { close(srv.done) })
	srv.writeMu.Lock()
	for r := range srv.replicas {
		srv.removeReplicaLocked(r)
	}
	srv.writeMu.Unlock()
	srv.closeLeaderConn()
}

// Shutdown stops the server and flushes the AOF. In-flight writes finish first.
func (srv *Server) Shutdown() error {
	srv.Stop()
	srv.writeMu.Lock()
	defer srv.writeMu.Unlock()

	srv.lnMu.Lock()
	if srv.ln != nil {
		srv.ln.Close()
	}
	srv.lnMu.Unlock()

	if srv.aof != nil {
		return srv.aof.Close()
	}
	return nil
}

func (srv *Server) getReplID() string      { return srv.replID.Load().(string) }
func (srv *Server) setReplID(id string)    { srv.replID.Store(id) }
func (srv *Server) getLeaderAddr() string  { return srv.leader.Load().(string) }
func (srv *Server) setLeaderAddr(a string) { srv.leader.Store(a) }
func (srv *Server) markContact()           { srv.lastContact.Store(time.Now().UnixNano()) }
func (srv *Server) sinceContact() time.Duration {
	return time.Since(time.Unix(0, srv.lastContact.Load()))
}

// propagate records a write that has just been applied. Commands pass the
// normalised form (e.g. EX converted to PXAT) so that replaying it later, on
// any node, gives the same result. Caller must hold writeMu.
func (srv *Server) propagate(args ...string) {
	cmd := resp.Command(args...)
	if srv.aof != nil {
		if err := srv.aof.Append(cmd); err != nil {
			log.Printf("AOF write failed: %v", err)
		}
	}
	if !srv.isReplica.Load() {
		srv.replicateLocked(cmd)
	}
}

func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	replicaPort := ""

	// Replies are buffered and flushed once no more pipelined commands are
	// waiting, so a pipeline is answered with one write instead of many.
	w := bufio.NewWriter(conn)
	send := func(v resp.Value) error {
		if _, err := w.Write(v.Marshal()); err != nil {
			return err
		}
		if reader.Buffered() == 0 {
			return w.Flush()
		}
		return nil
	}

	for {
		cmd, err := reader.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				w.Write(resp.Err("ERR Protocol error: " + err.Error()).Marshal())
				w.Flush()
			}
			return
		}

		switch cmd.Name() {
		case "REPLCONF":
			if len(cmd.Array) == 3 && strings.EqualFold(cmd.Array[1].Str, "listening-port") {
				replicaPort = cmd.Array[2].Str
			}
			if send(resp.OK()) != nil {
				return
			}
			continue

		case "PSYNC":
			if srv.isReplica.Load() {
				send(resp.Err("ERR this server is a replica and cannot serve PSYNC"))
				continue
			}
			if len(cmd.Array) != 3 {
				send(wrongArgs("psync"))
				continue
			}
			reqID := cmd.Array[1].Str
			reqOffset, err := strconv.ParseInt(cmd.Array[2].Str, 10, 64)
			if err != nil {
				reqOffset = -1
			}
			if w.Flush() != nil {
				return
			}
			// From here on the connection is a replication stream.
			srv.serveReplica(conn, reader, replicaPort, reqID, reqOffset)
			return
		}

		if err := send(dispatch(srv, cmd)); err != nil {
			return
		}
	}
}
