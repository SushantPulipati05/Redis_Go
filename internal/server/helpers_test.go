package server

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
	"github.com/SushantPulipati05/Redis_Go/internal/store"
)

func TestMain(m *testing.M) {
	heartbeatInterval = 50 * time.Millisecond
	leaderReadTimeout = 300 * time.Millisecond
	retryInterval = 50 * time.Millisecond
	failoverAfter = 200 * time.Millisecond
	electionJitter = 100 * time.Millisecond
	leaderWatchInterval = 100 * time.Millisecond
	peerTimeout = 300 * time.Millisecond
	quorumTimeout = 150 * time.Millisecond
	os.Exit(m.Run())
}

func startLeader(t *testing.T) (*Server, string) {
	t.Helper()
	srv := newServer(store.New())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv.port = ln.Addr().(*net.TCPAddr).Port
	srv.addr = ln.Addr().String()
	go srv.Serve(ln)
	srv.startLeaderLoops()
	t.Cleanup(srv.Stop)
	return srv, ln.Addr().String()
}

func startFollower(t *testing.T, leaderAddr string) (*Server, string) {
	t.Helper()
	srv := newServer(store.New())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv.port = ln.Addr().(*net.TCPAddr).Port
	srv.addr = ln.Addr().String()
	srv.setLeaderAddr(leaderAddr)
	srv.isReplica.Store(true)
	go srv.Serve(ln)
	srv.startReplicaLink()
	t.Cleanup(srv.Stop)
	return srv, ln.Addr().String()
}

type client struct {
	conn   net.Conn
	reader *resp.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{conn: conn, reader: resp.NewReader(conn)}
}

func (c *client) do(t *testing.T, parts ...string) string {
	t.Helper()
	vals := make([]resp.Value, len(parts))
	for i, p := range parts {
		vals[i] = resp.Bulk(p)
	}
	c.conn.Write(resp.ArrayOf(vals...).Marshal())
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	v, err := c.reader.Read()
	if err != nil {
		t.Fatalf("reading reply to %v: %v", parts, err)
	}
	return string(v.Marshal())
}

func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore() (*store.Store, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	s := store.New()
	s.SetClock(clock.now)
	return s, clock
}

func run(s *store.Store, parts ...string) string {
	vals := make([]resp.Value, len(parts))
	for i, p := range parts {
		vals[i] = resp.Bulk(p)
	}
	return string(dispatch(newServer(s), resp.ArrayOf(vals...)).Marshal())
}

func runOn(srv *Server, parts ...string) string {
	vals := make([]resp.Value, len(parts))
	for i, p := range parts {
		vals[i] = resp.Bulk(p)
	}
	return string(dispatch(srv, resp.ArrayOf(vals...)).Marshal())
}

type node struct {
	srv  *Server
	ln   net.Listener
	addr string
}

func (n *node) kill() {
	n.ln.Close()
	n.srv.Stop()
}

func startCluster(t *testing.T) []*node {
	t.Helper()
	nodes := make([]*node, 3)
	for i := range nodes {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &node{ln: ln, addr: ln.Addr().String()}
	}
	for i, n := range nodes {
		srv := newServer(store.New())
		srv.port = n.ln.Addr().(*net.TCPAddr).Port
		srv.addr = n.addr
		for j, other := range nodes {
			if j != i {
				srv.peers = append(srv.peers, other.addr)
			}
		}
		n.srv = srv
		go srv.Serve(n.ln)
		if i == 0 {
			srv.startLeaderLoops()
		} else {
			srv.setLeaderAddr(nodes[0].addr)
			srv.isReplica.Store(true)
			srv.startReplicaLink()
		}
		t.Cleanup(n.kill)
	}
	for _, n := range nodes[1:] {
		eventually(t, "follower link up", n.srv.linkUp.Load)
	}
	return nodes
}
