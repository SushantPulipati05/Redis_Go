package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

// ---------- helpers ----------

// startLeader runs a leader on a random free port and returns it + its address.
func startLeader(t *testing.T) (*Server, string) {
	t.Helper()
	srv := NewServer(NewStore())
	ln, err := net.Listen("tcp", "127.0.0.1:0") // port 0 = "pick any free port"
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv.port = ln.Addr().(*net.TCPAddr).Port
	go srv.Serve(ln)
	go srv.heartbeatLoop()
	return srv, ln.Addr().String()
}

// startFollower runs a follower of leaderAddr on a random free port.
func startFollower(t *testing.T, leaderAddr string) (*Server, string) {
	t.Helper()
	srv := NewServer(NewStore())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv.port = ln.Addr().(*net.TCPAddr).Port
	srv.leaderAddr = leaderAddr
	srv.isReplica.Store(true)
	go srv.Serve(ln)
	go srv.runReplicaLink()
	return srv, ln.Addr().String()
}

// client is a tiny RESP client for tests: send a command, read the reply.
type client struct {
	conn   net.Conn
	reader *RespReader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{conn: conn, reader: NewRespReader(conn)}
}

func (c *client) do(t *testing.T, parts ...string) string {
	t.Helper()
	vals := make([]Value, len(parts))
	for i, p := range parts {
		vals[i] = Bulk(p)
	}
	c.conn.Write(ArrayOf(vals...).Marshal())
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	v, err := c.reader.Read()
	if err != nil {
		t.Fatalf("reading reply to %v: %v", parts, err)
	}
	return string(v.Marshal())
}

// eventually retries check until it returns true or 3 seconds pass.
// Replication is asynchronous, so followers catch up "very soon", not instantly.
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

// ---------- tests ----------

// Data that existed BEFORE the follower connected arrives via the snapshot.
func TestFollowerGetsSnapshot(t *testing.T) {
	_, leaderAddr := startLeader(t)
	leader := dial(t, leaderAddr)
	leader.do(t, "SET", "name", "sushant")
	leader.do(t, "SET", "otp", "1234", "EX", "100")

	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)

	f := dial(t, followerAddr)
	if got := f.do(t, "GET", "name"); got != "$7\r\nsushant\r\n" {
		t.Errorf("GET name on follower = %q", got)
	}
	if got := f.do(t, "TTL", "otp"); got != ":100\r\n" && got != ":99\r\n" {
		t.Errorf("TTL otp on follower = %q, expected ~100", got)
	}
}

// Writes made AFTER the follower connected arrive via the live stream.
func TestFollowerGetsLiveWrites(t *testing.T) {
	_, leaderAddr := startLeader(t)
	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)

	leader := dial(t, leaderAddr)
	f := dial(t, followerAddr)

	leader.do(t, "SET", "city", "pune")
	eventually(t, "SET reaches follower", func() bool {
		return f.do(t, "GET", "city") == "$4\r\npune\r\n"
	})

	leader.do(t, "DEL", "city")
	eventually(t, "DEL reaches follower", func() bool {
		return f.do(t, "GET", "city") == "$-1\r\n"
	})
}

// Clients can read from a follower but not write to it.
func TestFollowerIsReadOnly(t *testing.T) {
	_, leaderAddr := startLeader(t)
	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)

	f := dial(t, followerAddr)
	got := f.do(t, "SET", "x", "1")
	if !strings.HasPrefix(got, "-READONLY") {
		t.Fatalf("write on follower = %q, want READONLY error", got)
	}
	if got := f.do(t, "GET", "x"); got != "$-1\r\n" {
		t.Errorf("rejected write must not change data, GET x = %q", got)
	}
}

// After the follower applies everything, its offset equals the leader's.
// That's how we'll know a follower is fully caught up (Day 5).
func TestOffsetsMatchWhenCaughtUp(t *testing.T) {
	leaderSrv, leaderAddr := startLeader(t)
	follower, _ := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)

	leader := dial(t, leaderAddr)
	for i := 0; i < 50; i++ {
		leader.do(t, "SET", "k", strings.Repeat("v", i))
	}
	eventually(t, "offsets match", func() bool {
		return follower.replOffset.Load() == leaderSrv.replOffset.Load()
	})
	if follower.getReplID() != leaderSrv.getReplID() {
		t.Errorf("follower should adopt the leader's replication id")
	}
}

// Two followers both receive every write.
func TestTwoFollowers(t *testing.T) {
	leaderSrv, leaderAddr := startLeader(t)
	f1srv, f1addr := startFollower(t, leaderAddr)
	f2srv, f2addr := startFollower(t, leaderAddr)
	eventually(t, "both links up", func() bool { return f1srv.linkUp.Load() && f2srv.linkUp.Load() })

	dial(t, leaderAddr).do(t, "SET", "shared", "yes")
	for _, addr := range []string{f1addr, f2addr} {
		f := dial(t, addr)
		eventually(t, "write reaches "+addr, func() bool {
			return f.do(t, "GET", "shared") == "$3\r\nyes\r\n"
		})
	}

	info := dial(t, leaderAddr).do(t, "INFO", "replication")
	if !strings.Contains(info, "connected_slaves:2") {
		t.Errorf("leader INFO should show 2 followers:\n%s", info)
	}
	_ = leaderSrv
}

// If the follower's connection drops, it reconnects and resyncs on its own.
func TestFollowerReconnects(t *testing.T) {
	leaderSrv, leaderAddr := startLeader(t)
	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)

	// Simulate a network failure: the leader drops every follower connection.
	leaderSrv.writeMu.Lock()
	for r := range leaderSrv.replicas {
		leaderSrv.removeReplicaLocked(r)
	}
	leaderSrv.writeMu.Unlock()

	// A write while the follower is disconnected...
	dial(t, leaderAddr).do(t, "SET", "missed", "while-down")

	// ...still reaches it after it reconnects (via the fresh snapshot).
	f := dial(t, followerAddr)
	eventually(t, "follower catches up after reconnect", func() bool {
		return f.do(t, "GET", "missed") == "$10\r\nwhile-down\r\n"
	})
}
