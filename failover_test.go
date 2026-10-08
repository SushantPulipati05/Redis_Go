package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestMain runs before all tests: shrink every timeout so failover tests take
// milliseconds instead of seconds. The logic is identical, just faster.
func TestMain(m *testing.M) {
	heartbeatInterval = 50 * time.Millisecond
	leaderReadTimeout = 300 * time.Millisecond
	retryInterval = 50 * time.Millisecond
	failoverAfter = 200 * time.Millisecond
	electionJitter = 100 * time.Millisecond
	leaderWatchInterval = 100 * time.Millisecond
	peerTimeout = 300 * time.Millisecond
	quorumTimeout = 150 * time.Millisecond // must stay < failoverAfter
	os.Exit(m.Run())
}

// node is one server in a test cluster.
type node struct {
	srv  *Server
	ln   net.Listener
	addr string
}

// kill simulates a crash: stop accepting connections and drop all links.
func (n *node) kill() {
	n.ln.Close()
	n.srv.Stop()
}

// startCluster starts 3 nodes: nodes[0] is the leader, the others follow it.
// Every node knows the other two as peers, so failover is enabled.
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
		srv := NewServer(NewStore())
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

// A follower that briefly disconnects catches up from the backlog
// (partial resync) instead of downloading a whole new snapshot.
func TestPartialResyncAfterDisconnect(t *testing.T) {
	leaderSrv, leaderAddr := startLeader(t)
	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)
	leader := dial(t, leaderAddr)
	leader.do(t, "SET", "a", "1")

	// Cut the connection (like a network blip)...
	leaderSrv.writeMu.Lock()
	for r := range leaderSrv.replicas {
		leaderSrv.removeReplicaLocked(r)
	}
	leaderSrv.writeMu.Unlock()
	// ...and write while the follower is gone.
	leader.do(t, "SET", "b", "2")

	f := dial(t, followerAddr)
	eventually(t, "missed write arrives", func() bool {
		return f.do(t, "GET", "b") == "$1\r\n2\r\n"
	})
	if full, partial := leaderSrv.syncFull.Load(), leaderSrv.syncPartial.Load(); full != 1 || partial != 1 {
		t.Fatalf("want 1 full sync (first connect) + 1 partial (reconnect), got full=%d partial=%d", full, partial)
	}
	eventually(t, "offsets match", func() bool {
		return follower.replOffset.Load() == leaderSrv.replOffset.Load()
	})
}

// The main event: kill the leader, a follower takes over, the other follows
// it, and the data survives.
func TestAutomaticFailover(t *testing.T) {
	nodes := startCluster(t)
	leader := dial(t, nodes[0].addr)
	leader.do(t, "SET", "name", "sushant")
	leader.do(t, "SET", "city", "pune")
	for _, n := range nodes[1:] {
		eventually(t, "data replicated", func() bool {
			return n.srv.replOffset.Load() == nodes[0].srv.replOffset.Load()
		})
	}

	nodes[0].kill() // the leader crashes

	// Exactly one of the two followers must become leader.
	var newLeader, other *node
	eventually(t, "a new leader is elected", func() bool {
		a, b := nodes[1], nodes[2]
		switch {
		case !a.srv.isReplica.Load() && b.srv.isReplica.Load():
			newLeader, other = a, b
		case !b.srv.isReplica.Load() && a.srv.isReplica.Load():
			newLeader, other = b, a
		default:
			return false
		}
		return true
	})
	if newLeader.srv.epoch.Load() < 1 {
		t.Errorf("new leader should have epoch >= 1, got %d", newLeader.srv.epoch.Load())
	}

	// The other follower switches to the new leader...
	eventually(t, "other follower follows the new leader", func() bool {
		return other.srv.getLeaderAddr() == newLeader.addr && other.srv.linkUp.Load()
	})

	// ...the old data is still there...
	nl := dial(t, newLeader.addr)
	if got := nl.do(t, "GET", "name"); got != "$7\r\nsushant\r\n" {
		t.Errorf("data lost in failover: GET name = %q", got)
	}

	// ...the new leader accepts writes, and they replicate.
	if got := nl.do(t, "SET", "after", "failover"); got != "+OK\r\n" {
		t.Fatalf("new leader should accept writes, got %q", got)
	}
	o := dial(t, other.addr)
	eventually(t, "write on new leader replicates", func() bool {
		return o.do(t, "GET", "after") == "$8\r\nfailover\r\n"
	})

	// Bonus: the other follower switched with a PARTIAL resync, thanks to the
	// new leader remembering the old history (replID2).
	if p := newLeader.srv.syncPartial.Load(); p < 1 {
		t.Errorf("expected the other follower to partial-resync from the new leader, got %d partial syncs", p)
	}
}

// When the old leader comes back, it must NOT act as a second leader.
// It finds the newer leader and becomes its follower.
func TestOldLeaderRejoinsAsFollower(t *testing.T) {
	nodes := startCluster(t)
	dial(t, nodes[0].addr).do(t, "SET", "k", "v1")
	for _, n := range nodes[1:] {
		eventually(t, "replicated", func() bool {
			return n.srv.replOffset.Load() == nodes[0].srv.replOffset.Load()
		})
	}
	oldAddr := nodes[0].addr
	nodes[0].kill()

	var newLeader *node
	eventually(t, "new leader", func() bool {
		for _, n := range nodes[1:] {
			if !n.srv.isReplica.Load() {
				newLeader = n
				return true
			}
		}
		return false
	})
	nl := dial(t, newLeader.addr)
	eventually(t, "new leader accepts writes once its follower connects", func() bool {
		return nl.do(t, "SET", "k", "v2") == "+OK\r\n"
	})

	// Restart a server on the old leader's address, configured as a leader.
	ln, err := net.Listen("tcp", oldAddr)
	if err != nil {
		t.Skipf("could not reuse port %s: %v", oldAddr, err)
	}
	t.Cleanup(func() { ln.Close() })
	back := NewServer(NewStore())
	back.port = ln.Addr().(*net.TCPAddr).Port
	back.addr = oldAddr
	back.peers = []string{nodes[1].addr, nodes[2].addr}
	t.Cleanup(back.Stop)

	if !back.discoverLeaderAtStartup() {
		t.Fatal("restarted old leader should discover the new leader")
	}
	go back.Serve(ln)
	back.startReplicaLink()

	b := dial(t, oldAddr)
	eventually(t, "old leader catches up as a follower", func() bool {
		return b.do(t, "GET", "k") == "$2\r\nv2\r\n"
	})
	if got := b.do(t, "SET", "x", "1"); !strings.HasPrefix(got, "-READONLY") {
		t.Errorf("old leader should now be read-only, got %q", got)
	}
}

// ---------- Voting rules (no network needed) ----------

func TestNoVoteWhileLeaderReachable(t *testing.T) {
	s := NewServer(NewStore())
	s.isReplica.Store(true)
	s.linkUp.Store(true) // we can still hear the leader
	if s.handleVote(1, "x:1", 100) {
		t.Fatal("must not vote to replace a leader we can still reach")
	}
}

func TestLeaderNeverVotes(t *testing.T) {
	s := NewServer(NewStore()) // a working leader
	if s.handleVote(1, "x:1", 100) {
		t.Fatal("a working leader must not vote for its replacement")
	}
}

func TestOneVotePerEpoch(t *testing.T) {
	s := NewServer(NewStore())
	s.isReplica.Store(true)
	if !s.handleVote(1, "a:1", 0) {
		t.Fatal("first candidate should get the vote")
	}
	if s.handleVote(1, "b:1", 0) {
		t.Fatal("second candidate in the same epoch must be refused")
	}
	if !s.handleVote(1, "a:1", 0) {
		t.Fatal("asking again for the same candidate is fine")
	}
	if !s.handleVote(2, "b:1", 0) {
		t.Fatal("a new epoch allows a new vote")
	}
	if s.handleVote(1, "c:1", 0) {
		t.Fatal("votes for old epochs must be refused")
	}
}

func TestNoVoteForCandidateWithLessData(t *testing.T) {
	s := NewServer(NewStore())
	s.isReplica.Store(true)
	s.replOffset.Store(500)
	if s.handleVote(1, "a:1", 400) {
		t.Fatal("must not elect a candidate that has less data than us")
	}
}

func TestBacklog(t *testing.T) {
	b := newBacklog(100)
	b.write([]byte("hello"))
	b.write([]byte("world"))
	if got, ok := b.readFrom(105); !ok || string(got) != "world" {
		t.Errorf("readFrom(105) = %q, %v", got, ok)
	}
	if _, ok := b.readFrom(99); ok {
		t.Error("offset before the backlog must be refused")
	}
	if got, ok := b.readFrom(110); !ok || len(got) != 0 {
		t.Error("offset at the end = nothing missing, should be ok")
	}
	if _, ok := b.readFrom(111); ok {
		t.Error("offset past the end must be refused")
	}
}

// ---------- Majority rule for writes ----------

// A leader that loses ALL its followers is in the minority (1 of 3):
// it must refuse writes, but still serve reads.
func TestMinorityLeaderRejectsWrites(t *testing.T) {
	nodes := startCluster(t)
	l := dial(t, nodes[0].addr)
	if got := l.do(t, "SET", "a", "1"); got != "+OK\r\n" {
		t.Fatalf("with all followers up, write should work, got %q", got)
	}

	nodes[1].kill()
	nodes[2].kill()

	// For a moment the leader doesn't know yet (until quorumTimeout passes),
	// so some writes may still succeed. Track the last one that did.
	lastOK, i := "1", 1
	eventually(t, "leader starts refusing writes", func() bool {
		i++
		v := strconv.Itoa(i)
		reply := l.do(t, "SET", "a", v)
		if reply == "+OK\r\n" {
			lastOK = v
		}
		return strings.HasPrefix(reply, "-NOREPLICAS")
	})
	want := fmt.Sprintf("$%d\r\n%s\r\n", len(lastOK), lastOK)
	if got := l.do(t, "GET", "a"); got != want {
		t.Errorf("reads must still work and show the last ACCEPTED value %q, got %q", lastOK, got)
	}
}

// Losing ONE follower out of 3 nodes still leaves a majority (2 of 3).
func TestLeaderWithMajorityKeepsWriting(t *testing.T) {
	nodes := startCluster(t)
	nodes[2].kill()
	time.Sleep(3 * quorumTimeout) // well past the point where node 2 counts as gone

	l := dial(t, nodes[0].addr)
	if got := l.do(t, "SET", "a", "1"); got != "+OK\r\n" {
		t.Fatalf("leader + 1 follower is a majority of 3, write should work, got %q", got)
	}
}

// A standalone server (no -peers) has no cluster to lose: always writable.
func TestStandaloneLeaderAlwaysWritable(t *testing.T) {
	_, addr := startLeader(t) // no peers, no followers
	time.Sleep(3 * quorumTimeout)
	if got := dial(t, addr).do(t, "SET", "a", "1"); got != "+OK\r\n" {
		t.Fatalf("got %q", got)
	}
}
