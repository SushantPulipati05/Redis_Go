package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/store"
)

func TestPartialResyncAfterDisconnect(t *testing.T) {
	leaderSrv, leaderAddr := startLeader(t)
	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)
	leader := dial(t, leaderAddr)
	leader.do(t, "SET", "a", "1")

	leaderSrv.writeMu.Lock()
	for r := range leaderSrv.replicas {
		leaderSrv.removeReplicaLocked(r)
	}
	leaderSrv.writeMu.Unlock()
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

	nodes[0].kill()

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

	eventually(t, "other follower follows the new leader", func() bool {
		return other.srv.getLeaderAddr() == newLeader.addr && other.srv.linkUp.Load()
	})

	nl := dial(t, newLeader.addr)
	if got := nl.do(t, "GET", "name"); got != "$7\r\nsushant\r\n" {
		t.Errorf("data lost in failover: GET name = %q", got)
	}

	if got := nl.do(t, "SET", "after", "failover"); got != "+OK\r\n" {
		t.Fatalf("new leader should accept writes, got %q", got)
	}
	o := dial(t, other.addr)
	eventually(t, "write on new leader replicates", func() bool {
		return o.do(t, "GET", "after") == "$8\r\nfailover\r\n"
	})

	if p := newLeader.srv.syncPartial.Load(); p < 1 {
		t.Errorf("expected the other follower to partial-resync from the new leader, got %d partial syncs", p)
	}
}

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

	ln, err := net.Listen("tcp", oldAddr)
	if err != nil {
		t.Skipf("could not reuse port %s: %v", oldAddr, err)
	}
	t.Cleanup(func() { ln.Close() })
	back := newServer(store.New())
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

func TestNoVoteWhileLeaderReachable(t *testing.T) {
	s := newServer(store.New())
	s.isReplica.Store(true)
	s.linkUp.Store(true)
	if s.handleVote(1, "x:1", 100) {
		t.Fatal("must not vote to replace a leader we can still reach")
	}
}

func TestLeaderNeverVotes(t *testing.T) {
	s := newServer(store.New())
	if s.handleVote(1, "x:1", 100) {
		t.Fatal("a working leader must not vote for its replacement")
	}
}

func TestOneVotePerEpoch(t *testing.T) {
	s := newServer(store.New())
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
	s := newServer(store.New())
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

func TestMinorityLeaderRejectsWrites(t *testing.T) {
	nodes := startCluster(t)
	l := dial(t, nodes[0].addr)
	if got := l.do(t, "SET", "a", "1"); got != "+OK\r\n" {
		t.Fatalf("with all followers up, write should work, got %q", got)
	}

	nodes[1].kill()
	nodes[2].kill()

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

func TestLeaderWithMajorityKeepsWriting(t *testing.T) {
	nodes := startCluster(t)
	nodes[2].kill()
	time.Sleep(3 * quorumTimeout)

	l := dial(t, nodes[0].addr)
	if got := l.do(t, "SET", "a", "1"); got != "+OK\r\n" {
		t.Fatalf("leader + 1 follower is a majority of 3, write should work, got %q", got)
	}
}

func TestStandaloneLeaderAlwaysWritable(t *testing.T) {
	_, addr := startLeader(t)
	time.Sleep(3 * quorumTimeout)
	if got := dial(t, addr).do(t, "SET", "a", "1"); got != "+OK\r\n" {
		t.Fatalf("got %q", got)
	}
}
