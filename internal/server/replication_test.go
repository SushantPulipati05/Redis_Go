package server

import (
	"strings"
	"testing"
)

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

func TestFollowerReconnects(t *testing.T) {
	leaderSrv, leaderAddr := startLeader(t)
	follower, followerAddr := startFollower(t, leaderAddr)
	eventually(t, "follower link up", follower.linkUp.Load)

	leaderSrv.writeMu.Lock()
	for r := range leaderSrv.replicas {
		leaderSrv.removeReplicaLocked(r)
	}
	leaderSrv.writeMu.Unlock()

	dial(t, leaderAddr).do(t, "SET", "missed", "while-down")

	f := dial(t, followerAddr)
	eventually(t, "follower catches up after reconnect", func() bool {
		return f.do(t, "GET", "missed") == "$10\r\nwhile-down\r\n"
	})
}
