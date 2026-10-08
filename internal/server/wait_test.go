package server

import (
	"strings"
	"testing"
	"time"
)

func TestWaitReturnsWhenFollowersHaveWrite(t *testing.T) {
	_, leaderAddr := startLeader(t)
	f1, _ := startFollower(t, leaderAddr)
	f2, _ := startFollower(t, leaderAddr)
	eventually(t, "both links up", func() bool { return f1.linkUp.Load() && f2.linkUp.Load() })

	c := dial(t, leaderAddr)
	c.do(t, "SET", "balance", "500")
	start := time.Now()
	if got := c.do(t, "WAIT", "2", "1000"); got != ":2\r\n" {
		t.Fatalf("WAIT 2 = %q, want :2", got)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("WAIT took %v; should return as soon as followers ACK", took)
	}
	for _, f := range []*Server{f1, f2} {
		if v, _ := f.store.Get("balance"); v != "500" {
			t.Errorf("follower missing the write, got %q", v)
		}
	}
}

func TestWaitTimesOut(t *testing.T) {
	_, leaderAddr := startLeader(t)
	f, _ := startFollower(t, leaderAddr)
	eventually(t, "link up", f.linkUp.Load)

	c := dial(t, leaderAddr)
	c.do(t, "SET", "k", "v")
	start := time.Now()
	if got := c.do(t, "WAIT", "3", "200"); got != ":1\r\n" {
		t.Fatalf("WAIT 3 with one follower = %q, want :1", got)
	}
	if took := time.Since(start); took < 190*time.Millisecond {
		t.Errorf("WAIT returned after %v, should have waited ~200ms", took)
	}
}

func TestWaitZeroReturnsImmediately(t *testing.T) {
	_, leaderAddr := startLeader(t)
	if got := dial(t, leaderAddr).do(t, "WAIT", "0", "0"); got != ":0\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWaitOnFollowerIsAnError(t *testing.T) {
	_, leaderAddr := startLeader(t)
	f, fAddr := startFollower(t, leaderAddr)
	eventually(t, "link up", f.linkUp.Load)
	if got := dial(t, fAddr).do(t, "WAIT", "1", "100"); !strings.HasPrefix(got, "-ERR WAIT cannot be used with replica") {
		t.Fatalf("got %q", got)
	}
}

func TestOffsetsStillMatchAfterWait(t *testing.T) {
	leader, leaderAddr := startLeader(t)
	f, _ := startFollower(t, leaderAddr)
	eventually(t, "link up", f.linkUp.Load)
	c := dial(t, leaderAddr)
	for i := 0; i < 5; i++ {
		c.do(t, "SET", "k", "v")
		c.do(t, "WAIT", "1", "500")
	}
	eventually(t, "offsets match", func() bool {
		return f.replOffset.Load() == leader.replOffset.Load()
	})
}
