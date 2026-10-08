package main

// Differential testing: send the SAME random commands to our server and to
// real Redis, and check that every reply is identical.
//
// This catches mistakes no hand-written test would think of: wrong error
// text, wrong counts for duplicate keys, option edge cases, and so on.
//
// It only runs when you point it at a real Redis:
//
//	REDIS_ADDR=localhost:6379 go test -run Differential -v
//
// It uses database 15 on the real Redis (SELECT 15) and empties it first,
// so your normal data in database 0 is left alone.

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDifferentialAgainstRealRedis(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("set REDIS_ADDR=host:port to compare against a real Redis")
	}

	// Our server, running in-process on a random port.
	_, ourAddr := startLeader(t)
	ours := dial(t, ourAddr)

	// Real Redis, in its own database so we don't touch real data.
	conn, err := net.Dial("tcp", redisAddr)
	if err != nil {
		t.Fatalf("can't reach Redis at %s: %v", redisAddr, err)
	}
	t.Cleanup(func() { conn.Close() })
	real := &client{conn: conn, reader: NewRespReader(conn)}
	real.do(t, "SELECT", "15")
	real.do(t, "FLUSHDB")

	n := 100_000
	if testing.Short() {
		n = 5_000
	}
	if s := os.Getenv("DIFF_N"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	seed := time.Now().UnixNano()
	if s := os.Getenv("DIFF_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	rng := rand.New(rand.NewSource(seed))
	t.Logf("running %d random commands (seed %d; rerun with DIFF_SEED=%d to reproduce)", n, seed, seed)

	for i := 0; i < n; i++ {
		cmd := randomCommand(rng)
		got := ours.do(t, cmd...)
		want := real.do(t, cmd...)
		if !repliesMatch(cmd, got, want) {
			t.Fatalf("command #%d %q\n  ours:  %q\n  redis: %q\n(reproduce with DIFF_SEED=%d)", i, cmd, got, want, seed)
		}
	}
}

// A small key space, so commands keep hitting the same keys and interact
// (SET then GET then DEL then EXISTS ...), which is where bugs hide.
var diffKeys = []string{"a", "b", "c", "d", "user:1", "user:2", "otp", "x"}

func randomCommand(rng *rand.Rand) []string {
	key := func() string { return diffKeys[rng.Intn(len(diffKeys))] }
	val := func() string {
		vals := []string{"1", "hello", "", "with space", "line\r\nbreak", "ünïcödé", strconv.Itoa(rng.Intn(1000))}
		return vals[rng.Intn(len(vals))]
	}
	// Long expiries only: nothing should actually expire during the run,
	// otherwise timing differences between the two servers would show up.
	secs := func() string { return strconv.Itoa(1000 + rng.Intn(9000)) }

	switch rng.Intn(20) {
	case 0, 1, 2:
		return []string{"SET", key(), val()}
	case 3:
		return []string{"SET", key(), val(), "EX", secs()}
	case 4:
		return []string{"SET", key(), val(), "PX", secs() + "000"}
	case 5:
		return []string{"SET", key(), val(), "NX"}
	case 6:
		return []string{"SET", key(), val(), "XX"}
	case 7:
		// Deliberately bad commands: the error text must match too.
		bad := [][]string{
			{"SET", key(), val(), "EX", "0"},
			{"SET", key(), val(), "EX", "abc"},
			{"SET", key(), val(), "EX"},
			{"SET", key(), val(), "NX", "XX"},
			{"SET", key(), val(), "EX", "10", "PX", "10"},
			{"SET", key(), val(), "BOGUS"},
			{"SET", key()},
			{"GET"},
			{"GET", key(), key()},
			{"DEL"},
			{"EXPIRE", key()},
			{"EXPIRE", key(), "soon"},
			{"TTL"},
		}
		return bad[rng.Intn(len(bad))]
	case 8, 9, 10:
		return []string{"GET", key()}
	case 11:
		return []string{"DEL", key(), key()}
	case 12:
		return []string{"EXISTS", key(), key(), key()}
	case 13:
		return []string{"EXPIRE", key(), secs()}
	case 14:
		return []string{"PEXPIRE", key(), secs() + "000"}
	case 15:
		return []string{"TTL", key()}
	case 16:
		return []string{"PTTL", key()}
	case 17:
		return []string{"PERSIST", key()}
	case 18:
		return []string{"ECHO", val()}
	default:
		return []string{"PING"}
	}
}

// repliesMatch compares replies exactly, except remaining TTLs: the two
// servers read the clock at slightly different moments, so a TTL may
// differ by a tiny amount. Special values (-1, -2) must match exactly.
func repliesMatch(cmd []string, got, want string) bool {
	if got == want {
		return true
	}
	if cmd[0] != "TTL" && cmd[0] != "PTTL" {
		return false
	}
	var g, w int64
	if _, err := fmt.Sscanf(got, ":%d\r\n", &g); err != nil {
		return false
	}
	if _, err := fmt.Sscanf(want, ":%d\r\n", &w); err != nil {
		return false
	}
	if g < 0 || w < 0 {
		return g == w
	}
	tolerance := int64(1) // seconds
	if cmd[0] == "PTTL" {
		tolerance = 250 // milliseconds
	}
	d := g - w
	return d >= -tolerance && d <= tolerance
}
