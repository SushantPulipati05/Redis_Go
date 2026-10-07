package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests move time forward instantly instead of sleeping.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore() (*Store, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	s := NewStore()
	s.now = clock.now
	return s, clock
}

// run sends one command through dispatch and returns the raw RESP reply.
func run(s *Store, parts ...string) string {
	vals := make([]Value, len(parts))
	for i, p := range parts {
		vals[i] = Bulk(p)
	}
	return string(dispatch(s, ArrayOf(vals...)).Marshal())
}

func TestKeyExpiresAfterTTL(t *testing.T) {
	s, clock := newTestStore()
	run(s, "SET", "otp", "1234", "EX", "10")

	clock.advance(9 * time.Second)
	if got := run(s, "GET", "otp"); got != "$4\r\n1234\r\n" {
		t.Fatalf("before expiry: got %q", got)
	}

	clock.advance(1 * time.Second) // exactly at the expiry time
	if got := run(s, "GET", "otp"); got != "$-1\r\n" {
		t.Fatalf("after expiry: got %q, want nil", got)
	}
}

func TestTTLValues(t *testing.T) {
	s, clock := newTestStore()
	cases := []struct{ got, want string }{
		{run(s, "TTL", "missing"), ":-2\r\n"},
		{run(s, "SET", "a", "1"), "+OK\r\n"},
		{run(s, "TTL", "a"), ":-1\r\n"},
		{run(s, "EXPIRE", "a", "100"), ":1\r\n"},
		{run(s, "TTL", "a"), ":100\r\n"},
		{run(s, "PTTL", "a"), ":100000\r\n"},
		{run(s, "PERSIST", "a"), ":1\r\n"},
		{run(s, "TTL", "a"), ":-1\r\n"},
		{run(s, "EXPIRE", "missing", "10"), ":0\r\n"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}

	// Plain SET removes an existing expiry (Redis behaviour).
	run(s, "SET", "b", "1", "EX", "5")
	run(s, "SET", "b", "2")
	clock.advance(10 * time.Second)
	if got := run(s, "GET", "b"); got != "$1\r\n2\r\n" {
		t.Errorf("SET should clear old TTL, got %q", got)
	}
}

func TestDelAndExists(t *testing.T) {
	s, _ := newTestStore()
	run(s, "SET", "a", "1")
	run(s, "SET", "b", "2")
	cases := []struct{ got, want string }{
		{run(s, "EXISTS", "a", "b", "c"), ":2\r\n"},
		{run(s, "EXISTS", "a", "a"), ":2\r\n"}, // counted twice, like Redis
		{run(s, "DEL", "a", "c"), ":1\r\n"},    // only "a" existed
		{run(s, "EXISTS", "a"), ":0\r\n"},
		{run(s, "DEL"), "-ERR wrong number of arguments for 'del' command\r\n"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}

func TestSetOptions(t *testing.T) {
	s, _ := newTestStore()
	cases := []struct{ got, want string }{
		{run(s, "SET", "lock", "me", "NX"), "+OK\r\n"},
		{run(s, "SET", "lock", "you", "NX"), "$-1\r\n"}, // already exists
		{run(s, "GET", "lock"), "$2\r\nme\r\n"},
		{run(s, "SET", "new", "x", "XX"), "$-1\r\n"}, // doesn't exist
		{run(s, "SET", "lock", "x", "XX"), "+OK\r\n"},
		{run(s, "SET", "k", "v", "EX", "abc"), "-ERR value is not an integer or out of range\r\n"},
		{run(s, "SET", "k", "v", "EX", "0"), "-ERR invalid expire time in 'set' command\r\n"},
		{run(s, "SET", "k", "v", "EX"), "-ERR syntax error\r\n"},
		{run(s, "SET", "k", "v", "NX", "XX"), "-ERR syntax error\r\n"},
		{run(s, "SET", "k", "v", "EX", "1", "PX", "1"), "-ERR syntax error\r\n"},
		{run(s, "set", "k", "v", "ex", "5"), "+OK\r\n"}, // options are case-insensitive
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}

// The background cleaner must delete expired keys even if nobody reads them.
func TestActiveExpiryDeletesUnreadKeys(t *testing.T) {
	s, clock := newTestStore()
	for i := 0; i < 100; i++ {
		run(s, "SET", fmt.Sprintf("k%d", i), "v", "EX", "1")
	}
	run(s, "SET", "keep", "v") // no expiry, must survive

	clock.advance(2 * time.Second)
	// Run cycles until a sample comes back clean, like RunActiveExpiry does.
	for {
		deleted, sampled := s.activeExpireCycle()
		if sampled == 0 || deleted*4 <= sampled {
			break
		}
	}

	if n := len(s.data); n != 1 {
		t.Fatalf("expected only 'keep' to remain, %d keys left", n)
	}
}

// Many goroutines racing on SET NX: exactly one must win.
// Run with `go test -race` to also check for data races.
func TestSetNXOnlyOneWinner(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Set("lock", "x", 0, SetIfNotExists) {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", winners)
	}
}
