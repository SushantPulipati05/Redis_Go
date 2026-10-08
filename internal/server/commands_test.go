package server

import (
	"testing"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
	"github.com/SushantPulipati05/Redis_Go/internal/store"
)

func TestKeyExpiresAfterTTL(t *testing.T) {
	s, clock := newTestStore()
	run(s, "SET", "otp", "1234", "EX", "10")

	clock.advance(9 * time.Second)
	if got := run(s, "GET", "otp"); got != "$4\r\n1234\r\n" {
		t.Fatalf("before expiry: got %q", got)
	}

	clock.advance(1 * time.Second)
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
		{run(s, "EXISTS", "a", "a"), ":2\r\n"},
		{run(s, "DEL", "a", "c"), ":1\r\n"},
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
		{run(s, "SET", "lock", "you", "NX"), "$-1\r\n"},
		{run(s, "GET", "lock"), "$2\r\nme\r\n"},
		{run(s, "SET", "new", "x", "XX"), "$-1\r\n"},
		{run(s, "SET", "lock", "x", "XX"), "+OK\r\n"},
		{run(s, "SET", "k", "v", "EX", "abc"), "-ERR value is not an integer or out of range\r\n"},
		{run(s, "SET", "k", "v", "EX", "0"), "-ERR invalid expire time in 'set' command\r\n"},
		{run(s, "SET", "k", "v", "EX"), "-ERR syntax error\r\n"},
		{run(s, "SET", "k", "v", "NX", "XX"), "-ERR syntax error\r\n"},
		{run(s, "SET", "k", "v", "EX", "1", "PX", "1"), "-ERR syntax error\r\n"},
		{run(s, "set", "k", "v", "ex", "5"), "+OK\r\n"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}

func TestSetGetFlow(t *testing.T) {
	s := store.New()
	cmd := func(parts ...string) string {
		vals := make([]resp.Value, len(parts))
		for i, p := range parts {
			vals[i] = resp.Bulk(p)
		}
		return string(dispatch(newServer(s), resp.ArrayOf(vals...)).Marshal())
	}

	cases := []struct {
		got, want string
	}{
		{cmd("ping"), "+PONG\r\n"},
		{cmd("GET", "name"), "$-1\r\n"},
		{cmd("SET", "name", "sushant"), "+OK\r\n"},
		{cmd("GET", "name"), "$7\r\nsushant\r\n"},
		{cmd("GET"), "-ERR wrong number of arguments for 'get' command\r\n"},
		{cmd("NOPE"), "-ERR unknown command 'NOPE'\r\n"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}
