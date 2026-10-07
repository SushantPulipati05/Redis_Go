package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runOn sends one command to a full Server (so writes reach its AOF).
func runOn(srv *Server, parts ...string) string {
	vals := make([]Value, len(parts))
	for i, p := range parts {
		vals[i] = Bulk(p)
	}
	return string(dispatch(srv, ArrayOf(vals...)).Marshal())
}

// newServerWithAOF builds a server with a fake clock, logging to path.
func newServerWithAOF(t *testing.T, path string, clock *fakeClock) *Server {
	t.Helper()
	store := NewStore()
	store.now = clock.now
	srv := NewServer(store)
	if _, err := LoadAOF(path, srv); err != nil {
		t.Fatalf("load: %v", err)
	}
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	srv.aof = aof
	return srv
}

// Data written before a "restart" must be there after it.
func TestAOFSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}

	srv := newServerWithAOF(t, path, clock)
	runOn(srv, "SET", "name", "sushant")
	runOn(srv, "SET", "temp", "x")
	runOn(srv, "DEL", "temp")
	runOn(srv, "SET", "lock", "a", "NX")
	runOn(srv, "SET", "lock", "b", "NX") // rejected: must NOT be logged
	runOn(srv, "SET", "city", "pune")
	runOn(srv, "EXPIRE", "city", "100")
	runOn(srv, "PERSIST", "city")
	srv.aof.Close()

	// "Restart": brand-new server, same file.
	srv2 := newServerWithAOF(t, path, clock)
	defer srv2.aof.Close()
	cases := []struct{ got, want string }{
		{runOn(srv2, "GET", "name"), "$7\r\nsushant\r\n"},
		{runOn(srv2, "GET", "temp"), "$-1\r\n"},
		{runOn(srv2, "GET", "lock"), "$1\r\na\r\n"},
		{runOn(srv2, "TTL", "city"), ":-1\r\n"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}

// A key with 100s left that's down for 30s must come back with 70s, not 100s.
// This is why the AOF stores absolute times (PXAT) instead of "EX 100".
func TestAOFKeepsAbsoluteExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}

	srv := newServerWithAOF(t, path, clock)
	runOn(srv, "SET", "otp", "1234", "EX", "100")
	runOn(srv, "SET", "short", "x", "EX", "10")
	srv.aof.Close()

	clock.advance(30 * time.Second) // server was "down" for 30 seconds

	srv2 := newServerWithAOF(t, path, clock)
	defer srv2.aof.Close()
	if got := runOn(srv2, "TTL", "otp"); got != ":70\r\n" {
		t.Errorf("TTL after restart = %q, want :70", got)
	}
	if got := runOn(srv2, "GET", "short"); got != "$-1\r\n" {
		t.Errorf("key that expired while down should be gone, got %q", got)
	}
}

// The file stores normalised commands: relative EX becomes absolute PXAT.
func TestAOFFileContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}

	srv := newServerWithAOF(t, path, clock)
	runOn(srv, "set", "a", "1", "ex", "5")
	runOn(srv, "GET", "a") // reads are never logged
	srv.aof.Close()

	data, _ := os.ReadFile(path)
	want := "*5\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n$4\r\nPXAT\r\n$10\r\n1000005000\r\n"
	if string(data) != want {
		t.Fatalf("file = %q\nwant   %q", data, want)
	}
}

// Crash in the middle of writing the last command: load what's complete,
// cut off the fragment, keep going.
func TestAOFTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	complete := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
	partial := "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nhel" // cut off mid-value
	os.WriteFile(path, []byte(complete+partial), 0o644)

	srv := NewServer(NewStore())
	n, err := LoadAOF(path, srv)
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want 1 command and no error", n, err)
	}
	if got := runOn(srv, "GET", "a"); got != "$1\r\n1\r\n" {
		t.Errorf("GET a = %q", got)
	}
	data, _ := os.ReadFile(path)
	if string(data) != complete {
		t.Errorf("file should be truncated to the last complete command, got %q", data)
	}
}

// Garbage in the middle is real corruption: refuse to start.
func TestAOFCorruptMiddle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	content := "*1\r\n$4\r\nPING\r\n" + "garbage\r\n" + "*1\r\n$4\r\nPING\r\n"
	os.WriteFile(path, []byte(content), 0o644)

	_, err := LoadAOF(path, NewServer(NewStore()))
	if err == nil || !strings.Contains(err.Error(), "corrupted at byte 14") {
		t.Fatalf("expected corruption error at byte 14, got %v", err)
	}
}

func TestAOFMissingFileIsFreshStart(t *testing.T) {
	n, err := LoadAOF(filepath.Join(t.TempDir(), "nope.aof"), NewServer(NewStore()))
	if n != 0 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
