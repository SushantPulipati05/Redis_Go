package main

import (
	"strings"
	"testing"
)

// Run with:  go test ./...

func TestParseSetCommand(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nsushant\r\n"
	v, err := NewRespReader(strings.NewReader(input)).Read()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SET", "name", "sushant"}
	if len(v.Array) != len(want) {
		t.Fatalf("got %d parts, want %d", len(v.Array), len(want))
	}
	for i, w := range want {
		if v.Array[i].Str != w {
			t.Errorf("part %d = %q, want %q", i, v.Array[i].Str, w)
		}
	}
}

// Two commands arriving in one TCP packet must be read as two commands.
func TestTwoCommandsInOneRead(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
	r := NewRespReader(strings.NewReader(input))
	first, err := r.Read()
	if err != nil || first.Array[0].Str != "PING" {
		t.Fatalf("first = %+v, err = %v", first, err)
	}
	second, err := r.Read()
	if err != nil || second.Array[1].Str != "hi" {
		t.Fatalf("second = %+v, err = %v", second, err)
	}
}

// Bulk strings are length-based, so values may contain \r\n.
func TestBinarySafeValue(t *testing.T) {
	input := "*1\r\n$4\r\na\r\nb\r\n"
	v, err := NewRespReader(strings.NewReader(input)).Read()
	if err != nil || v.Array[0].Str != "a\r\nb" {
		t.Fatalf("got %+v, err = %v", v, err)
	}
}

func TestSetGetFlow(t *testing.T) {
	s := NewStore()
	cmd := func(parts ...string) string {
		vals := make([]Value, len(parts))
		for i, p := range parts {
			vals[i] = Bulk(p)
		}
		return string(dispatch(NewServer(s), ArrayOf(vals...)).Marshal())
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
