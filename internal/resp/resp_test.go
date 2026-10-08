package resp

import (
	"strings"
	"testing"
)

func TestParseCommand(t *testing.T) {
	v, err := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nsushant\r\n")).Read()
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

func TestPipelinedCommands(t *testing.T) {
	r := NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"))
	first, err := r.Read()
	if err != nil || first.Name() != "PING" {
		t.Fatalf("first = %+v, err = %v", first, err)
	}
	second, err := r.Read()
	if err != nil || second.Array[1].Str != "hi" {
		t.Fatalf("second = %+v, err = %v", second, err)
	}
}

func TestBinarySafeBulk(t *testing.T) {
	v, err := NewReader(strings.NewReader("*1\r\n$4\r\na\r\nb\r\n")).Read()
	if err != nil || v.Array[0].Str != "a\r\nb" {
		t.Fatalf("got %+v, err = %v", v, err)
	}
}

func TestReplyTypes(t *testing.T) {
	r := NewReader(strings.NewReader("+OK\r\n-ERR bad\r\n:42\r\n$-1\r\n"))
	cases := []Value{OK(), Err("ERR bad"), Int(42), NullBulk()}
	for _, want := range cases {
		got, err := r.Read()
		if err != nil || string(got.Marshal()) != string(want.Marshal()) {
			t.Errorf("got %+v (%v), want %+v", got, err, want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	enc := Command("SET", "k", "line\r\nbreak")
	v, err := NewReader(strings.NewReader(string(enc))).Read()
	if err != nil || string(v.Marshal()) != string(enc) {
		t.Fatalf("round trip changed the bytes: %q -> %q (%v)", enc, v.Marshal(), err)
	}
}
