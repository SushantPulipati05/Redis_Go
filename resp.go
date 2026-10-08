package main

// RESP = REdis Serialization Protocol: the format clients and Redis use to talk.
//
//   +OK\r\n                 simple string
//   -ERR message\r\n        error
//   :42\r\n                 integer
//   $5\r\nhello\r\n         bulk string (length-prefixed, can hold any bytes)
//   $-1\r\n                 null bulk string ("no value")
//   *2\r\n$3\r\nGET\r\n$1\r\na\r\n   array of 2 bulk strings  ->  ["GET", "a"]
//
// Clients always send commands as an array of bulk strings.

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// The first byte of every RESP value says what type it is.
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// Safety limits so a broken or malicious client can't make us allocate huge memory.
const (
	maxBulkLen  = 512 * 1024 * 1024 // 512 MB, same as real Redis
	maxArrayLen = 1024 * 1024
)

// Value is one RESP value. Only the fields that match Type are used.
type Value struct {
	Type  byte
	Str   string  // for SimpleString, Error, BulkString
	Num   int64   // for Integer
	Array []Value // for Array
	Null  bool    // true for $-1 (null bulk) or *-1 (null array)
}

// ---------- Reading (parsing what the client sent) ----------

// RespReader wraps the connection in a bufio.Reader.
// Why buffered? TCP is a *stream*: one conn.Read may return half a command,
// or two commands glued together. bufio lets us read "exactly one line" or
// "exactly N bytes" no matter how the bytes arrived.
type RespReader struct {
	r *bufio.Reader
}

func NewRespReader(rd io.Reader) *RespReader {
	return &RespReader{r: bufio.NewReader(rd)}
}

// Read parses and returns exactly one complete RESP value.
func (rr *RespReader) Read() (Value, error) {
	typ, err := rr.r.ReadByte()
	if err != nil {
		return Value{}, err // io.EOF here = client disconnected cleanly
	}

	switch typ {
	case Array:
		return rr.readArray()
	case BulkString:
		return rr.readBulk()
	case SimpleString, Error:
		// +OK / -ERR ... : the rest of the line is the text.
		// Clients never send these, but a follower reads them from its leader.
		line, err := rr.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: typ, Str: line}, nil
	case Integer:
		line, err := rr.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid integer %q", line)
		}
		return Value{Type: Integer, Num: n}, nil
	default:
		return Value{}, fmt.Errorf("unknown RESP type byte %q", typ)
	}
}

// Buffered returns how many bytes have been read from the underlying source
// but not consumed yet. The AOF loader uses it to work out exactly where the
// last complete command ended.
func (rr *RespReader) Buffered() int {
	return rr.r.Buffered()
}

// readLine reads up to "\r\n" and returns the text without it.
func (rr *RespReader) readLine() (string, error) {
	line, err := rr.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("line does not end with \\r\\n")
	}
	return line[:len(line)-2], nil
}

// readInt reads a line like "3" and converts it to a number.
func (rr *RespReader) readInt() (int, error) {
	line, err := rr.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(line)
	if err != nil {
		return 0, fmt.Errorf("invalid length %q", line)
	}
	return n, nil
}

// readArray handles "*<count>\r\n" followed by <count> values.
func (rr *RespReader) readArray() (Value, error) {
	count, err := rr.readInt()
	if err != nil {
		return Value{}, err
	}
	if count == -1 {
		return Value{Type: Array, Null: true}, nil
	}
	if count < 0 || count > maxArrayLen {
		return Value{}, fmt.Errorf("invalid array length %d", count)
	}

	items := make([]Value, 0, count)
	for i := 0; i < count; i++ {
		// Each element is itself a RESP value, so we call Read recursively.
		v, err := rr.Read()
		if err != nil {
			return Value{}, err
		}
		items = append(items, v)
	}
	return Value{Type: Array, Array: items}, nil
}

// readBulk handles "$<length>\r\n<exactly length bytes>\r\n".
func (rr *RespReader) readBulk() (Value, error) {
	length, err := rr.readInt()
	if err != nil {
		return Value{}, err
	}
	if length == -1 {
		return Value{Type: BulkString, Null: true}, nil
	}
	if length < 0 || length > maxBulkLen {
		return Value{}, fmt.Errorf("invalid bulk length %d", length)
	}

	// Read exactly length bytes + the trailing \r\n.
	// We use the length (not "read until \r\n") because the data itself
	// may contain \r\n — that's why it's called "binary safe".
	buf := make([]byte, length+2)
	if _, err := io.ReadFull(rr.r, buf); err != nil {
		return Value{}, err
	}
	if buf[length] != '\r' || buf[length+1] != '\n' {
		return Value{}, errors.New("bulk string not terminated by \\r\\n")
	}
	return Value{Type: BulkString, Str: string(buf[:length])}, nil
}

// ---------- Writing (turning a reply into bytes) ----------

// Marshal converts a Value into RESP bytes ready to send.
func (v Value) Marshal() []byte {
	switch v.Type {
	case SimpleString:
		return []byte("+" + v.Str + "\r\n")
	case Error:
		return []byte("-" + v.Str + "\r\n")
	case Integer:
		return []byte(":" + strconv.FormatInt(v.Num, 10) + "\r\n")
	case BulkString:
		if v.Null {
			return []byte("$-1\r\n")
		}
		return []byte("$" + strconv.Itoa(len(v.Str)) + "\r\n" + v.Str + "\r\n")
	case Array:
		if v.Null {
			return []byte("*-1\r\n")
		}
		out := []byte("*" + strconv.Itoa(len(v.Array)) + "\r\n")
		for _, item := range v.Array {
			out = append(out, item.Marshal()...)
		}
		return out
	default:
		return []byte("-ERR internal: unknown reply type\r\n")
	}
}

// Small helpers so command code reads nicely: return OK(), Bulk("x"), ...
func OK() Value                 { return Value{Type: SimpleString, Str: "OK"} }
func Simple(s string) Value     { return Value{Type: SimpleString, Str: s} }
func Err(msg string) Value      { return Value{Type: Error, Str: msg} }
func Int(n int64) Value         { return Value{Type: Integer, Num: n} }
func Bulk(s string) Value       { return Value{Type: BulkString, Str: s} }
func NullBulk() Value           { return Value{Type: BulkString, Null: true} }
func ArrayOf(vs ...Value) Value { return Value{Type: Array, Array: vs} }
