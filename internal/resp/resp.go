// Package resp implements the Redis serialization protocol (RESP2).
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Type is the first byte of a RESP value.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

const (
	maxBulkLen  = 512 * 1024 * 1024
	maxArrayLen = 1024 * 1024
)

// Value is a single RESP value. Only the fields relevant to Type are set.
type Value struct {
	Type  Type
	Str   string
	Num   int64
	Array []Value
	Null  bool
}

// Reader reads RESP values from a stream. It is buffered, so a value split
// across several TCP reads, or several values in one read, are handled.
type Reader struct {
	r *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(rd)}
}

// Read returns the next complete value.
func (rr *Reader) Read() (Value, error) {
	b, err := rr.r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch t := Type(b); t {
	case Array:
		return rr.readArray()
	case BulkString:
		return rr.readBulk()
	case SimpleString, Error:
		line, err := rr.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: t, Str: line}, nil
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
		return Value{}, fmt.Errorf("unknown RESP type byte %q", b)
	}
}

// Buffered returns the number of bytes read from the source but not yet consumed.
func (rr *Reader) Buffered() int {
	return rr.r.Buffered()
}

func (rr *Reader) readLine() (string, error) {
	line, err := rr.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("line does not end with \\r\\n")
	}
	return line[:len(line)-2], nil
}

func (rr *Reader) readInt() (int, error) {
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

func (rr *Reader) readArray() (Value, error) {
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
		v, err := rr.Read()
		if err != nil {
			return Value{}, err
		}
		items = append(items, v)
	}
	return Value{Type: Array, Array: items}, nil
}

// readBulk reads by length rather than scanning for \r\n, so values may
// contain arbitrary bytes.
func (rr *Reader) readBulk() (Value, error) {
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

	buf := make([]byte, length+2)
	if _, err := io.ReadFull(rr.r, buf); err != nil {
		return Value{}, err
	}
	if buf[length] != '\r' || buf[length+1] != '\n' {
		return Value{}, errors.New("bulk string not terminated by \\r\\n")
	}
	return Value{Type: BulkString, Str: string(buf[:length])}, nil
}

// Marshal encodes v in wire format.
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

func OK() Value                 { return Value{Type: SimpleString, Str: "OK"} }
func Simple(s string) Value     { return Value{Type: SimpleString, Str: s} }
func Err(msg string) Value      { return Value{Type: Error, Str: msg} }
func Int(n int64) Value         { return Value{Type: Integer, Num: n} }
func Bulk(s string) Value       { return Value{Type: BulkString, Str: s} }
func NullBulk() Value           { return Value{Type: BulkString, Null: true} }
func ArrayOf(vs ...Value) Value { return Value{Type: Array, Array: vs} }

// Command encodes a command as an array of bulk strings.
func Command(args ...string) []byte {
	vals := make([]Value, len(args))
	for i, a := range args {
		vals[i] = Bulk(a)
	}
	return ArrayOf(vals...).Marshal()
}

// Name returns the upper-cased command name of v, or "" if v is not a command.
func (v Value) Name() string {
	if v.Type != Array || len(v.Array) == 0 || v.Array[0].Type != BulkString {
		return ""
	}
	return strings.ToUpper(v.Array[0].Str)
}
