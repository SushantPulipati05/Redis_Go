package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The database itself (Store) lives in store.go.
// This file turns parsed commands into calls on the Store and builds replies.

// A command handler receives the arguments (everything after the command name)
// and returns the reply to send back.
type commandFunc func(s *Store, args []string) Value

// The command table: name -> handler. Adding a command = adding one line here.
var commands = map[string]commandFunc{
	"PING":    cmdPing,
	"ECHO":    cmdEcho,
	"SET":     cmdSet,
	"GET":     cmdGet,
	"DEL":     cmdDel,
	"EXISTS":  cmdExists,
	"EXPIRE":  cmdExpire,
	"PEXPIRE": cmdPExpire,
	"TTL":     cmdTTL,
	"PTTL":    cmdPTTL,
	"PERSIST": cmdPersist,
	"COMMAND": cmdCommand,
}

// dispatch turns a parsed RESP array like ["SET","a","1"] into a reply.
func dispatch(s *Store, v Value) Value {
	if v.Type != Array || v.Null || len(v.Array) == 0 {
		return Err("ERR Protocol error: expected a command array")
	}

	// Convert every element to a plain string; commands must be bulk strings.
	parts := make([]string, len(v.Array))
	for i, item := range v.Array {
		if item.Type != BulkString || item.Null {
			return Err("ERR Protocol error: command parts must be bulk strings")
		}
		parts[i] = item.Str
	}

	// Commands are case-insensitive: "ping", "PIng" and "PING" all work.
	name := strings.ToUpper(parts[0])
	handler, ok := commands[name]
	if !ok {
		return Err(fmt.Sprintf("ERR unknown command '%s'", parts[0]))
	}
	return handler(s, parts[1:])
}

// ---------- Shared error replies (same text as real Redis) ----------

func wrongArgs(cmd string) Value {
	return Err(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd)))
}

var (
	errSyntax = Err("ERR syntax error")
	errNotInt = Err("ERR value is not an integer or out of range")
)

func errInvalidExpire(cmd string) Value {
	return Err(fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
}

// boolInt converts true/false to the 1/0 integers Redis uses for yes/no replies.
func boolInt(b bool) Value {
	if b {
		return Int(1)
	}
	return Int(0)
}

// ---------- Basic commands ----------

// PING -> PONG,  PING hello -> "hello"
func cmdPing(s *Store, args []string) Value {
	switch len(args) {
	case 0:
		return Simple("PONG")
	case 1:
		return Bulk(args[0])
	default:
		return wrongArgs("ping")
	}
}

// ECHO hello -> "hello"
func cmdEcho(s *Store, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("echo")
	}
	return Bulk(args[0])
}

// GET key -> value, or (nil) if missing or expired
func cmdGet(s *Store, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("get")
	}
	val, ok := s.Get(args[0])
	if !ok {
		return NullBulk()
	}
	return Bulk(val)
}

// SET key value [EX seconds | PX milliseconds] [NX | XX]
//
//	SET otp 1234 EX 30   -> expires in 30 seconds
//	SET lock me NX       -> only if "lock" doesn't exist yet (returns nil if it does)
//	SET name x XX        -> only if "name" already exists
func cmdSet(s *Store, args []string) Value {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0], args[1]

	var ttl time.Duration
	hasTTL := false
	cond := SetAlways

	// Walk through the options after key and value.
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX", "PX":
			if hasTTL || i+1 >= len(args) {
				return errSyntax // EX given twice, EX+PX together, or no number after it
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errNotInt
			}
			if n <= 0 {
				return errInvalidExpire("set")
			}
			if strings.ToUpper(args[i]) == "EX" {
				ttl = time.Duration(n) * time.Second
			} else {
				ttl = time.Duration(n) * time.Millisecond
			}
			hasTTL = true
			i++ // skip the number we just consumed
		case "NX":
			if cond == SetIfExists {
				return errSyntax // NX and XX together make no sense
			}
			cond = SetIfNotExists
		case "XX":
			if cond == SetIfNotExists {
				return errSyntax
			}
			cond = SetIfExists
		default:
			return errSyntax
		}
	}

	if !s.Set(key, val, ttl, cond) {
		return NullBulk() // NX/XX condition not met: Redis replies (nil)
	}
	return OK()
}

// ---------- Key commands ----------

// DEL key [key ...] -> number of keys actually deleted
func cmdDel(s *Store, args []string) Value {
	if len(args) < 1 {
		return wrongArgs("del")
	}
	return Int(int64(s.Del(args...)))
}

// EXISTS key [key ...] -> how many of them exist
func cmdExists(s *Store, args []string) Value {
	if len(args) < 1 {
		return wrongArgs("exists")
	}
	return Int(int64(s.Exists(args...)))
}

// expireGeneric handles EXPIRE (seconds) and PEXPIRE (milliseconds).
func expireGeneric(s *Store, args []string, name string, unit time.Duration) Value {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	n, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInt
	}
	return boolInt(s.Expire(args[0], time.Duration(n)*unit))
}

// EXPIRE key seconds -> 1 if the timeout was set, 0 if the key doesn't exist
func cmdExpire(s *Store, args []string) Value {
	return expireGeneric(s, args, "expire", time.Second)
}

// PEXPIRE key milliseconds
func cmdPExpire(s *Store, args []string) Value {
	return expireGeneric(s, args, "pexpire", time.Millisecond)
}

// TTL key -> seconds left, -1 if no expiry, -2 if the key doesn't exist
func cmdTTL(s *Store, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("ttl")
	}
	ms := s.TTL(args[0])
	if ms < 0 {
		return Int(ms) // -1 or -2 pass straight through
	}
	// Round to the nearest second, the same way Redis does.
	return Int((ms + 500) / 1000)
}

// PTTL key -> like TTL but in milliseconds
func cmdPTTL(s *Store, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("pttl")
	}
	return Int(s.TTL(args[0]))
}

// PERSIST key -> 1 if an expiry was removed, 0 otherwise
func cmdPersist(s *Store, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("persist")
	}
	return boolInt(s.Persist(args[0]))
}

// redis-cli sends "COMMAND DOCS" when it starts, to get help text for
// autocomplete. Replying with an empty array keeps it happy.
func cmdCommand(s *Store, args []string) Value {
	return ArrayOf()
}
