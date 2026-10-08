package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// The database itself (Store) lives in store.go, persistence in aof.go.
// This file turns parsed commands into calls on the Store, records writes
// (propagate), and builds replies.

type command struct {
	fn func(srv *Server, args []string) Value

	// write = this command can change data. Write commands run one at a time
	// (see Server.writeMu) so they're logged in exactly the order they happen.
	write bool
}

// The command table: name -> handler.
var commands = map[string]command{
	"PING":    {fn: cmdPing},
	"ECHO":    {fn: cmdEcho},
	"GET":     {fn: cmdGet},
	"EXISTS":  {fn: cmdExists},
	"TTL":     {fn: cmdTTL},
	"PTTL":    {fn: cmdPTTL},
	"COMMAND": {fn: cmdCommand},
	"INFO":    {fn: cmdInfo},

	"SET":       {fn: cmdSet, write: true},
	"DEL":       {fn: cmdDel, write: true},
	"EXPIRE":    {fn: cmdExpire, write: true},
	"PEXPIRE":   {fn: cmdPExpire, write: true},
	"EXPIREAT":  {fn: cmdExpireAt, write: true},
	"PEXPIREAT": {fn: cmdPExpireAt, write: true},
	"PERSIST":   {fn: cmdPersist, write: true},
}

// Node-to-node commands used for failover (failover.go). They're added in
// init() rather than in the table above because they (indirectly) call
// execute(), which reads the table -- Go doesn't allow a variable's
// initial value to depend on itself.
func init() {
	commands["REPLSTATUS"] = command{fn: cmdReplStatus}
	commands["REPLVOTE"] = command{fn: cmdReplVote}
	commands["REPLLEADER"] = command{fn: cmdReplLeader}
}

// dispatch runs a command sent by a normal client.
func dispatch(srv *Server, v Value) Value {
	return execute(srv, v, false)
}

// execute turns a parsed RESP array like ["SET","a","1"] into a reply.
//
// internal=true means the command didn't come from a normal client: it's
// being replayed from the AOF or was sent by our leader. Those are allowed
// to write even on a read-only follower.
func execute(srv *Server, v Value, internal bool) Value {
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
	cmd, ok := commands[name]
	if !ok {
		return Err(fmt.Sprintf("ERR unknown command '%s'", parts[0]))
	}

	// Followers are read-only for clients. If clients could write to a
	// follower, its data would quietly drift away from the leader's.
	if cmd.write && !internal && srv.isReplica.Load() {
		return Err("READONLY You can't write against a read only replica.")
	}

	if cmd.write {
		srv.writeMu.Lock()
		defer srv.writeMu.Unlock()

		// A leader cut off from the majority of the cluster refuses writes
		// (see hasQuorumLocked). Same error text as real Redis.
		if !internal && !srv.isReplica.Load() && !srv.hasQuorumLocked() {
			return Err("NOREPLICAS Not enough good replicas to write.")
		}
	}
	return cmd.fn(srv, parts[1:])
}

// ---------- Shared helpers and error replies (same text as real Redis) ----------

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

// toDuration converts n units (seconds or milliseconds) to a time.Duration,
// refusing numbers so big they would overflow.
func toDuration(n int64, unit time.Duration) (time.Duration, bool) {
	if n > math.MaxInt64/int64(unit) || n < math.MinInt64/int64(unit) {
		return 0, false
	}
	return time.Duration(n) * unit, true
}

// unixMs formats a time as Unix milliseconds, the form we write to the AOF.
func unixMs(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

// ---------- Basic commands ----------

// PING -> PONG,  PING hello -> "hello"
func cmdPing(srv *Server, args []string) Value {
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
func cmdEcho(srv *Server, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("echo")
	}
	return Bulk(args[0])
}

// GET key -> value, or (nil) if missing or expired
func cmdGet(srv *Server, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("get")
	}
	val, ok := srv.store.Get(args[0])
	if !ok {
		return NullBulk()
	}
	return Bulk(val)
}

// SET key value [EX s | PX ms | EXAT unix-s | PXAT unix-ms] [NX | XX]
//
//	SET otp 1234 EX 30   -> expires in 30 seconds
//	SET lock me NX       -> only if "lock" doesn't exist yet (returns nil if it does)
//	SET name x XX        -> only if "name" already exists
//
// EXAT/PXAT give an absolute time. Mostly used by our own AOF, but real
// Redis supports them too.
func cmdSet(srv *Server, args []string) Value {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0], args[1]

	var expireAt time.Time // zero = never expires
	hasExpiry := false
	cond := SetAlways

	// Walk through the options after key and value.
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpiry || i+1 >= len(args) {
				return errSyntax // two expiry options, or no number after it
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errNotInt
			}
			if n <= 0 {
				return errInvalidExpire("set")
			}
			switch opt {
			case "EX", "PX":
				unit := time.Second
				if opt == "PX" {
					unit = time.Millisecond
				}
				d, ok := toDuration(n, unit)
				if !ok {
					return errInvalidExpire("set")
				}
				expireAt = srv.store.now().Add(d)
			case "EXAT":
				expireAt = time.Unix(n, 0)
			case "PXAT":
				expireAt = time.UnixMilli(n)
			}
			hasExpiry = true
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

	if !srv.store.Set(key, val, expireAt, cond) {
		return NullBulk() // NX/XX condition not met: Redis replies (nil), nothing to log
	}

	// Log the normalised form: relative EX/PX becomes an absolute PXAT, and
	// NX/XX are dropped (the condition already passed, replay must just set it).
	if expireAt.IsZero() {
		srv.propagate("SET", key, val)
	} else {
		srv.propagate("SET", key, val, "PXAT", unixMs(expireAt))
	}
	return OK()
}

// ---------- Key commands ----------

// DEL key [key ...] -> number of keys actually deleted
func cmdDel(srv *Server, args []string) Value {
	if len(args) < 1 {
		return wrongArgs("del")
	}
	n := srv.store.Del(args...)
	if n > 0 {
		srv.propagate(append([]string{"DEL"}, args...)...)
	}
	return Int(int64(n))
}

// EXISTS key [key ...] -> how many of them exist
func cmdExists(srv *Server, args []string) Value {
	if len(args) < 1 {
		return wrongArgs("exists")
	}
	return Int(int64(srv.store.Exists(args...)))
}

// expireAtGeneric sets an absolute expiry and logs it as PEXPIREAT.
func expireAtGeneric(srv *Server, key string, at time.Time) Value {
	ok := srv.store.ExpireAt(key, at)
	if ok {
		srv.propagate("PEXPIREAT", key, unixMs(at))
	}
	return boolInt(ok)
}

// relativeExpire handles EXPIRE (seconds) and PEXPIRE (milliseconds).
func relativeExpire(srv *Server, args []string, name string, unit time.Duration) Value {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	n, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInt
	}
	d, ok := toDuration(n, unit)
	if !ok {
		return errInvalidExpire(name)
	}
	return expireAtGeneric(srv, args[0], srv.store.now().Add(d))
}

// absoluteExpire handles EXPIREAT (unix seconds) and PEXPIREAT (unix ms).
func absoluteExpire(srv *Server, args []string, name string, millis bool) Value {
	if len(args) != 2 {
		return wrongArgs(name)
	}
	n, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return errNotInt
	}
	at := time.Unix(n, 0)
	if millis {
		at = time.UnixMilli(n)
	}
	return expireAtGeneric(srv, args[0], at)
}

// EXPIRE key seconds -> 1 if the timeout was set, 0 if the key doesn't exist
func cmdExpire(srv *Server, args []string) Value {
	return relativeExpire(srv, args, "expire", time.Second)
}

// PEXPIRE key milliseconds
func cmdPExpire(srv *Server, args []string) Value {
	return relativeExpire(srv, args, "pexpire", time.Millisecond)
}

// EXPIREAT key unix-seconds
func cmdExpireAt(srv *Server, args []string) Value {
	return absoluteExpire(srv, args, "expireat", false)
}

// PEXPIREAT key unix-milliseconds
func cmdPExpireAt(srv *Server, args []string) Value {
	return absoluteExpire(srv, args, "pexpireat", true)
}

// TTL key -> seconds left, -1 if no expiry, -2 if the key doesn't exist
func cmdTTL(srv *Server, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("ttl")
	}
	ms := srv.store.TTL(args[0])
	if ms < 0 {
		return Int(ms) // -1 or -2 pass straight through
	}
	// Round to the nearest second, the same way Redis does.
	return Int((ms + 500) / 1000)
}

// PTTL key -> like TTL but in milliseconds
func cmdPTTL(srv *Server, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("pttl")
	}
	return Int(srv.store.TTL(args[0]))
}

// PERSIST key -> 1 if an expiry was removed, 0 otherwise
func cmdPersist(srv *Server, args []string) Value {
	if len(args) != 1 {
		return wrongArgs("persist")
	}
	ok := srv.store.Persist(args[0])
	if ok {
		srv.propagate("PERSIST", args[0])
	}
	return boolInt(ok)
}

// redis-cli sends "COMMAND DOCS" when it starts, to get help text for
// autocomplete. Replying with an empty array keeps it happy.
func cmdCommand(srv *Server, args []string) Value {
	return ArrayOf()
}

// INFO [replication] -> status text, e.g. role, connected followers, offsets.
// We only implement the replication section; other sections return it too.
func cmdInfo(srv *Server, args []string) Value {
	if len(args) > 1 {
		return wrongArgs("info")
	}
	return Bulk(srv.replicationInfo())
}
