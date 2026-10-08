package server

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

type command struct {
	fn    func(srv *Server, args []string) resp.Value
	write bool
}

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

// Registered in init because these handlers reach execute indirectly,
// which would otherwise be an initialization cycle.
func init() {
	commands["REPLSTATUS"] = command{fn: cmdReplStatus}
	commands["REPLVOTE"] = command{fn: cmdReplVote}
	commands["REPLLEADER"] = command{fn: cmdReplLeader}
	commands["WAIT"] = command{fn: cmdWait}
}

// dispatch runs a command from a client.
func dispatch(srv *Server, v resp.Value) resp.Value {
	return execute(srv, v, false)
}

// execute runs a command. internal is true for commands replayed from the AOF
// or received from the leader; those may write on a read-only follower.
func execute(srv *Server, v resp.Value, internal bool) resp.Value {
	if v.Type != resp.Array || v.Null || len(v.Array) == 0 {
		return resp.Err("ERR Protocol error: expected a command array")
	}

	parts := make([]string, len(v.Array))
	for i, item := range v.Array {
		if item.Type != resp.BulkString || item.Null {
			return resp.Err("ERR Protocol error: command parts must be bulk strings")
		}
		parts[i] = item.Str
	}

	name := strings.ToUpper(parts[0])
	cmd, ok := commands[name]
	if !ok {
		return resp.Err(fmt.Sprintf("ERR unknown command '%s'", parts[0]))
	}

	if cmd.write && !internal && srv.isReplica.Load() {
		return resp.Err("READONLY You can't write against a read only replica.")
	}

	if cmd.write {
		srv.writeMu.Lock()
		defer srv.writeMu.Unlock()

		// A leader that can't reach a majority may have been partitioned
		// away; refusing writes keeps the two sides from diverging.
		if !internal && !srv.isReplica.Load() && !srv.hasQuorumLocked() {
			return resp.Err("NOREPLICAS Not enough good replicas to write.")
		}
	}
	return cmd.fn(srv, parts[1:])
}

func wrongArgs(cmd string) resp.Value {
	return resp.Err(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd)))
}

var (
	errSyntax = resp.Err("ERR syntax error")
	errNotInt = resp.Err("ERR value is not an integer or out of range")
)

func errInvalidExpire(cmd string) resp.Value {
	return resp.Err(fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
}

func boolInt(b bool) resp.Value {
	if b {
		return resp.Int(1)
	}
	return resp.Int(0)
}

// toDuration converts n units to a Duration, rejecting values that overflow.
func toDuration(n int64, unit time.Duration) (time.Duration, bool) {
	if n > math.MaxInt64/int64(unit) || n < math.MinInt64/int64(unit) {
		return 0, false
	}
	return time.Duration(n) * unit, true
}

func unixMs(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}
