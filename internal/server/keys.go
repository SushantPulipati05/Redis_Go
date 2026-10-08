package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
	"github.com/SushantPulipati05/Redis_Go/internal/store"
)

func cmdPing(srv *Server, args []string) resp.Value {
	switch len(args) {
	case 0:
		return resp.Simple("PONG")
	case 1:
		return resp.Bulk(args[0])
	default:
		return wrongArgs("ping")
	}
}

func cmdEcho(srv *Server, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgs("echo")
	}
	return resp.Bulk(args[0])
}

func cmdGet(srv *Server, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgs("get")
	}
	val, ok := srv.store.Get(args[0])
	if !ok {
		return resp.NullBulk()
	}
	return resp.Bulk(val)
}

// SET key value [EX s | PX ms | EXAT unix-s | PXAT unix-ms] [NX | XX]
func cmdSet(srv *Server, args []string) resp.Value {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0], args[1]

	var expireAt time.Time
	hasExpiry := false
	cond := store.Always

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpiry || i+1 >= len(args) {
				return errSyntax
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
				expireAt = srv.store.Now().Add(d)
			case "EXAT":
				expireAt = time.Unix(n, 0)
			case "PXAT":
				expireAt = time.UnixMilli(n)
			}
			hasExpiry = true
			i++
		case "NX":
			if cond == store.IfExists {
				return errSyntax
			}
			cond = store.IfNotExists
		case "XX":
			if cond == store.IfNotExists {
				return errSyntax
			}
			cond = store.IfExists
		default:
			return errSyntax
		}
	}

	if !srv.store.Set(key, val, expireAt, cond) {
		return resp.NullBulk()
	}

	// Relative expiries are logged as absolute ones so a replay after
	// downtime doesn't extend the key's lifetime.
	if expireAt.IsZero() {
		srv.propagate("SET", key, val)
	} else {
		srv.propagate("SET", key, val, "PXAT", unixMs(expireAt))
	}
	return resp.OK()
}

func cmdDel(srv *Server, args []string) resp.Value {
	if len(args) < 1 {
		return wrongArgs("del")
	}
	n := srv.store.Del(args...)
	if n > 0 {
		srv.propagate(append([]string{"DEL"}, args...)...)
	}
	return resp.Int(int64(n))
}

func cmdExists(srv *Server, args []string) resp.Value {
	if len(args) < 1 {
		return wrongArgs("exists")
	}
	return resp.Int(int64(srv.store.Exists(args...)))
}

func expireAtGeneric(srv *Server, key string, at time.Time) resp.Value {
	ok := srv.store.ExpireAt(key, at)
	if ok {
		srv.propagate("PEXPIREAT", key, unixMs(at))
	}
	return boolInt(ok)
}

func relativeExpire(srv *Server, args []string, name string, unit time.Duration) resp.Value {
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
	return expireAtGeneric(srv, args[0], srv.store.Now().Add(d))
}

func absoluteExpire(srv *Server, args []string, name string, millis bool) resp.Value {
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

func cmdExpire(srv *Server, args []string) resp.Value {
	return relativeExpire(srv, args, "expire", time.Second)
}

func cmdPExpire(srv *Server, args []string) resp.Value {
	return relativeExpire(srv, args, "pexpire", time.Millisecond)
}

func cmdExpireAt(srv *Server, args []string) resp.Value {
	return absoluteExpire(srv, args, "expireat", false)
}

func cmdPExpireAt(srv *Server, args []string) resp.Value {
	return absoluteExpire(srv, args, "pexpireat", true)
}

func cmdTTL(srv *Server, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgs("ttl")
	}
	ms := srv.store.TTL(args[0])
	if ms < 0 {
		return resp.Int(ms)
	}
	return resp.Int((ms + 500) / 1000) // rounded, as Redis does
}

func cmdPTTL(srv *Server, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgs("pttl")
	}
	return resp.Int(srv.store.TTL(args[0]))
}

func cmdPersist(srv *Server, args []string) resp.Value {
	if len(args) != 1 {
		return wrongArgs("persist")
	}
	ok := srv.store.Persist(args[0])
	if ok {
		srv.propagate("PERSIST", args[0])
	}
	return boolInt(ok)
}

// redis-cli sends COMMAND DOCS on start-up; an empty reply is enough.
func cmdCommand(srv *Server, args []string) resp.Value {
	return resp.ArrayOf()
}
