package main

import (
	"fmt"
	"strings"
	"sync"
)

// Store is the actual database: a map from key to value.
//
// Many client goroutines use it at the same time, and Go maps are NOT safe
// for concurrent writes (two goroutines writing at once can crash the program).
// So every access goes through a lock:
//   - RLock: many readers at once are fine (GET, GET, GET...)
//   - Lock:  a writer gets exclusive access (SET waits for readers to finish)
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val
}

// A command handler receives the arguments (everything after the command name)
// and returns the reply to send back.
type commandFunc func(s *Store, args []string) Value

// The command table: name -> handler. Adding a command = adding one line here.
var commands = map[string]commandFunc{
	"PING":    cmdPing,
	"ECHO":    cmdEcho,
	"SET":     cmdSet,
	"GET":     cmdGet,
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

// Same error text as real Redis, so differential testing can compare replies later.
func wrongArgs(cmd string) Value {
	return Err(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd)))
}

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

// SET key value -> OK   (options like EX come on Day 2)
func cmdSet(s *Store, args []string) Value {
	if len(args) != 2 {
		return wrongArgs("set")
	}
	s.Set(args[0], args[1])
	return OK()
}

// GET key -> value, or (nil) if the key doesn't exist
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

// redis-cli sends "COMMAND DOCS" when it starts, to get help text for
// autocomplete. Replying with an empty array keeps it happy.
func cmdCommand(s *Store, args []string) Value {
	return ArrayOf()
}
