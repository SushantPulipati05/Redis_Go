// Package store is the in-memory keyspace: string values with optional expiry.
package store

import (
	"sync"
	"time"
)

// Store is safe for concurrent use. Expiry times live in a separate map, so
// keys without a TTL cost nothing extra and the active expirer only scans
// keys that can expire.
type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time
	now     func() time.Time
}

func New() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
		now:     time.Now,
	}
}

// SetClock replaces the time source. Used by tests.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

// Now returns the store's current time.
func (s *Store) Now() time.Time { return s.now() }

// Condition restricts when Set writes.
type Condition int

const (
	Always      Condition = iota
	IfNotExists           // NX
	IfExists              // XX
)

// Special TTL results, matching Redis.
const (
	TTLMissing  = -2
	TTLNoExpiry = -1
)

// Caller must hold mu (read or write).
func (s *Store) isExpired(key string) bool {
	exp, ok := s.expires[key]
	return ok && !s.now().Before(exp)
}

// Caller must hold mu for writing.
func (s *Store) remove(key string) {
	delete(s.data, key)
	delete(s.expires, key)
}

// exists reports whether key is live, deleting it if it has expired.
// Caller must hold mu for writing.
func (s *Store) exists(key string) bool {
	if _, ok := s.data[key]; !ok {
		return false
	}
	if s.isExpired(key) {
		s.remove(key)
		return false
	}
	return true
}

// Get returns the value of key. Expired keys are deleted lazily on access.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	val, ok := s.data[key]
	expired := ok && s.isExpired(key)
	s.mu.RUnlock()
	if !expired {
		return val, ok
	}

	// Re-check under the write lock: the key may have been set again
	// between releasing the read lock and acquiring this one.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exists(key) {
		return s.data[key], true
	}
	return "", false
}

// Set stores val under key. expireAt is absolute; the zero time means no
// expiry and clears any existing one. It reports false if cond prevented the
// write. The condition check and the write happen under one lock.
func (s *Store) Set(key, val string, expireAt time.Time, cond Condition) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	present := s.exists(key)
	if (cond == IfNotExists && present) || (cond == IfExists && !present) {
		return false
	}
	if !expireAt.IsZero() && !s.now().Before(expireAt) {
		s.remove(key)
		return true
	}
	s.data[key] = val
	if expireAt.IsZero() {
		delete(s.expires, key)
	} else {
		s.expires[key] = expireAt
	}
	return true
}

// Del deletes keys and returns how many existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if s.exists(k) {
			s.remove(k)
			n++
		}
	}
	return n
}

// Exists counts how many of keys exist. Repeated keys are counted each time.
func (s *Store) Exists(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if s.exists(k) {
			n++
		}
	}
	return n
}

// TTL returns the remaining time to live in milliseconds, or TTLMissing / TTLNoExpiry.
func (s *Store) TTL(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(key) {
		return TTLMissing
	}
	exp, ok := s.expires[key]
	if !ok {
		return TTLNoExpiry
	}
	return exp.Sub(s.now()).Milliseconds()
}

// ExpireAt sets an absolute expiry on an existing key. A time in the past
// deletes the key. It reports false if the key does not exist.
func (s *Store) ExpireAt(key string, at time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(key) {
		return false
	}
	if !s.now().Before(at) {
		s.remove(key)
		return true
	}
	s.expires[key] = at
	return true
}

// Persist removes the expiry of key, reporting whether it had one.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(key) {
		return false
	}
	if _, ok := s.expires[key]; !ok {
		return false
	}
	delete(s.expires, key)
	return true
}

// Flush deletes every key.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string)
	s.expires = make(map[string]time.Time)
}

// Entry is one key in a snapshot. ExpireAt is zero if the key never expires.
type Entry struct {
	Key, Value string
	ExpireAt   time.Time
}

// Snapshot returns every live key.
func (s *Store) Snapshot() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Entry, 0, len(s.data))
	for k, v := range s.data {
		if s.isExpired(k) {
			continue
		}
		out = append(out, Entry{Key: k, Value: v, ExpireAt: s.expires[k]})
	}
	return out
}

// Len returns the number of keys, including expired ones not yet removed.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
