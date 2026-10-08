package main

import (
	"sync"
	"time"
)

// Store is the database: keys -> values, plus expiry times for keys that have one.
//
// Like real Redis, expiry info lives in a SEPARATE map (expires). Most keys
// never expire, so they cost nothing extra, and the background cleaner only
// has to look at keys that can actually expire.
//
// Concurrency: many client goroutines share one Store, so every access goes
// through mu (see the Day 1 notes: RLock = shared read, Lock = exclusive write).
type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time

	// now returns the current time. It's a field (not a direct time.Now call)
	// so tests can plug in a fake clock and "jump" time forward instantly
	// instead of sleeping for real.
	now func() time.Time
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
		now:     time.Now,
	}
}

// isExpired reports whether key has an expiry time that has passed.
// Caller must hold the lock (read or write).
func (s *Store) isExpired(key string) bool {
	exp, has := s.expires[key]
	return has && !s.now().Before(exp)
}

// deleteKey removes a key and its expiry. Caller must hold the WRITE lock.
func (s *Store) deleteKey(key string) {
	delete(s.data, key)
	delete(s.expires, key)
}

// exists reports whether key is present and not expired, lazily deleting it
// if it has expired. Caller must hold the WRITE lock.
func (s *Store) exists(key string) bool {
	if _, ok := s.data[key]; !ok {
		return false
	}
	if s.isExpired(key) {
		s.deleteKey(key)
		return false
	}
	return true
}

// Get returns the value for key. Expired keys are treated as missing.
//
// This is "lazy expiration": we don't delete a key the instant it expires;
// we notice when someone touches it. The fast path only needs a read lock.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	val, ok := s.data[key]
	expired := ok && s.isExpired(key)
	s.mu.RUnlock()

	if !expired {
		return val, ok
	}

	// Slow path: the key expired, so delete it. Deleting needs the write lock.
	// We can't upgrade a read lock to a write lock in Go, so we release and
	// re-acquire. In that gap another client might have SET the key again,
	// so we must CHECK AGAIN before deleting ("double-checked" pattern).
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exists(key) { // exists() re-checks and deletes if still expired
		return s.data[key], true
	}
	return "", false
}

// SetCondition controls SET's NX / XX options.
type SetCondition int

const (
	SetAlways      SetCondition = iota // plain SET
	SetIfNotExists                     // NX: only if the key does NOT exist
	SetIfExists                        // XX: only if the key DOES exist
)

// Set stores key=val.
//
// expireAt is an ABSOLUTE time ("delete at 10:05:30"), not a duration.
// The zero time.Time{} means "never expires" (and, like Redis, removes any old
// expiry). Absolute times matter for persistence: if the AOF stored "EX 10",
// replaying it after a restart would wrongly give the key a fresh 10 seconds.
//
// Returns false if NX/XX blocked the write.
//
// The existence check and the write happen under ONE lock. If we checked,
// unlocked, then locked again to write, two clients doing "SET k v NX" at
// the same moment could both see "doesn't exist" and both write. That bug
// is called check-then-act, and holding the lock across both steps prevents it.
func (s *Store) Set(key, val string, expireAt time.Time, cond SetCondition) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	present := s.exists(key)
	if cond == SetIfNotExists && present {
		return false
	}
	if cond == SetIfExists && !present {
		return false
	}

	// An expiry time that's already in the past means the key is dead on
	// arrival (this happens when replaying an old AOF entry).
	if !expireAt.IsZero() && !s.now().Before(expireAt) {
		s.deleteKey(key)
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

// Del removes keys and returns how many actually existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if s.exists(k) {
			s.deleteKey(k)
			n++
		}
	}
	return n
}

// Exists returns how many of the given keys exist. Like Redis, a key named
// twice is counted twice: EXISTS a a -> 2.
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

// Special TTL results, same numbers Redis returns.
const (
	TTLMissing  = -2 // key does not exist
	TTLNoExpiry = -1 // key exists but never expires
)

// TTL returns the remaining time to live in milliseconds,
// or TTLMissing / TTLNoExpiry.
func (s *Store) TTL(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(key) {
		return TTLMissing
	}
	exp, has := s.expires[key]
	if !has {
		return TTLNoExpiry
	}
	return exp.Sub(s.now()).Milliseconds()
}

// ExpireAt makes an existing key expire at an absolute time. Returns false if
// the key doesn't exist. A time that's already passed deletes the key now
// (Redis behaviour for EXPIRE with 0 or a negative number).
func (s *Store) ExpireAt(key string, at time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(key) {
		return false
	}
	if !s.now().Before(at) {
		s.deleteKey(key)
		return true
	}
	s.expires[key] = at
	return true
}

// Persist removes a key's expiry. Returns true if there was one to remove.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(key) {
		return false
	}
	if _, has := s.expires[key]; !has {
		return false
	}
	delete(s.expires, key)
	return true
}

// ---------- Active expiration (background cleaner) ----------
//
// Lazy expiration alone has a leak: a key that expires and is NEVER read again
// stays in memory forever. So, like real Redis, we also run a background job:
//
//   every 100ms:
//     sample up to 20 keys that have an expiry
//     delete the ones that have expired
//     if more than 25% of the sample was expired, there are probably many
//     more, so repeat immediately; otherwise wait for the next tick.
//
// Sampling (instead of scanning every key) keeps each pass cheap, so the
// cleaner never freezes the server even with millions of keys.

const (
	expireSampleSize = 20
	expireInterval   = 100 * time.Millisecond
)

// activeExpireCycle runs one sampling pass and returns how many keys it deleted
// and how many it sampled.
func (s *Store) activeExpireCycle() (deleted, sampled int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ranging over a Go map visits keys in a randomised order, which gives us
	// a cheap random sample: just stop after expireSampleSize keys.
	for key, exp := range s.expires {
		if sampled == expireSampleSize {
			break
		}
		sampled++
		if !s.now().Before(exp) {
			s.deleteKey(key)
			deleted++
		}
	}
	return deleted, sampled
}

// RunActiveExpiry loops forever; start it once with `go store.RunActiveExpiry()`.
func (s *Store) RunActiveExpiry() {
	ticker := time.NewTicker(expireInterval)
	defer ticker.Stop()
	for range ticker.C {
		for {
			deleted, sampled := s.activeExpireCycle()
			// Stop when the sample was mostly clean (<= 25% expired).
			if sampled == 0 || deleted*4 <= sampled {
				break
			}
		}
	}
}

// ---------- Used by replication ----------

// Flush deletes every key. A follower calls this before loading a fresh
// snapshot from its leader.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string)
	s.expires = make(map[string]time.Time)
}

// SnapshotRESP returns the whole database as a list of SET commands in RESP
// format, e.g. "SET name sushant" and "SET otp 1234 PXAT 1791391290425".
// A new follower replays these to get an exact copy of the data.
func (s *Store) SnapshotRESP() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []byte
	for key, val := range s.data {
		if s.isExpired(key) {
			continue // don't ship keys that are already dead
		}
		cmd := []Value{Bulk("SET"), Bulk(key), Bulk(val)}
		if exp, has := s.expires[key]; has {
			cmd = append(cmd, Bulk("PXAT"), Bulk(unixMs(exp)))
		}
		out = append(out, ArrayOf(cmd...).Marshal()...)
	}
	return out
}
