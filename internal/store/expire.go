package store

import "time"

// Active expiration, following Redis: every 100ms sample 20 keys that have a
// TTL and delete the expired ones. If more than 25% of the sample had
// expired, sample again straight away. Without this, keys that expire and are
// never read again would stay in memory forever.

const (
	expireSampleSize = 20
	expireInterval   = 100 * time.Millisecond
)

// expireCycle runs one sampling pass. Map iteration order is randomised,
// which gives a cheap random sample.
func (s *Store) expireCycle() (deleted, sampled int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for key, exp := range s.expires {
		if sampled == expireSampleSize {
			break
		}
		sampled++
		if !now.Before(exp) {
			s.remove(key)
			deleted++
		}
	}
	return deleted, sampled
}

// expireUntilClean repeats expireCycle while more than 25% of samples expired.
func (s *Store) expireUntilClean() {
	for {
		deleted, sampled := s.expireCycle()
		if sampled == 0 || deleted*4 <= sampled {
			return
		}
	}
}

// RunActiveExpiry runs active expiration until done is closed.
func (s *Store) RunActiveExpiry(done <-chan struct{}) {
	ticker := time.NewTicker(expireInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			s.expireUntilClean()
		}
	}
}
