package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore() (*Store, *fakeClock) {
	c := &fakeClock{t: time.Unix(1_000_000, 0)}
	s := New()
	s.SetClock(c.now)
	return s, c
}

func TestKeyExpires(t *testing.T) {
	s, c := newTestStore()
	s.Set("otp", "1234", c.now().Add(10*time.Second), Always)

	c.advance(9 * time.Second)
	if v, ok := s.Get("otp"); !ok || v != "1234" {
		t.Fatalf("before expiry: %q %v", v, ok)
	}
	c.advance(time.Second)
	if _, ok := s.Get("otp"); ok {
		t.Fatal("key should have expired")
	}
}

func TestSetClearsExpiry(t *testing.T) {
	s, c := newTestStore()
	s.Set("k", "1", c.now().Add(5*time.Second), Always)
	s.Set("k", "2", time.Time{}, Always)
	c.advance(10 * time.Second)
	if v, ok := s.Get("k"); !ok || v != "2" {
		t.Fatalf("got %q %v", v, ok)
	}
}

func TestSetWithPastExpiryDeletes(t *testing.T) {
	s, c := newTestStore()
	s.Set("k", "v", time.Time{}, Always)
	s.Set("k", "v", c.now().Add(-time.Second), Always)
	if s.Exists("k") != 0 {
		t.Fatal("a key set with an expiry in the past should not exist")
	}
}

func TestConditions(t *testing.T) {
	s, _ := newTestStore()
	if !s.Set("k", "a", time.Time{}, IfNotExists) {
		t.Fatal("NX on a missing key should write")
	}
	if s.Set("k", "b", time.Time{}, IfNotExists) {
		t.Fatal("NX on an existing key should not write")
	}
	if s.Set("other", "x", time.Time{}, IfExists) {
		t.Fatal("XX on a missing key should not write")
	}
	if v, _ := s.Get("k"); v != "a" {
		t.Fatalf("got %q", v)
	}
}

func TestTTL(t *testing.T) {
	s, c := newTestStore()
	if got := s.TTL("missing"); got != TTLMissing {
		t.Errorf("missing key: %d", got)
	}
	s.Set("k", "v", time.Time{}, Always)
	if got := s.TTL("k"); got != TTLNoExpiry {
		t.Errorf("no expiry: %d", got)
	}
	s.ExpireAt("k", c.now().Add(1500*time.Millisecond))
	if got := s.TTL("k"); got != 1500 {
		t.Errorf("ttl: %d", got)
	}
	if !s.Persist("k") || s.TTL("k") != TTLNoExpiry {
		t.Error("persist should remove the expiry")
	}
}

func TestActiveExpiryRemovesUnreadKeys(t *testing.T) {
	s, c := newTestStore()
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("k%d", i), "v", c.now().Add(time.Second), Always)
	}
	s.Set("keep", "v", time.Time{}, Always)

	c.advance(2 * time.Second)
	s.expireUntilClean()

	if n := s.Len(); n != 1 {
		t.Fatalf("expected only 'keep' to remain, %d keys left", n)
	}
}

func TestSnapshot(t *testing.T) {
	s, c := newTestStore()
	exp := c.now().Add(time.Minute)
	s.Set("a", "1", time.Time{}, Always)
	s.Set("b", "2", exp, Always)
	s.Set("gone", "x", c.now().Add(time.Second), Always)
	c.advance(2 * time.Second)

	got := map[string]Entry{}
	for _, e := range s.Snapshot() {
		got[e.Key] = e
	}
	if len(got) != 2 || got["a"].Value != "1" || !got["b"].ExpireAt.Equal(exp) {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
}

func TestConcurrentNXHasOneWinner(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	var winners atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Set("lock", "x", time.Time{}, IfNotExists) {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", winners.Load())
	}
}
