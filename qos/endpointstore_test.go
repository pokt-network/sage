package qos

import (
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

type epData struct {
	BlockHeight uint64
	IsArchival  bool
}

func newTestStore() *EndpointStore[epData] {
	return NewEndpointStore[epData](slog.Default())
}

func TestEndpointStore_GetSet(t *testing.T) {
	s := newTestStore()
	addr := domain.EndpointAddr("pokt1-https://node1.com")

	// Get on empty store.
	if _, ok := s.Get(addr); ok {
		t.Fatal("expected not found")
	}

	// Set and retrieve.
	s.Set(addr, epData{BlockHeight: 100, IsArchival: true})
	data, ok := s.Get(addr)
	if !ok {
		t.Fatal("expected found")
	}
	if data.BlockHeight != 100 || !data.IsArchival {
		t.Fatalf("unexpected data: %+v", data)
	}
}

func TestEndpointStore_Update(t *testing.T) {
	s := newTestStore()
	addr := domain.EndpointAddr("pokt1-https://node1.com")

	// Update on non-existent creates with zero value.
	s.Update(addr, func(d *epData) {
		d.BlockHeight = 200
	})
	data, ok := s.Get(addr)
	if !ok {
		t.Fatal("expected found after Update")
	}
	if data.BlockHeight != 200 {
		t.Fatalf("expected 200, got %d", data.BlockHeight)
	}

	// Update existing.
	s.Update(addr, func(d *epData) {
		d.IsArchival = true
	})
	data, _ = s.Get(addr)
	if !data.IsArchival || data.BlockHeight != 200 {
		t.Fatalf("unexpected data after second Update: %+v", data)
	}
}

func TestEndpointStore_Touch(t *testing.T) {
	s := newTestStore()
	a1 := domain.EndpointAddr("pokt1-https://node1.com")
	a2 := domain.EndpointAddr("pokt2-https://node2.com")

	s.Set(a1, epData{})
	s.Set(a2, epData{})

	// Touch only a1; a2 should not be affected (but we mainly test no panic).
	s.Touch(domain.EndpointAddrList{a1})

	// Touch non-existent should not panic.
	s.Touch(domain.EndpointAddrList{domain.EndpointAddr("missing")})
}

func TestEndpointStore_SweepStale(t *testing.T) {
	s := newTestStore()
	a1 := domain.EndpointAddr("pokt1-https://node1.com")
	a2 := domain.EndpointAddr("pokt2-https://node2.com")

	s.Set(a1, epData{BlockHeight: 100})
	s.Set(a2, epData{BlockHeight: 200})

	// Nothing stale yet (both just set).
	removed := s.SweepStale(time.Hour)
	if len(removed) != 0 {
		t.Fatalf("expected 0 removed, got %d", len(removed))
	}

	// Force staleness by using a very short TTL.
	time.Sleep(2 * time.Millisecond)
	removed = s.SweepStale(time.Millisecond)
	if len(removed) != 2 {
		t.Fatalf("expected 2 removed, got %d", len(removed))
	}
	if s.Count() != 0 {
		t.Fatalf("expected 0 remaining, got %d", s.Count())
	}
}

func TestEndpointStore_Range(t *testing.T) {
	s := newTestStore()
	s.Set(domain.EndpointAddr("a"), epData{BlockHeight: 1})
	s.Set(domain.EndpointAddr("b"), epData{BlockHeight: 2})
	s.Set(domain.EndpointAddr("c"), epData{BlockHeight: 3})

	var visited int
	s.Range(func(_ domain.EndpointAddr, _ epData) bool {
		visited++
		return visited < 2 // stop after 2
	})
	if visited != 2 {
		t.Fatalf("expected 2 visited, got %d", visited)
	}
}

func TestEndpointStore_Addrs(t *testing.T) {
	s := newTestStore()
	s.Set(domain.EndpointAddr("a"), epData{})
	s.Set(domain.EndpointAddr("b"), epData{})
	s.Set(domain.EndpointAddr("c"), epData{})

	addrs := s.Addrs()
	if len(addrs) != 3 {
		t.Fatalf("expected 3, got %d", len(addrs))
	}

	// Sort for deterministic comparison.
	sort.Slice(addrs, func(i, j int) bool { return addrs[i] < addrs[j] })
	if addrs[0] != "a" || addrs[1] != "b" || addrs[2] != "c" {
		t.Fatalf("unexpected addrs: %v", addrs)
	}
}

func TestEndpointStore_NilLogger(t *testing.T) {
	// Should not panic with nil logger.
	s := NewEndpointStore[int](nil)
	s.Set(domain.EndpointAddr("x"), 42)
	if s.Count() != 1 {
		t.Fatal("expected count 1")
	}
}

// The three plugins used to write this closure themselves and disagreed about
// a stored height of 0: two let the endpoint through as unjudgeable, one
// filtered it out as hopelessly stale. Pinning the answer here is the point of
// having one implementation.
func TestHeightGetter(t *testing.T) {
	type ep struct{ height uint64 }
	store := NewEndpointStore[ep](nil)

	store.Update("pokt1a-https://a.example.com", func(e *ep) { e.height = 500 })
	store.Update("pokt1zero-https://zero.example.com", func(e *ep) { e.height = 0 })

	get := HeightGetter(store, func(e ep) uint64 { return e.height }, HeightProjection{})

	if h, ok := get("pokt1a-https://a.example.com"); !ok || h != 500 {
		t.Errorf("known endpoint: got (%d, %v), want (500, true)", h, ok)
	}
	if _, ok := get("pokt1missing-https://missing.example.com"); ok {
		t.Error("an endpoint absent from the store must report unknown")
	}
	if _, ok := get("pokt1zero-https://zero.example.com"); ok {
		t.Error("a stored height of 0 must report unknown, not a real height of zero — filtering on it penalizes an endpoint for our own missing data")
	}
}

// TestEndpointStore_Clear drops every stored endpoint at once, the way an
// operator-triggered chain-state reset needs to.
func TestEndpointStore_Clear(t *testing.T) {
	s := newTestStore()
	s.Set("a", epData{BlockHeight: 1})
	s.Set("b", epData{BlockHeight: 2})
	if got := s.Count(); got != 2 {
		t.Fatalf("Count() = %d before Clear, want 2", got)
	}

	s.Clear()

	if got := s.Count(); got != 0 {
		t.Fatalf("Count() = %d after Clear, want 0", got)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected \"a\" gone after Clear")
	}
}

// A registration the store has never seen — a session rollover's new address
// for a known backend — takes its host's latest reading, while that reading
// is recent. Its own reading, once it has one, wins.
func TestHeightGetter_FallsBackToHostReading(t *testing.T) {
	type data struct{ H uint64 }
	s := NewEndpointStore[data](nil)
	get := HeightGetter(s, func(d data) uint64 { return d.H }, HeightProjection{})

	s.ObserveHeight("old-https://n1.behind.net", func(d *data) { d.H = 900 })
	if h, ok := get("new-https://n1.behind.net"); !ok || h != 900 {
		t.Fatalf("new address on a known host: %d %v, want 900 true", h, ok)
	}
	if _, ok := get("new-https://n2.behind.net"); ok {
		t.Fatal("another host has no reading: must stay unknown")
	}
	s.ObserveHeight("new-https://n1.behind.net", func(d *data) { d.H = 950 })
	s.ObserveHeight("old-https://n1.behind.net", func(d *data) { d.H = 1000 })
	if h, _ := get("new-https://n1.behind.net"); h != 950 {
		t.Fatalf("own reading must win over the host's: got %d, want 950", h)
	}

	s.mu.Lock()
	e := s.hosts["n1.behind.net"]
	e.HeightAt = time.Now().Add(-hostHeightMaxAge - time.Minute)
	s.hosts["n1.behind.net"] = e
	s.mu.Unlock()
	if _, ok := get("third-https://n1.behind.net"); ok {
		t.Fatal("a host reading older than hostHeightMaxAge must not stand in")
	}
}
