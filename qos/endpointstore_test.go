package qos

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

type epData struct {
	BlockHeight uint64
	IsArchival  bool
}

func newTestStore() *EndpointStore[epData] {
	return NewEndpointStore[epData]()
}

func TestEndpointStore_Get(t *testing.T) {
	s := newTestStore()
	addr := domain.EndpointAddr("pokt1-https://node1.com")

	// Get on empty store.
	if _, ok := s.Get(addr); ok {
		t.Fatal("expected not found")
	}

	// Store and retrieve.
	s.Update(addr, func(d *epData) { *d = epData{BlockHeight: 100, IsArchival: true} })
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

// The three plugins used to write this closure themselves and disagreed about
// a stored height of 0: two let the endpoint through as unjudgeable, one
// filtered it out as hopelessly stale. Pinning the answer here is the point of
// having one implementation.
func TestHeightGetter(t *testing.T) {
	type ep struct{ height uint64 }
	store := NewEndpointStore[ep]()

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
	s.Update("a", func(d *epData) { *d = epData{BlockHeight: 1} })
	s.Update("b", func(d *epData) { *d = epData{BlockHeight: 2} })
	if got := len(s.endpoints); got != 2 {
		t.Fatalf("len(endpoints) = %d before Clear, want 2", got)
	}

	s.Clear()

	if got := len(s.endpoints); got != 0 {
		t.Fatalf("len(endpoints) = %d after Clear, want 0", got)
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
	s := NewEndpointStore[data]()
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
