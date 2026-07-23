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
