package qos

import (
	"log/slog"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// EndpointStore is a generic, thread-safe store for per-endpoint data.
// It eliminates the duplicated endpoint maps across EVM/Cosmos/Solana.
type EndpointStore[T any] struct {
	mu        sync.RWMutex
	endpoints map[domain.EndpointAddr]storedEndpoint[T]
	logger    *slog.Logger
}

type storedEndpoint[T any] struct {
	Data     T
	LastSeen time.Time
}

// NewEndpointStore creates an empty EndpointStore.
func NewEndpointStore[T any](logger *slog.Logger) *EndpointStore[T] {
	if logger == nil {
		logger = slog.Default()
	}
	return &EndpointStore[T]{
		endpoints: make(map[domain.EndpointAddr]storedEndpoint[T]),
		logger:    logger,
	}
}

// HeightGetter builds the height lookup that BlockHeightFilter and
// LeastStaleFallback both take, from a store and an accessor for whichever
// field that chain calls its height.
//
// Every plugin needs this and each used to write its own closure — Cosmos three
// times in one function — and the copies did not agree. Two treated a stored
// height of 0 as "unknown, let the endpoint through"; one treated it as a real
// height and filtered the endpoint out as hopelessly stale. Nothing recorded
// which was intended, and the difference is only visible in a pool where an
// endpoint has been seen but never reported.
//
// Zero means unknown here, for every chain. An endpoint we have no height for
// is one we cannot judge on height, and excluding it on that basis penalizes it
// for our own missing data — the same reasoning BlockHeightFilter already
// applies to an endpoint that is absent from the store entirely. Treating
// "absent" and "present with no height" differently was the accident.
func HeightGetter[T any](store *EndpointStore[T], height func(T) uint64) func(domain.EndpointAddr) (uint64, bool) {
	return func(addr domain.EndpointAddr) (uint64, bool) {
		data, ok := store.Get(addr)
		if !ok {
			return 0, false
		}
		h := height(data)
		if h == 0 {
			return 0, false
		}
		return h, true
	}
}

// Get returns the data for the given endpoint, and whether it was found.
func (s *EndpointStore[T]) Get(addr domain.EndpointAddr) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ep, ok := s.endpoints[addr]
	if !ok {
		var zero T
		return zero, false
	}
	return ep.Data, true
}

// Set stores data for an endpoint, updating LastSeen.
func (s *EndpointStore[T]) Set(addr domain.EndpointAddr, data T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints[addr] = storedEndpoint[T]{
		Data:     data,
		LastSeen: time.Now(),
	}
}

// Update applies fn to the stored data in-place. If the endpoint does not exist,
// it is created with the zero value of T before fn is called.
func (s *EndpointStore[T]) Update(addr domain.EndpointAddr, fn func(*T)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep, ok := s.endpoints[addr]
	if !ok {
		ep = storedEndpoint[T]{LastSeen: time.Now()}
	}
	fn(&ep.Data)
	ep.LastSeen = time.Now()
	s.endpoints[addr] = ep
}

// Touch updates LastSeen for all given addresses. Addresses not in the store are ignored.
func (s *EndpointStore[T]) Touch(addrs domain.EndpointAddrList) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, addr := range addrs {
		if ep, ok := s.endpoints[addr]; ok {
			ep.LastSeen = now
			s.endpoints[addr] = ep
		}
	}
}

// SweepStale removes endpoints not seen within the given TTL and returns their addresses.
func (s *EndpointStore[T]) SweepStale(ttl time.Duration) []domain.EndpointAddr {
	cutoff := time.Now().Add(-ttl)
	s.mu.Lock()
	defer s.mu.Unlock()
	var removed []domain.EndpointAddr
	for addr, ep := range s.endpoints {
		if ep.LastSeen.Before(cutoff) {
			delete(s.endpoints, addr)
			removed = append(removed, addr)
		}
	}
	if len(removed) > 0 {
		s.logger.Info("swept stale endpoints", "count", len(removed))
	}
	return removed
}

// Range iterates over all endpoints. Return false from fn to stop iteration.
func (s *EndpointStore[T]) Range(fn func(domain.EndpointAddr, T) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for addr, ep := range s.endpoints {
		if !fn(addr, ep.Data) {
			return
		}
	}
}

// Clear removes every stored endpoint. It exists for an operator-triggered
// chain-state reset: the store repopulates from the next health-check cycle
// and the next relays, and an endpoint the store no longer knows is treated
// as unknown, which callers already let through.
func (s *EndpointStore[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints = make(map[domain.EndpointAddr]storedEndpoint[T])
}

// Count returns the number of stored endpoints.
func (s *EndpointStore[T]) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.endpoints)
}

// Addrs returns a list of all endpoint addresses in the store.
func (s *EndpointStore[T]) Addrs() domain.EndpointAddrList {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(domain.EndpointAddrList, 0, len(s.endpoints))
	for addr := range s.endpoints {
		out = append(out, addr)
	}
	return out
}
