package qos

import (
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// EndpointStore is a generic, thread-safe store for per-endpoint data.
// It eliminates the duplicated endpoint maps across EVM/Cosmos/Solana.
type EndpointStore[T any] struct {
	mu        sync.RWMutex
	endpoints map[domain.EndpointAddr]storedEndpoint[T]
	// hosts is the latest height reading per host, for a registration that
	// has none yet (see HeightGetter).
	hosts map[string]storedEndpoint[T]
}

// hostHeightMaxAge is how old a host's reading may be and still stand in for
// a registration of that host with none of its own.
const hostHeightMaxAge = 10 * time.Minute

// hostHeightsMax bounds the per-host readings. A service has a few dozen
// hosts; the cap is a backstop against churn, cleared wholesale when hit.
const hostHeightsMax = 4096

type storedEndpoint[T any] struct {
	Data T
	// HeightAt is when the height in Data was last observed.
	HeightAt time.Time
}

// NewEndpointStore creates an empty EndpointStore.
func NewEndpointStore[T any]() *EndpointStore[T] {
	return &EndpointStore[T]{
		endpoints: make(map[domain.EndpointAddr]storedEndpoint[T]),
		hosts:     make(map[string]storedEndpoint[T]),
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
// The height is projected to the moment the perceived head was read (see
// HeightProjection), so a reading taken a probe cycle ago is not judged as
// that many blocks behind. A zero projection returns the stored height.
//
// Zero means unknown here, for every chain. An endpoint we have no height for
// is one we cannot judge on height, and excluding it on that basis penalizes it
// for our own missing data — the same reasoning BlockHeightFilter already
// applies to an endpoint that is absent from the store entirely. Treating
// "absent" and "present with no height" differently was the accident.
//
// Except where the host has reported. A height belongs to the backend, not to
// the registration in front of it, and a session rollover hands every
// supplier a new address the store has never seen. On mainnet base
// (2026-09-29) that made one operator's 34 registrations, all on nodes 27,000
// blocks behind, unknown — and so admitted — for the minute after each
// rollover until the next probe: 1,200-3,500 client relays to them in that
// minute, every 20 minutes. An address with no height of its own takes its
// host's latest reading, if one was taken in the last hostHeightMaxAge.
func HeightGetter[T any](store *EndpointStore[T], height func(T) uint64, projection HeightProjection) func(domain.EndpointAddr) (uint64, bool) {
	return func(addr domain.EndpointAddr) (uint64, bool) {
		store.mu.RLock()
		ep, ok := store.endpoints[addr]
		if !ok || height(ep.Data) == 0 {
			var hostOK bool
			ep, hostOK = store.hosts[addr.Domain()]
			ok = hostOK && time.Since(ep.HeightAt) <= hostHeightMaxAge
		}
		store.mu.RUnlock()
		if !ok {
			return 0, false
		}
		h := height(ep.Data)
		if h == 0 {
			return 0, false
		}
		return projection.Project(h, ep.HeightAt), true
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

// ObserveHeight applies fn to the stored data in place for a new height
// reading, creating the endpoint with the zero value of T first if it does not
// exist, and records when the height was observed, which HeightGetter projects
// from.
func (s *EndpointStore[T]) ObserveHeight(addr domain.EndpointAddr, fn func(*T)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	ep := s.endpoints[addr]
	fn(&ep.Data)
	ep.HeightAt = now
	s.endpoints[addr] = ep
	if host := addr.Domain(); host != "" {
		if len(s.hosts) >= hostHeightsMax {
			clear(s.hosts)
		}
		s.hosts[host] = ep
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
	s.hosts = make(map[string]storedEndpoint[T])
}
