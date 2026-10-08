package qos

import (
	"sync"
	"time"
)

// HostMemory remembers one observation per host, each trusted for a TTL.
//
// Per host, not per supplier address: many staked addresses front one URL,
// and what a plugin learns from an answer (archival retention, the lowest
// height held) is a property of the node behind it. Keyed per address it
// would be relearned once per address. Bounded: when full, a new host clears
// it wholesale rather than evicting, because the memory rebuilds at one relay
// per host and a map that size means something upstream is producing hosts,
// not that the memory is worth preserving. Safe for concurrent use.
type HostMemory[V any] struct {
	mu       sync.RWMutex
	entries  map[string]hostEntry[V]
	ttl      time.Duration
	maxHosts int
	now      func() time.Time
}

type hostEntry[V any] struct {
	value  V
	expiry time.Time
}

// NewHostMemory creates an empty HostMemory whose observations are trusted
// for ttl and which holds at most maxHosts hosts.
func NewHostMemory[V any](ttl time.Duration, maxHosts int) *HostMemory[V] {
	return &HostMemory[V]{entries: make(map[string]hostEntry[V]), ttl: ttl, maxHosts: maxHosts, now: time.Now}
}

// Set records an observation for host, trusted for the TTL. An empty host is
// ignored.
func (m *HostMemory[V]) Set(host string, value V) {
	m.SetUntil(host, value, m.now().Add(m.ttl))
}

// SetUntil records an observation for host, trusted until expiry. An empty
// host is ignored.
func (m *HostMemory[V]) SetUntil(host string, value V, expiry time.Time) {
	if host == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, known := m.entries[host]; !known && len(m.entries) >= m.maxHosts {
		m.entries = make(map[string]hostEntry[V])
	}
	m.entries[host] = hostEntry[V]{value: value, expiry: expiry}
}

// Get returns the host's observation; ok is false when there is none or it
// has aged out, which is a third state, not a negative.
func (m *HostMemory[V]) Get(host string) (value V, ok bool) {
	m.mu.RLock()
	e, found := m.entries[host]
	m.mu.RUnlock()
	if !found || !m.now().Before(e.expiry) {
		return value, false
	}
	return e.value, true
}

// Reset forgets everything.
func (m *HostMemory[V]) Reset() {
	m.mu.Lock()
	m.entries = make(map[string]hostEntry[V])
	m.mu.Unlock()
}
