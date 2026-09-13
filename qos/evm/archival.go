package evm

import (
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// archivalMemory remembers, per host, whether the node behind it served or
// refused a historical-state query, and for how long that is trusted.
//
// Per host, not per supplier address: on Pocket many staked addresses front
// one URL (nodefleet's pkp-og carried 162 on kava), and retention is a
// property of the node behind it. Keyed per address the mark was relearned
// once per address, one archival request each; the cosmos plugin's pruned
// memory keys on the host for the same reason. Bounded, and the marks
// expire: see archivalTTL.
type archivalMemory struct {
	mu      sync.RWMutex
	entries map[string]archivalEntry
	ttl     time.Duration
	max     int
	now     func() time.Time
}

type archivalEntry struct {
	archival bool
	expiry   time.Time
}

// maxArchivalHosts bounds the memory; hosts come from staked URLs.
const maxArchivalHosts = 4096

func newArchivalMemory() *archivalMemory {
	return &archivalMemory{entries: make(map[string]archivalEntry), ttl: archivalTTL, max: maxArchivalHosts, now: time.Now}
}

// hostKey is the memory's key for an address: its host, or the whole address
// when it carries none (tests and mocks use bare names).
func hostKey(addr domain.EndpointAddr) string {
	if h := addr.Domain(); h != "" {
		return h
	}
	return string(addr)
}

// set records an observation for the host, trusted for the TTL.
func (m *archivalMemory) set(host string, archival bool) {
	m.setUntil(host, archival, m.now().Add(m.ttl))
}

func (m *archivalMemory) setUntil(host string, archival bool, expiry time.Time) {
	if host == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, known := m.entries[host]; !known && len(m.entries) >= m.max {
		m.entries = make(map[string]archivalEntry)
	}
	m.entries[host] = archivalEntry{archival: archival, expiry: expiry}
}

// get returns the host's observation; known is false when there is none or
// it has aged out, which is a third state, not a negative.
func (m *archivalMemory) get(host string) (archival, known bool) {
	m.mu.RLock()
	e, ok := m.entries[host]
	m.mu.RUnlock()
	if !ok || !m.now().Before(e.expiry) {
		return false, false
	}
	return e.archival, true
}

func (m *archivalMemory) reset() {
	m.mu.Lock()
	m.entries = make(map[string]archivalEntry)
	m.mu.Unlock()
}
