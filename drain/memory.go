package drain

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// MemoryStore keeps drains in process memory. It is the whole store when
// Redis is absent and the read-through cache when Redis is present. Expiry
// is lazy: entries are only ever dropped by a Set, a Release, or on read —
// there is no background sweep.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[Key]Entry
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[Key]Entry)}
}

// Set installs or refreshes a drain, or releases it when e.Until is not
// after now.
func (s *MemoryStore) Set(_ context.Context, e Entry) error {
	e.Operator = strings.ToLower(e.Operator)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !e.Until.After(time.Now()) {
		delete(s.entries, e.Key)
		return nil
	}
	s.entries[e.Key] = e
	return nil
}

// Release removes any drain at k. A k with no drain is a no-op.
func (s *MemoryStore) Release(_ context.Context, k Key) error {
	k.Operator = strings.ToLower(k.Operator)
	s.mu.Lock()
	delete(s.entries, k)
	s.mu.Unlock()
	return nil
}

// Drained reports whether a live drain covers (serviceID, operator, rpcType):
// a scoped entry for exactly that RPC type, or an unscoped entry (RPCType
// "") for the operator. Callers pass an already-lowercased operator — the
// chokepoint lowercases once per URL rather than once per check here.
// RLock only, no allocation.
func (s *MemoryStore) Drained(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) bool {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.entries[Key{ServiceID: serviceID, Operator: operator, RPCType: rpcType}]; ok && e.Until.After(now) {
		return true
	}
	e, ok := s.entries[Key{ServiceID: serviceID, Operator: operator}]
	return ok && e.Until.After(now)
}

// Active lists the live drains for serviceID, sorted by Operator then
// RPCType. Nil when none are live.
func (s *MemoryStore) Active(_ context.Context, serviceID domain.ServiceID) []Entry {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Entry
	for _, e := range s.entries {
		if e.ServiceID == serviceID && e.Until.After(now) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Operator != out[j].Operator {
			return out[i].Operator < out[j].Operator
		}
		return out[i].RPCType < out[j].RPCType
	})
	return out
}

// replaceAll swaps the whole entry map for next in one lock, so a reader sees
// either the old set of drains or the new one and never a half-applied mix.
// RedisStore's refresh loop uses it to rebuild the cache from Redis: a drain
// released on another replica is a key that is gone, which only a wholesale
// replace can express. A nil next clears every drain.
func (s *MemoryStore) replaceAll(next map[Key]Entry) {
	if next == nil {
		next = make(map[Key]Entry)
	}
	s.mu.Lock()
	s.entries = next
	s.mu.Unlock()
}

var _ Store = (*MemoryStore)(nil)
