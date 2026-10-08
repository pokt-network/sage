package reputation

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"
)

// MemoryStorage is a thread-safe in-memory implementation of Storage.
type MemoryStorage struct {
	mu      sync.RWMutex
	states  map[string]State
	opStats map[string]OperatorStat
	parties PartyPenalties
	notes   map[string]NotificationCounts
	policy  map[string]PolicyPenalty
}

// NewMemoryStorage creates a new in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		states:  make(map[string]State),
		opStats: make(map[string]OperatorStat),
	}
}

// GetOperatorStats returns every stored operator stat.
func (m *MemoryStorage) GetOperatorStats(_ context.Context) (map[string]OperatorStat, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return maps.Clone(m.opStats), nil
}

// SetOperatorStat stores one operator stat.
func (m *MemoryStorage) SetOperatorStat(_ context.Context, field string, st OperatorStat) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opStats[field] = st
	return nil
}

// SetState stores the state for the given key.
func (m *MemoryStorage) SetState(_ context.Context, key string, st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[key] = st
	return nil
}

// GetStates retrieves every stored state.
func (m *MemoryStorage) GetStates(_ context.Context) (map[string]State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return maps.Clone(m.states), nil
}

// DeleteStale implements StaleDeleter.
func (m *MemoryStorage) DeleteStale(_ context.Context, olderThan time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := olderThan.Unix()
	n := 0
	for k, st := range m.states {
		if st.UpdatedAt < cutoff {
			delete(m.states, k)
			n++
		}
	}
	return n, nil
}

// GetPartyPenalties returns the stored priced parties.
func (m *MemoryStorage) GetPartyPenalties(_ context.Context) (PartyPenalties, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.parties, nil
}

// SetPartyPenalties replaces the stored priced parties.
func (m *MemoryStorage) SetPartyPenalties(_ context.Context, p PartyPenalties) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.parties = p
	return nil
}

// PutNotificationCounts replaces one pod's notification counts.
func (m *MemoryStorage) PutNotificationCounts(_ context.Context, pod string, c NotificationCounts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notes == nil {
		m.notes = make(map[string]NotificationCounts)
	}
	m.notes[pod] = c
	return nil
}

// NotificationCounts returns every pod's notification counts.
func (m *MemoryStorage) NotificationCounts(_ context.Context) (map[string]NotificationCounts, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return maps.Clone(m.notes), nil
}

// DeleteNotificationCounts drops the named pods' counts.
func (m *MemoryStorage) DeleteNotificationCounts(_ context.Context, pods ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pod := range pods {
		delete(m.notes, pod)
	}
	return nil
}

// PutPolicyPenalty stores a party's policy penalty, replacing its previous one.
func (m *MemoryStorage) PutPolicyPenalty(_ context.Context, p PolicyPenalty) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.policy == nil {
		m.policy = make(map[string]PolicyPenalty)
	}
	m.policy[p.Party] = p
	return nil
}

// PolicyPenalties returns every stored policy penalty.
func (m *MemoryStorage) PolicyPenalties(_ context.Context) ([]PolicyPenalty, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.AppendSeq(make([]PolicyPenalty, 0, len(m.policy)), maps.Values(m.policy)), nil
}

// DeletePolicyPenalty removes a party's policy penalty.
func (m *MemoryStorage) DeletePolicyPenalty(_ context.Context, party string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.policy[party]
	delete(m.policy, party)
	return ok, nil
}
