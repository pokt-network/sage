package reputation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// ErrStateNotFound is returned when a state key does not exist in storage.
var ErrStateNotFound = errors.New("state not found")

// MemoryStorage is a thread-safe in-memory implementation of Storage.
type MemoryStorage struct {
	mu      sync.RWMutex
	states  map[string]State
	opStats map[string]OperatorStat
}

// NewMemoryStorage creates a new in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		states:  make(map[string]State),
		opStats: make(map[string]OperatorStat),
	}
}

var _ OperatorStatStore = (*MemoryStorage)(nil)

// GetOperatorStats returns every stored operator stat.
func (m *MemoryStorage) GetOperatorStats(_ context.Context) (map[string]OperatorStat, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]OperatorStat, len(m.opStats))
	for k, v := range m.opStats {
		out[k] = v
	}
	return out, nil
}

// SetOperatorStat stores one operator stat.
func (m *MemoryStorage) SetOperatorStat(_ context.Context, field string, st OperatorStat) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opStats[field] = st
	return nil
}

// GetState retrieves the state for the given key.
func (m *MemoryStorage) GetState(_ context.Context, key string) (State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.states[key]
	if !ok {
		return State{}, ErrStateNotFound
	}
	return st, nil
}

// SetState stores the state for the given key.
func (m *MemoryStorage) SetState(_ context.Context, key string, st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[key] = st
	return nil
}

// GetStates retrieves all states whose keys begin with the given prefix.
func (m *MemoryStorage) GetStates(_ context.Context, prefix string) (map[string]State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]State)
	for k, v := range m.states {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result, nil
}

// DeleteState removes the state for the given key.
func (m *MemoryStorage) DeleteState(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, key)
	return nil
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
