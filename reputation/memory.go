package reputation

import (
	"context"
	"errors"
	"strings"
	"sync"
)

// ErrStateNotFound is returned when a state key does not exist in storage.
var ErrStateNotFound = errors.New("state not found")

// ErrScoreNotFound is the former name of ErrStateNotFound.
var ErrScoreNotFound = ErrStateNotFound

// MemoryStorage is a thread-safe in-memory implementation of Storage.
type MemoryStorage struct {
	mu     sync.RWMutex
	states map[string]State
}

// NewMemoryStorage creates a new in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		states: make(map[string]State),
	}
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
