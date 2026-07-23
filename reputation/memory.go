package reputation

import (
	"context"
	"errors"
	"strings"
	"sync"
)

// ErrScoreNotFound is returned when a score key does not exist in storage.
var ErrScoreNotFound = errors.New("score not found")

// MemoryStorage is a thread-safe in-memory implementation of Storage.
type MemoryStorage struct {
	mu     sync.RWMutex
	scores map[string]float64
}

// NewMemoryStorage creates a new in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		scores: make(map[string]float64),
	}
}

// GetScore retrieves the score for the given key.
func (m *MemoryStorage) GetScore(_ context.Context, key string) (float64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	score, ok := m.scores[key]
	if !ok {
		return 0, ErrScoreNotFound
	}
	return score, nil
}

// SetScore stores the score for the given key.
func (m *MemoryStorage) SetScore(_ context.Context, key string, score float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scores[key] = score
	return nil
}

// GetScores retrieves all scores whose keys begin with the given prefix.
func (m *MemoryStorage) GetScores(_ context.Context, prefix string) (map[string]float64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]float64)
	for k, v := range m.scores {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result, nil
}

// DeleteScore removes the score for the given key.
func (m *MemoryStorage) DeleteScore(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.scores, key)
	return nil
}
