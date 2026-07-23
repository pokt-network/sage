package featureflag

import (
	"context"
	"sync"

	"github.com/pokt-network/sage/domain"
)

// MemoryStore is an in-memory FlagStore implementation.
// Thread-safe. Used for testing and single-instance deployments.
type MemoryStore struct {
	mu               sync.RWMutex
	global           map[string]bool
	serviceOverrides map[string]map[domain.ServiceID]bool
}

// NewMemoryStore creates a new in-memory flag store seeded with the given defaults.
func NewMemoryStore(defaults map[string]bool) *MemoryStore {
	global := make(map[string]bool, len(defaults))
	for k, v := range defaults {
		global[k] = v
	}
	return &MemoryStore{
		global:           global,
		serviceOverrides: make(map[string]map[domain.ServiceID]bool),
	}
}

func (s *MemoryStore) IsEnabled(_ context.Context, flag string, serviceID domain.ServiceID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check per-service override first.
	if overrides, ok := s.serviceOverrides[flag]; ok {
		if val, ok := overrides[serviceID]; ok {
			return val
		}
	}

	// Check global setting.
	if val, ok := s.global[flag]; ok {
		return val
	}

	// Fall back to compiled defaults.
	return DefaultFlags[flag]
}

func (s *MemoryStore) Set(_ context.Context, flag string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.global[flag] = enabled
	return nil
}

func (s *MemoryStore) SetForService(_ context.Context, flag string, serviceID domain.ServiceID, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serviceOverrides[flag] == nil {
		s.serviceOverrides[flag] = make(map[domain.ServiceID]bool)
	}
	s.serviceOverrides[flag][serviceID] = enabled
	return nil
}

func (s *MemoryStore) GetAll(_ context.Context) (map[string]FlagState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]FlagState)

	// Start with compiled defaults.
	for flag, enabled := range DefaultFlags {
		result[flag] = FlagState{Enabled: enabled}
	}

	// Apply global overrides.
	for flag, enabled := range s.global {
		st := result[flag]
		st.Enabled = enabled
		result[flag] = st
	}

	// Apply per-service overrides.
	for flag, overrides := range s.serviceOverrides {
		st := result[flag]
		if st.ServiceOverrides == nil {
			st.ServiceOverrides = make(map[domain.ServiceID]bool, len(overrides))
		}
		for svc, val := range overrides {
			st.ServiceOverrides[svc] = val
		}
		result[flag] = st
	}

	return result, nil
}

func (s *MemoryStore) Delete(_ context.Context, flag string, serviceID domain.ServiceID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if serviceID != "" {
		if overrides, ok := s.serviceOverrides[flag]; ok {
			delete(overrides, serviceID)
			if len(overrides) == 0 {
				delete(s.serviceOverrides, flag)
			}
		}
		return nil
	}

	delete(s.global, flag)
	delete(s.serviceOverrides, flag)
	return nil
}
