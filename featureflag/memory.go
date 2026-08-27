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

// IsEnabled resolves a flag in precedence order: per-service override, then
// the global setting, then the compiled DefaultFlags. A flag absent from
// DefaultFlags resolves to false — which is how a flag someone forgot to add
// there silently never runs.
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

// Set changes a flag globally. Per-service overrides still win over it; use
// Delete to clear those.
func (s *MemoryStore) Set(_ context.Context, flag string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.global[flag] = enabled
	return nil
}

// SetForService overrides a flag for one service, taking precedence over the
// global setting for that service only.
func (s *MemoryStore) SetForService(_ context.Context, flag string, serviceID domain.ServiceID, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serviceOverrides[flag] == nil {
		s.serviceOverrides[flag] = make(map[domain.ServiceID]bool)
	}
	s.serviceOverrides[flag][serviceID] = enabled
	return nil
}

// GetAll returns the effective state of every known flag, layering global and
// per-service overrides onto DefaultFlags. Every flag in DefaultFlags appears,
// set or not, so the admin API can list what exists rather than only what
// somebody has touched.
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

// Delete clears a setting so the flag falls back to the next level down. With
// a serviceID it removes only that service's override; with an empty serviceID
// it removes the global setting and every service override, returning the flag
// to its compiled default.
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

// DeleteGlobal removes the global value of a flag, leaving every per-service
// override in place.
//
// This store seeds the config file's overrides straight into the global map,
// so one delete covers both sources — unlike RedisStore, which keeps them in
// separate layers and has to drop each. See FlagStore.DeleteGlobal for why the
// per-service overrides must survive.
func (s *MemoryStore) DeleteGlobal(_ context.Context, flag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.global, flag)
	return nil
}
