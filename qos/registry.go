package qos

import (
	"fmt"
	"sync"

	"github.com/pokt-network/sage/domain"
)

// Registry maps ServiceIDs to their QoS plugin implementations.
type Registry struct {
	mu      sync.RWMutex
	plugins map[domain.ServiceID]Plugin
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		plugins: make(map[domain.ServiceID]Plugin),
	}
}

// Register associates a Plugin with a ServiceID.
// Returns an error if the service is already registered.
func (r *Registry) Register(id domain.ServiceID, p Plugin) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.plugins[id]; exists {
		return fmt.Errorf("qos: service %q already registered", id)
	}
	r.plugins[id] = p
	return nil
}

// Get returns the Plugin for the given ServiceID, or nil if not found.
func (r *Registry) Get(id domain.ServiceID) Plugin {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.plugins[id]
}

// Plugins returns a snapshot copy of all registered plugins.
func (r *Registry) Plugins() map[domain.ServiceID]Plugin {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[domain.ServiceID]Plugin, len(r.plugins))
	for k, v := range r.plugins {
		out[k] = v
	}
	return out
}

// Count returns the number of registered plugins.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.plugins)
}
