// Package featureflag provides runtime feature flags that can be toggled
// per-service without redeployment.
package featureflag

import (
	"context"

	"github.com/pokt-network/sage/domain"
)

// FlagStore provides runtime feature flag checking.
type FlagStore interface {
	// IsEnabled checks if a flag is enabled for a service.
	// Checks per-service override first, then global setting, then default.
	IsEnabled(ctx context.Context, flag string, serviceID domain.ServiceID) bool

	// Set sets a global flag value.
	Set(ctx context.Context, flag string, enabled bool) error

	// SetForService sets a per-service flag override.
	SetForService(ctx context.Context, flag string, serviceID domain.ServiceID, enabled bool) error

	// GetAll returns all flag states (global + per-service overrides).
	GetAll(ctx context.Context) (map[string]FlagState, error)

	// Delete removes a flag globally, or a per-service override if serviceID is non-empty.
	Delete(ctx context.Context, flag string, serviceID domain.ServiceID) error

	// DeleteGlobal removes only the global value of a flag, leaving every
	// per-service override in place.
	//
	// The narrow half of Delete, and the one a config reload needs. A flag
	// dropped from feature_flags in the YAML has to stop overriding
	// DefaultFlags — but config carries global values only, so wiping the
	// per-service overrides an operator set through the admin API would make a
	// deleted line in a file revoke a decision it never made.
	DeleteGlobal(ctx context.Context, flag string) error
}

// FlagState represents the state of a single feature flag.
type FlagState struct {
	Enabled          bool                      `json:"enabled"`
	ServiceOverrides map[domain.ServiceID]bool `json:"service_overrides,omitempty"`
}
