// Package protocol defines interfaces for relay transport implementations.
package protocol

import (
	"context"

	"github.com/pokt-network/sage/domain"
)

// Relayer sends a relay request to a specific endpoint and returns the response.
// Implementations handle protocol-specific signing, encoding, and transport.
type Relayer interface {
	SendRelay(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, payload domain.Payload) (*domain.Response, error)
}

// EndpointProvider lists available endpoints for a service.
type EndpointProvider interface {
	AvailableEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error)
}

// URLResolver answers which URL a relay to an endpoint actually dials for an
// RPC type. An endpoint address carries the supplier's public URL, one per
// supplier whatever the type; an operator that stakes one host per type
// (kleomedes: eu-s-01-osmosis-json and eu-s-01-osmosis-rest) dials a
// different host for its REST face than the address names. Reputation keys
// and method-block hosts ask here so a face is scored under the host that
// served it. ok is false when the endpoint is not in any current session or
// does not stake the type.
type URLResolver interface {
	EndpointURLFor(endpoint domain.EndpointAddr, rpcType domain.RPCType) (string, bool)
}

// SessionManager manages session lifecycle.
type SessionManager interface {
	ConfiguredServices() map[domain.ServiceID]struct{}
	IsReady(ctx context.Context) bool
}

// SupplierManager handles supplier blacklisting.
type SupplierManager interface {
	BlacklistSupplier(serviceID domain.ServiceID, addr string)
	UnblacklistSupplier(serviceID domain.ServiceID, addr string) bool
	IsBlacklisted(serviceID domain.ServiceID, addr string) bool
}
