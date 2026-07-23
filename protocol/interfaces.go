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
