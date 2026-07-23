// Package mock provides an in-process protocol backend that serves canned
// responses instead of relaying to Pocket Network suppliers. It exists so the
// full middleware chain (parse, cache, retry, hedge, selection, heuristic,
// reputation, observation) can be exercised and load-tested without a
// fullnode, suppliers, or signing keys.
//
// NOT for production: enable via `protocol: {type: mock}` in config.
package mock

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pokt-network/sage/domain"
)

// defaultResponseBody is a minimal valid JSON-RPC success response. It passes
// all heuristic tiers (valid JSON, has jsonrpc/result/id), so the success
// path is representative of a healthy supplier.
const defaultResponseBody = `{"jsonrpc":"2.0","id":1,"result":"0x10d4f3a"}`

// Mock implements protocol.Relayer, protocol.EndpointProvider, and
// protocol.SessionManager with synthetic endpoints and canned responses.
type Mock struct {
	services  map[domain.ServiceID]struct{}
	endpoints domain.EndpointAddrList
	latency   time.Duration
	respBody  []byte
}

// New creates a Mock backend serving the given services. endpointCount fake
// endpoints (distinct supplier addresses and domains) are advertised for
// every service. latency, when positive, is slept on every SendRelay to
// simulate supplier response time. respBody overrides the canned response
// when non-empty.
func New(serviceIDs []domain.ServiceID, endpointCount int, latency time.Duration, respBody string, logger *slog.Logger) *Mock {
	if endpointCount <= 0 {
		endpointCount = 10
	}
	if respBody == "" {
		respBody = defaultResponseBody
	}

	services := make(map[domain.ServiceID]struct{}, len(serviceIDs))
	for _, id := range serviceIDs {
		services[id] = struct{}{}
	}

	// Address format mirrors Shannon: "<supplier>-<url>". Distinct domains so
	// circuit breaking and supplier affinity treat each endpoint independently.
	endpoints := make(domain.EndpointAddrList, endpointCount)
	for i := range endpoints {
		endpoints[i] = domain.EndpointAddr(
			fmt.Sprintf("pokt1mock%03d-https://supplier-%03d.mock.local", i, i))
	}

	if logger != nil {
		logger.Warn("mock protocol backend active — canned responses, no real relays",
			"endpoints", endpointCount, "simulated_latency", latency.String())
	}

	return &Mock{
		services:  services,
		endpoints: endpoints,
		latency:   latency,
		respBody:  []byte(respBody),
	}
}

// SendRelay returns the canned response after the configured simulated latency.
func (m *Mock) SendRelay(ctx context.Context, _ domain.ServiceID, endpoint domain.EndpointAddr, _ domain.Payload) (*domain.Response, error) {
	start := time.Now()
	if m.latency > 0 {
		select {
		case <-time.After(m.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &domain.Response{
		Body:           m.respBody,
		HTTPStatusCode: 200,
		Latency:        time.Since(start),
		EndpointAddr:   endpoint,
	}, nil
}

// AvailableEndpoints returns a fresh copy of the synthetic endpoint list.
// Callers (circuit breaker, retry) filter the slice in place, so sharing the
// backing array across requests would corrupt it.
func (m *Mock) AvailableEndpoints(_ context.Context, serviceID domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	if _, ok := m.services[serviceID]; !ok {
		return nil, fmt.Errorf("mock: service %q not configured", serviceID)
	}
	out := make(domain.EndpointAddrList, len(m.endpoints))
	copy(out, m.endpoints)
	return out, nil
}

// ConfiguredServices returns the configured service set.
func (m *Mock) ConfiguredServices() map[domain.ServiceID]struct{} {
	return m.services
}

// IsReady always reports ready — there is no fullnode to wait for.
func (m *Mock) IsReady(_ context.Context) bool { return true }
