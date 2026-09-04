package middleware

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// Validate returns a middleware that checks the request is addressed to a
// configured service and carries an RPC type that service declares. The
// allowed types are built from config at construction time to avoid
// per-request map lookups of the slice.
//
// A service absent from the config is refused with 400 and the list of
// services that exist, as PATH does. It used to pass through and fail deep in
// the protocol layer, where the only thing left to say was "relay failed" —
// a typo in Target-Service-Id looked like an outage. With no services
// configured at all, nothing is validated; that is a test harness, not a
// deployment.
func Validate(services []config.ServiceConfig) relay.Middleware {
	// Build map[ServiceID]map[RPCType]bool at construction time.
	allowed := make(map[domain.ServiceID]map[domain.RPCType]bool, len(services))
	available := make([]string, 0, len(services))
	for _, svc := range services {
		sid := domain.ServiceID(svc.ID)
		m := make(map[domain.RPCType]bool, len(svc.RPCTypes))
		for _, rt := range svc.RPCTypes {
			m[domain.RPCType(rt)] = true
		}
		allowed[sid] = m
		available = append(available, svc.ID)
	}
	sort.Strings(available)

	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if len(allowed) == 0 {
				return next.HandleRelay(ctx)
			}
			types, known := allowed[ctx.ServiceID]
			if !known {
				return rejectRequest(ctx, nil, http.StatusBadRequest, domain.ErrValidation,
					fmt.Sprintf("service %q is not configured", ctx.ServiceID), nil,
					map[string]any{"available_services": available})
			}

			if !types[ctx.RPCType] {
				declared := make([]string, 0, len(types))
				for rt := range types {
					declared = append(declared, string(rt))
				}
				sort.Strings(declared)
				return rejectRequest(ctx, nil, http.StatusBadRequest, domain.ErrValidation,
					fmt.Sprintf("RPC type %q not supported for service %q", ctx.RPCType, ctx.ServiceID), nil,
					map[string]any{
						"service_id":        string(ctx.ServiceID),
						"detected_type":     string(ctx.RPCType),
						"allowed_rpc_types": declared,
					})
			}

			return next.HandleRelay(ctx)
		})
	}
}
