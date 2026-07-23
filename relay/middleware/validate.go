package middleware

import (
	"net/http"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// Validate returns a middleware that checks the detected RPC type is allowed
// for the service. The allowed types are built from config at construction
// time to avoid per-request map lookups of the slice.
//
// If a service is not present in the config, the request passes through
// without validation (unknown services are allowed).
func Validate(services []config.ServiceConfig) relay.Middleware {
	// Build map[ServiceID]map[RPCType]bool at construction time.
	allowed := make(map[domain.ServiceID]map[domain.RPCType]bool, len(services))
	for _, svc := range services {
		sid := domain.ServiceID(svc.ID)
		m := make(map[domain.RPCType]bool, len(svc.RPCTypes))
		for _, rt := range svc.RPCTypes {
			m[domain.RPCType(rt)] = true
		}
		allowed[sid] = m
	}

	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			types, known := allowed[ctx.ServiceID]
			if !known {
				// Service not in config — pass through.
				return next.HandleRelay(ctx)
			}

			if !types[ctx.RPCType] {
				ctx.Err = domain.NewRelayError(
					domain.ErrValidation,
					"RPC type not supported for service",
					nil,
					false,
				)
				if ctx.Writer != nil {
					ctx.Writer.SetStatusCode(http.StatusBadRequest)
					_ = ctx.Writer.Write([]byte(`{"error":"RPC type not supported for service"}`))
				}
				return ctx.Err
			}

			return next.HandleRelay(ctx)
		})
	}
}
