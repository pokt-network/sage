package middleware

import (
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// SelectEndpoint returns a middleware that selects the best endpoint for the
// relay. It applies chain-specific QoS filtering via the plugin, falls back
// gracefully to the full endpoint list when QoS filtering produces no
// candidates, and then delegates final selection to the reputation service.
func SelectEndpoint(repSvc reputation.Service, endpointProvider protocol.EndpointProvider, registry *qos.Registry, flags featureflag.FlagStore) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Fetch available endpoints from the protocol layer if not already set.
			if len(ctx.Endpoints) == 0 && endpointProvider != nil {
				eps, err := endpointProvider.AvailableEndpoints(ctx.Ctx, ctx.ServiceID, ctx.RPCType)
				if err != nil {
					return err
				}
				ctx.Endpoints = eps
			}

			candidates := ctx.Endpoints

			// Apply chain-specific filtering via QoS plugin.
			if ctx.Plugin != nil {
				filtered, err := ctx.Plugin.SelectEndpoints(ctx.Endpoints, ctx.Payloads)
				if err == nil && len(filtered) > 0 {
					candidates = filtered
				} else {
					// Graceful degraded fallback: use original endpoints.
					ctx.Degraded = true
				}
			}

			// If there are still no endpoints, degrade and use the original list.
			if len(candidates) == 0 {
				ctx.Degraded = true
				candidates = ctx.Endpoints
			}

			// Select the best endpoint by reputation.
			ctx.Endpoint = repSvc.SelectBest(ctx.Ctx, ctx.ServiceID, candidates, ctx.RPCType)

			// Publish the choice for any goroutine watching this relay from
			// outside it — today only Hedge, which needs the primary arm's
			// endpoint while that arm is still running. Nil for every
			// unhedged relay, which is nearly all of them.
			if ctx.SelectedEndpoint != nil {
				ep := ctx.Endpoint
				ctx.SelectedEndpoint.Store(&ep)
			}

			return next.HandleRelay(ctx)
		})
	}
}
