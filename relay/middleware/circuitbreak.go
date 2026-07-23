package middleware

import (
	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/relay"
)

// CircuitBreakRecorder is notified when a domain is circuit-broken.
// metrics.Recorder satisfies it.
type CircuitBreakRecorder interface {
	RecordCircuitBreak(serviceID domain.ServiceID, brokenDomain string)
}

// CircuitBreak returns a middleware that:
//  1. Pre-relay: filters out endpoints whose domain is currently circuit-broken.
//  2. Post-relay: if ctx.HeuristicResult.ShouldCircuitBreak==true, marks the
//     endpoint's domain as broken.
//
// IMPORTANT: ShouldRetry alone does NOT trigger circuit-breaking. Only
// ShouldCircuitBreak causes a domain to be marked broken.
//
// If the "circuit_breaker" feature flag is disabled, the middleware passes
// through to the inner handler without wrapping.
//
// breakRecorder is notified whenever a domain is broken. Nil is allowed and
// disables recording.
func CircuitBreak(
	breaker *circuitbreaker.Breaker,
	endpointProvider protocol.EndpointProvider,
	flags featureflag.FlagStore,
	breakRecorder CircuitBreakRecorder,
) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if !flags.IsEnabled(ctx.Ctx, featureflag.FlagCircuitBreaker, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			serviceID := string(ctx.ServiceID)

			// Populate ctx.Endpoints if not yet set: CircuitBreak runs before
			// SelectEndpoint (which sits inside Retry/Hedge), so without this
			// the very first attempt would see an empty list and skip the
			// broken-domain filter entirely. SelectEndpoint skips its own
			// fetch when the list is already populated.
			if len(ctx.Endpoints) == 0 && endpointProvider != nil {
				eps, err := endpointProvider.AvailableEndpoints(ctx.Ctx, ctx.ServiceID, ctx.RPCType)
				if err == nil {
					ctx.Endpoints = eps
				}
				// On error, leave the list empty — SelectEndpoint retries the
				// fetch and surfaces the error.
			}

			// Pre-relay: remove endpoints whose domain is broken.
			if len(ctx.Endpoints) > 0 {
				filtered := ctx.Endpoints[:0:len(ctx.Endpoints)]
				filtered = filtered[:0]
				for _, ep := range ctx.Endpoints {
					if !breaker.IsBroken(serviceID, ep.Domain()) {
						filtered = append(filtered, ep)
					}
				}
				ctx.Endpoints = filtered
			}

			err := next.HandleRelay(ctx)

			// Post-relay: check heuristic result for circuit-break signal.
			if ctx.HeuristicResult != nil && ctx.HeuristicResult.ShouldCircuitBreak && ctx.Endpoint != "" {
				reason := ctx.HeuristicResult.Reason
				if reason == "" {
					reason = "heuristic_circuit_break"
				}
				brokenDomain := ctx.Endpoint.Domain()
				breaker.MarkBroken(serviceID, brokenDomain, reason)
				if breakRecorder != nil {
					breakRecorder.RecordCircuitBreak(ctx.ServiceID, brokenDomain)
				}
			}

			return err
		})
	}
}
