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
//  2. Post-relay: reports the attempt's outcome to the breaker — a
//     ShouldCircuitBreak result as a failure, a clean relay as a success.
//
// IMPORTANT: ShouldRetry alone does NOT trigger circuit-breaking. Only
// ShouldCircuitBreak causes a domain to be marked broken — and even then only
// if the domain's recent failure RATE clears the breaker's gate. One bad relay
// must never remove a hostname (and everything behind it) from the pool.
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

			// Post-relay: feed the breaker's failure-rate gate.
			//
			// Both outcomes matter. The failure is only a candidate — the
			// breaker decides whether the domain's recent RATE justifies
			// removing it — and the success is the denominator that makes that
			// rate meaningful. Reporting only failures would mean every failure
			// reads as a 100% failure rate, which is the first-error behavior
			// the gate exists to replace.
			//
			// This middleware sits inside Retry/Hedge, so it runs once per
			// attempt and ctx.Endpoint is that attempt's endpoint.
			//
			// A relay that failed without asking for a circuit break counts as
			// neither: it is a failure the breaker has no opinion on (a 429, a
			// client error), and folding it into either side of the ratio would
			// distort a signal that is specifically about a host being broken.
			if ctx.Endpoint != "" {
				brokenDomain := ctx.Endpoint.Domain()
				switch {
				case ctx.HeuristicResult != nil && ctx.HeuristicResult.ShouldCircuitBreak:
					reason := ctx.HeuristicResult.Reason
					if reason == "" {
						reason = "heuristic_circuit_break"
					}
					if breaker.MarkBroken(serviceID, brokenDomain, reason) && breakRecorder != nil {
						breakRecorder.RecordCircuitBreak(ctx.ServiceID, brokenDomain)
					}
				case err == nil:
					breaker.RecordSuccess(serviceID, brokenDomain)
				}
			}

			return err
		})
	}
}
