package middleware

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// Heuristic returns a middleware that analyses the relay outcome after the
// inner chain has run. It runs heuristic.Analyze on the response body and
// stores the AnalysisResult on the context. When the result indicates the
// request should be retried, a retryable RelayError is returned so that outer
// retry/hedge middleware can react.
//
// When the inner chain returned an ERROR there is no body, but the failure is
// still evidence, so it is graded on the way out by
// heuristic.AnalyzeTransportError: a dead host, a host that cannot do this
// method, or a client that hung up. That part runs regardless of the
// "heuristic" feature flag — grading a transport error is attribution, not
// response analysis, and the circuit breaker, the method blocks and
// reputation all key on it. The flag gates body analysis only.
func Heuristic(flags featureflag.FlagStore, registry *qos.Registry) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Run the inner chain first.
			if err := next.HandleRelay(ctx); err != nil {
				// No body to analyse, but the failure itself is evidence: a
				// dead host, a host that cannot do this method, or a client
				// that hung up. Without a verdict here none of that reached
				// the breaker, the method blocks, or reputation correctly.
				// The flag gate below is deliberately not applied: grading a
				// transport error is attribution, not response analysis.
				result := heuristic.AnalyzeTransportError(err, ctx.Ctx.Err())
				// The plugin may know the route better than the analyzer
				// (qos.VerdictRefiner): a verdict refined to "deliver" must
				// also stop Retry, which keys on the error's own flag.
				if refineVerdict(registry, ctx, &result) && !result.ShouldRetry && domain.IsRetryable(err) {
					if re, ok := err.(*domain.RelayError); ok {
						err = domain.NewRelayError(re.Kind, re.Message, re.Cause, false)
						ctx.Err = err
					}
				}
				ctx.HeuristicResult = &result
				return err
			}

			// Skip analysis if the flag is disabled.
			if flags != nil && !flags.IsEnabled(ctx.Ctx, featureflag.FlagHeuristic, ctx.ServiceID) {
				return nil
			}

			// Only analyse when we have a response.
			if ctx.Response == nil {
				return nil
			}

			// gRPC reports its outcome in grpc-status rather than in the body,
			// so it gets the analyzer that can read one. Without this a chain
			// error like NOT_FOUND would be retried across suppliers and
			// penalize each of them for answering correctly.
			var result heuristic.AnalysisResult
			if ctx.RPCType == domain.RPCTypeGRPC {
				code, message, ok := ctx.Response.GRPCStatus()
				result = heuristic.AnalyzeGRPC(ctx.Response.Body, code, message, ok)
			} else {
				result = heuristic.Analyze(
					ctx.Response.Body,
					ctx.Response.HTTPStatusCode,
					ctx.RPCType,
				)
			}

			// A -32601 on a method the plugin catalogues is retried on another
			// operator. The analyzer leaves it unretried because it cannot tell
			// a real method from a bogus name, and a bogus name must not bounce
			// across the pool; here the catalogue tells them apart. On the
			// 2026-09-13 canary two thirds of kava's json_rpc stakes fronted a
			// CometBFT node and answered eth_blockNumber with -32601; the
			// method block that verdict sets steers the NEXT request, this
			// retry serves the one in hand. PATH passes the -32601 to the
			// client. Attribution stays client so nothing is scored.
			if result.Reason == heuristic.ReasonMethodNotFound && !result.ShouldRetry && namedMethod(registry, ctx) {
				result.ShouldRetry = true
			}

			// The plugin's word on the route: a 5xx the node answers by
			// design to a query it cannot serve is the chain's answer, not
			// the host's failure (qos.VerdictRefiner).
			refineVerdict(registry, ctx, &result)

			// penalize_408 is the live undo for scoring a supplier's 408
			// (heuristic/analyzer.go): off, the 408 is still retried, not scored.
			if result.Reason == "http_408" && flags != nil && !flags.IsEnabled(ctx.Ctx, featureflag.FlagPenalize408, ctx.ServiceID) {
				result.ShouldPenalize = false
			}

			ctx.HeuristicResult = &result

			if result.ShouldRetry {
				relayErr := domain.NewRelayError(
					domain.ErrEndpoint,
					"heuristic analysis suggests retry: "+result.Reason,
					domain.ErrRetryVerdict,
					true,
				)
				ctx.Err = relayErr
				return relayErr
			}

			return nil
		})
	}
}

// refineVerdict lets the service's plugin re-attribute the verdict from the
// request's shape (qos.VerdictRefiner); reports whether it did.
func refineVerdict(registry *qos.Registry, ctx *relay.Context, result *heuristic.AnalysisResult) bool {
	if len(ctx.Payloads) == 0 {
		return false
	}
	plugin := ctx.Plugin
	if plugin == nil && registry != nil {
		plugin = registry.Get(ctx.ServiceID)
	}
	refiner, ok := plugin.(qos.VerdictRefiner)
	if !ok {
		return false
	}
	refined, ok := refiner.RefineVerdict(ctx.Payloads[0], *result)
	if !ok {
		return false
	}
	*result = refined
	return true
}

// namedMethod reports whether the request's method is one the service's
// plugin catalogues: not empty, not the MethodOther bucket.
func namedMethod(registry *qos.Registry, ctx *relay.Context) bool {
	m := normalizedMethod(registry, ctx)
	return m != "" && m != qos.MethodOther
}
