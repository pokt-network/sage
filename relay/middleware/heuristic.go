package middleware

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

// Heuristic returns a middleware that analyses the relay response after the
// inner chain has run. It runs heuristic.Analyze on the response body and
// stores the AnalysisResult on the context. When the result indicates the
// request should be retried, a retryable RelayError is returned so that outer
// retry/hedge middleware can react.
func Heuristic(flags featureflag.FlagStore) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Run the inner chain first.
			if err := next.HandleRelay(ctx); err != nil {
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

			ctx.HeuristicResult = &result

			if result.ShouldRetry {
				relayErr := domain.NewRelayError(
					domain.ErrEndpoint,
					"heuristic analysis suggests retry: "+result.Reason,
					nil,
					true,
				)
				ctx.Err = relayErr
				return relayErr
			}

			return nil
		})
	}
}
