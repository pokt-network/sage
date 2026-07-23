package middleware

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// CrossValidator is implemented by any component that records response digests
// for background consensus checking.
type CrossValidator interface {
	// RecordDigest hashes responseBody and records it against the given
	// serviceID, endpoint, and method tuple. Implementations must be safe
	// for concurrent use.
	RecordDigest(serviceID domain.ServiceID, endpoint domain.EndpointAddr, method string, responseBody []byte)
}

// CrossValidate returns a middleware that, after the inner handler completes,
// records the response digest for background consensus analysis when the
// "cross_validation" feature flag is enabled.
//
// The method is extracted from the first payload (JSON-RPC method name or
// empty string for REST/CometBFT). Recording is best-effort: if there is no
// response body, the middleware is a no-op.
func CrossValidate(flags featureflag.FlagStore, validator CrossValidator) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			err := next.HandleRelay(ctx)

			// Only record when the flag is on and we have a response.
			if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagCrossValidation, ctx.ServiceID) {
				return err
			}

			if ctx.Response == nil || len(ctx.Response.Body) == 0 {
				return err
			}

			method := ""
			if len(ctx.Payloads) > 0 {
				method = ctx.Payloads[0].Method()
			}

			validator.RecordDigest(ctx.ServiceID, ctx.Endpoint, method, ctx.Response.Body)

			return err
		})
	}
}
