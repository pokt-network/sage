package middleware

import (
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// Shadow returns a middleware that enables "shadow mode" when the "shadow_mode"
// feature flag is on. In shadow mode the request is processed fully through
// the inner handler chain (including sending to the backend supplier), but the
// response is never written to the client.
//
// This is useful for:
//   - Dark-launching new suppliers without affecting client traffic.
//   - Benchmarking backend latency without serving the response.
//   - Validating response correctness in production without exposure.
//
// Shadow always returns nil so outer middleware treats the request as
// successful regardless of what the inner chain returned.
func Shadow(flags featureflag.FlagStore) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagShadowMode, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			// Run the full relay pipeline (including send to backend).
			_ = next.HandleRelay(ctx)

			// Suppress the response — client sees nothing.
			if ctx.Writer != nil {
				ctx.Writer.SetShadow(true)
			}

			// Always report success from the shadow middleware's perspective.
			return nil
		})
	}
}
