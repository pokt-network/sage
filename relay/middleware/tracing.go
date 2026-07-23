package middleware

import (
	"log/slog"
	"time"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// Tracing returns a middleware that adds timing instrumentation to the logger
// context when the "tracing" feature flag is enabled.
//
// Current implementation (Phase 1 stub):
//   - Logs "relay_start" with request_id and service_id at the beginning.
//   - Logs "relay_end" with request_id, service_id, endpoint, and
//     duration_ms at the end.
//
// A future phase will replace the logger-based instrumentation with real
// OpenTelemetry spans once the OTel SDK dependency is added to go.mod.
func Tracing(flags featureflag.FlagStore) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagTracing, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			start := time.Now()

			ctx.Logger.Debug("relay_start",
				slog.String("request_id", ctx.RequestID),
				slog.String("service_id", string(ctx.ServiceID)),
			)

			err := next.HandleRelay(ctx)

			duration := time.Since(start)

			ctx.Logger.Debug("relay_end",
				slog.String("request_id", ctx.RequestID),
				slog.String("service_id", string(ctx.ServiceID)),
				slog.String("endpoint", string(ctx.Endpoint)),
				slog.Int64("duration_ms", duration.Milliseconds()),
			)

			// TODO(phase-2): replace with real OpenTelemetry spans:
			//   span := tracer.Start(ctx.Ctx, "relay")
			//   defer span.End()
			//   span.SetAttributes(attribute.String("service.id", string(ctx.ServiceID)))

			return err
		})
	}
}
