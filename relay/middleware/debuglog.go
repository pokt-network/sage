package middleware

import (
	"log/slog"
	"time"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// DebugLog returns a middleware that logs the full request and response at
// slog.LevelDebug when the "debug_log" feature flag is enabled.
//
// Logged fields:
//   - Request: request_id, service_id, endpoint, method, request_body
//   - Response: request_id, http_status, response_body, latency_ms
//
// If the flag is disabled the middleware is transparent with zero overhead.
func DebugLog(flags featureflag.FlagStore) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagDebugLog, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			// Capture request details before the relay.
			method := ""
			var requestBody []byte
			if len(ctx.Payloads) > 0 {
				method = ctx.Payloads[0].Method()
				requestBody = ctx.Payloads[0].Bytes()
			}

			ctx.Logger.Debug("relay_request",
				slog.String("request_id", ctx.RequestID),
				slog.String("service_id", string(ctx.ServiceID)),
				slog.String("endpoint", string(ctx.Endpoint)),
				slog.String("method", method),
				slog.String("request_body", string(requestBody)),
			)

			start := time.Now()
			err := next.HandleRelay(ctx)
			latency := time.Since(start)

			// Capture response details after the relay.
			httpStatus := 0
			var responseBody []byte
			if ctx.Response != nil {
				httpStatus = ctx.Response.HTTPStatusCode
				responseBody = ctx.Response.Body
			}

			ctx.Logger.Debug("relay_response",
				slog.String("request_id", ctx.RequestID),
				slog.Int("http_status", httpStatus),
				slog.String("response_body", string(responseBody)),
				slog.Int64("latency_ms", latency.Milliseconds()),
			)

			return err
		})
	}
}
