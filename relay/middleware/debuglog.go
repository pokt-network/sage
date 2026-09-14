package middleware

import (
	"log/slog"
	"time"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// DebugLog returns a middleware that logs the full request and response at
// slog.LevelDebug when the "debug_log" feature flag is enabled. It sits
// inside select_endpoint, so one line pair is one attempt at one endpoint.
//
// Logged fields:
//   - relay_request: request_id, service_id, rpc_type, endpoint, url (what
//     the attempt dials for this RPC type, when the provider can say),
//     http_method, path, method (the wire method), normalized_method (the
//     plugin's catalogue name), request_body
//   - relay_response: request_id, endpoint, url, http_status, error (the
//     attempt's error, empty on success), response_body, latency_ms
//
// The URL is resolved because an endpoint address carries the supplier's
// primary URL, and an operator staking one host per RPC type dials a
// different one for REST; the ops-side reading of osmosis on 2026-09-14
// attributed REST traffic to the JSON-RPC host until the key labelling was
// fixed, and the debug line said the same wrong host.
//
// If the flag is disabled the middleware is transparent with zero overhead.
func DebugLog(flags featureflag.FlagStore, registry *qos.Registry, provider protocol.EndpointProvider) relay.Middleware {
	resolver, _ := provider.(protocol.URLResolver)
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagDebugLog, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			// Capture request details before the relay.
			method, httpMethod, path := "", "", ""
			var requestBody []byte
			if len(ctx.Payloads) > 0 {
				p := ctx.Payloads[0]
				method = p.Method()
				httpMethod = p.HTTPMethod()
				path = p.Path()
				requestBody = p.Bytes()
			}
			if httpMethod == "" {
				httpMethod = "POST"
			}
			if path == "" {
				path = "/"
			}
			dialed := ""
			if resolver != nil && ctx.Endpoint != "" {
				if u, ok := resolver.EndpointURLFor(ctx.Endpoint, ctx.RPCType); ok {
					dialed = u
				}
			}

			ctx.Logger.Debug("relay_request",
				slog.String("request_id", ctx.RequestID),
				slog.String("service_id", string(ctx.ServiceID)),
				slog.String("rpc_type", string(ctx.RPCType)),
				slog.String("endpoint", string(ctx.Endpoint)),
				slog.String("url", dialed),
				slog.String("http_method", httpMethod),
				slog.String("path", path),
				slog.String("method", method),
				slog.String("normalized_method", normalizedMethod(registry, ctx)),
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
			errText := ""
			if err != nil {
				errText = err.Error()
			}

			ctx.Logger.Debug("relay_response",
				slog.String("request_id", ctx.RequestID),
				slog.String("endpoint", string(ctx.Endpoint)),
				slog.String("url", dialed),
				slog.Int("http_status", httpStatus),
				slog.String("error", errText),
				slog.String("response_body", string(responseBody)),
				slog.Int64("latency_ms", latency.Milliseconds()),
			)

			return err
		})
	}
}
