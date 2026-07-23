package middleware

import (
	"log/slog"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// Retry returns a middleware that retries failed relay attempts up to MaxRetries
// additional times, each time on a different endpoint than previously tried.
// If the "retry" feature flag is disabled or MaxRetries==0, the middleware
// passes through to the inner handler without wrapping.
func Retry(flags featureflag.FlagStore, configFn func(domain.ServiceID) config.RetryConfig) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if !flags.IsEnabled(ctx.Ctx, featureflag.FlagRetry, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			cfg := configFn(ctx.ServiceID)
			if cfg.MaxRetries == 0 {
				return next.HandleRelay(ctx)
			}

			// Allocated lazily — the no-retry success path (the overwhelming
			// majority) never needs it.
			var triedEndpoints map[domain.EndpointAddr]bool
			start := time.Now()

			var lastErr error
			maxAttempts := cfg.MaxRetries + 1

			for attempt := 0; attempt < maxAttempts; attempt++ {
				if attempt > 0 {
					// Exclude the endpoint we just tried.
					if triedEndpoints == nil {
						triedEndpoints = make(map[domain.EndpointAddr]bool, maxAttempts)
					}
					triedEndpoints[ctx.Endpoint] = true

					ctx.Endpoints = ctx.Endpoints.Exclude(triedEndpoints)
					if len(ctx.Endpoints) == 0 {
						break
					}

					// Clear selected endpoint to force re-selection by inner chain.
					ctx.Endpoint = ""
					// Clear previous response/error state.
					ctx.Response = nil
					ctx.Err = nil

					if ctx.Logger != nil {
						ctx.Logger.Info("retrying relay",
							slog.String("service_id", string(ctx.ServiceID)),
							slog.Int("attempt", attempt),
							slog.Int("max_retries", cfg.MaxRetries),
							slog.String("last_error", lastErr.Error()),
						)
					}
				}

				err := next.HandleRelay(ctx)
				if err == nil {
					return nil
				}

				lastErr = err

				if !domain.IsRetryable(err) {
					return err
				}

				if cfg.MaxLatency > 0 && time.Since(start) >= cfg.MaxLatency {
					return err
				}
			}

			return lastErr
		})
	}
}
