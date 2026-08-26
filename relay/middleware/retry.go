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
			var triedOperators map[string]bool
			// The candidate pool as it stood after the first attempt, before any
			// per-attempt narrowing. Retries are derived from THIS rather than
			// from the previous attempt's list: operator preference narrows the
			// pool for one attempt only, and compounding those narrowings across
			// attempts would strand later retries with nothing to pick from.
			var pool domain.EndpointAddrList
			operatorAware := flags.IsEnabled(ctx.Ctx, featureflag.FlagOperatorAwareSelection, ctx.ServiceID)
			start := time.Now()

			var lastErr error
			maxAttempts := cfg.MaxRetries + 1

			for attempt := 0; attempt < maxAttempts; attempt++ {
				if attempt > 0 {
					// Nobody is waiting for a retry once the request context is
					// done: the client hung up, or the Timeout middleware has
					// already answered. Every further attempt would select and
					// sign a relay that fails on arrival with the same context
					// error — and each of those is a failure recorded against a
					// supplier for something the supplier did not do.
					if ctx.Ctx.Err() != nil {
						return lastErr
					}

					// Exclude the endpoint we just tried.
					if triedEndpoints == nil {
						triedEndpoints = make(map[domain.EndpointAddr]bool, maxAttempts)
						triedOperators = make(map[string]bool, maxAttempts)
						pool = ctx.Endpoints
					}
					triedEndpoints[ctx.Endpoint] = true
					triedOperators[ctx.Endpoint.Operator()] = true

					available := pool.Exclude(triedEndpoints)
					if len(available) == 0 {
						break
					}

					// Prefer an operator we have not already failed against. A
					// retry exists to reach different infrastructure, and one
					// operator's hostnames are one operator's infrastructure —
					// avoiding only the failed endpoint can land the retry on
					// the same rack behind the same outage. This is a
					// preference, not a filter: ExcludeOperators returns the
					// list untouched when every remaining candidate belongs to
					// an operator we have tried.
					if operatorAware {
						available = available.ExcludeOperators(triedOperators)
					}
					ctx.Endpoints = available

					// Clear selected endpoint to force re-selection by inner chain.
					ctx.Endpoint = ""
					// Clear previous response/error state.
					ctx.Response = nil
					ctx.Err = nil
					// A heuristic verdict belongs to the attempt that produced
					// it. Without this, an attempt whose own Heuristic pass
					// is skipped (flag off, or no response to analyse) would
					// read the PRIOR attempt's verdict — e.g. MethodBlocks
					// would mark this attempt's (healthy) endpoint for a
					// MethodBlocking verdict some earlier, different endpoint
					// actually earned.
					ctx.HeuristicResult = nil

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
