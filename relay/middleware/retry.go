package middleware

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// RetryRecorder is notified once per retry attempt with a coarse reason.
// metrics.Recorder satisfies it. Nil disables recording.
type RetryRecorder interface {
	RecordRetry(serviceID domain.ServiceID, reason string)
}

// Retry returns a middleware that retries failed relay attempts up to
// MaxRetries additional times, each on a different endpoint. It is
// RetryWithRecorder with no metric recorder. If the "retry" flag is disabled
// or MaxRetries==0 the middleware passes through.
func Retry(flags featureflag.FlagStore, configFn func(domain.ServiceID) config.RetryConfig) relay.Middleware {
	return RetryWithRecorder(flags, configFn, nil)
}

// RetryWithRecorder returns the retry middleware, recording sage_retry_total
// on each retry when rec is non-nil.
func RetryWithRecorder(flags featureflag.FlagStore, configFn func(domain.ServiceID) config.RetryConfig, rec RetryRecorder) relay.Middleware {
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

			// runAttempt runs one attempt under a per-attempt deadline (see
			// perAttemptContext), restoring ctx.Ctx afterwards so retry
			// bookkeeping and the caller see the original request context.
			runAttempt := func(attemptsLeft int) error {
				attemptCtx, cancel := perAttemptContext(ctx.Ctx, attemptsLeft)
				defer cancel()
				saved := ctx.Ctx
				ctx.Ctx = attemptCtx
				err := next.HandleRelay(ctx)
				ctx.Ctx = saved
				return err
			}

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
					// Attempts share the request deadline (timeout sits outside
					// retry). An attempt started with a sliver of it left cannot
					// succeed, and when the deadline fires on it the heuristic
					// grades it transport_timeout — a major penalty and a method
					// mark against an endpoint that had no real chance, for time
					// an earlier host consumed. Below a fifth of the budget the
					// last error is the honest answer.
					if !budgetForRetry(ctx.Ctx, start) {
						return lastErr
					}

					if rec != nil {
						rec.RecordRetry(ctx.ServiceID, retryReason(lastErr))
					}

					// A session rollover between selection and send leaves the
					// whole endpoint list from the old session — its other
					// members are stale too. Drop the list so the inner chain
					// (SelectEndpoint) refetches from the fresh session, and
					// reset the exclusion bookkeeping, which was keyed on the
					// old pool. Everything else about a retry is the same.
					if errors.Is(lastErr, domain.ErrEndpointsStale) {
						ctx.Endpoints = nil
						ctx.Endpoint = ""
						ctx.Response = nil
						ctx.Err = nil
						ctx.HeuristicResult = nil
						triedEndpoints = nil
						triedOperators = nil
						pool = nil
						if ctx.Logger != nil {
							ctx.Logger.Info("retrying relay after session rollover",
								slog.String("service_id", string(ctx.ServiceID)),
								slog.Int("attempt", attempt))
						}
						err := runAttempt(maxAttempts - attempt)
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
						continue
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

				err := runAttempt(maxAttempts - attempt)
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

// retryBudgetFraction is the share of the request's deadline budget that must
// remain for another attempt to be started. A fifth: enough for a warm relay
// on any sane relay_timeout, small enough that one slow host does not forfeit
// the retry outright.
const retryBudgetFraction = 5

// budgetForRetry reports whether enough of the request deadline remains to
// start another attempt. A context with no deadline always has budget.
func budgetForRetry(ctx context.Context, start time.Time) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(deadline) >= deadline.Sub(start)/retryBudgetFraction
}

// perAttemptContext bounds a single attempt to an equal share of the request's
// remaining deadline, so a slow or blackholing supplier cannot consume the
// whole budget and starve the attempt that would reach a healthy one. The last
// attempt (attemptsLeft <= 1) and a request with no deadline run unbounded —
// never cap the final try, and never invent a deadline where none exists.
//
// It only bites a supplier slower than the service's per-attempt expectation
// (relay_timeout / attempts); a healthy supplier answers well inside it. The
// hedge race honours ctx.Ctx.Done(), so this bounds a hedged attempt's WAIT
// too — the detached arms still finish and self-score.
func perAttemptContext(parent context.Context, attemptsLeft int) (context.Context, context.CancelFunc) {
	dl, ok := parent.Deadline()
	if !ok || attemptsLeft <= 1 {
		return context.WithCancel(parent)
	}
	remaining := time.Until(dl)
	if remaining <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, remaining/time.Duration(attemptsLeft))
}

// retryReason is the coarse label sage_retry_total carries. It stays low
// cardinality: a rollover, a timeout, or the failed attempt's error kind.
func retryReason(err error) string {
	if errors.Is(err, domain.ErrEndpointsStale) {
		return "rollover"
	}
	var re *domain.RelayError
	if errors.As(err, &re) {
		return re.Kind.String()
	}
	return "error"
}
