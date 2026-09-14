package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// AttemptScaledTimeout turns a per-attempt relay timeout into the request
// deadline Timeout enforces: relay_timeout × (max_retries + 1).
//
// relay_timeout bounds ONE attempt — that is what its doc says and what PATH
// does (per sendHTTPRequest). The Timeout middleware sits outside Retry and
// Retry splits what is left of the deadline across the attempts left, so
// applying relay_timeout to the whole request gave the first attempt half of
// it: 2.5 s instead of 5 s on a 5 s service with one retry. On mainnet
// (2026-09-14) that turned sei's and solana's supplier tails into 504s PATH
// did not have. With the request deadline scaled by the attempt count, the
// split hands each attempt the full relay_timeout again.
//
// Zero passes through as zero: no timeout configured means no deadline.
func AttemptScaledTimeout(perAttempt func(domain.ServiceID) time.Duration, retryFn func(domain.ServiceID) config.RetryConfig) func(domain.ServiceID) time.Duration {
	return func(svc domain.ServiceID) time.Duration {
		d := perAttempt(svc)
		if d <= 0 || retryFn == nil {
			return d
		}
		if n := retryFn(svc).MaxRetries; n > 0 {
			d *= time.Duration(n + 1)
		}
		return d
	}
}

// Timeout returns a middleware that enforces a per-relay deadline.
// If the supplied configFn returns 0, the middleware passes through without
// adding a timeout.
func Timeout(configFn func(domain.ServiceID) time.Duration) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			d := configFn(ctx.ServiceID)
			if d == 0 {
				return next.HandleRelay(ctx)
			}

			timeoutCtx, cancel := context.WithTimeout(ctx.Ctx, d)
			defer cancel()

			ctx.Ctx = timeoutCtx

			err := next.HandleRelay(ctx)

			// Wrap any deadline-exceeded error (whether propagated by the inner
			// handler or swallowed) as a typed, retryable RelayError.
			if errors.Is(err, context.DeadlineExceeded) ||
				(err == nil && errors.Is(timeoutCtx.Err(), context.DeadlineExceeded)) {
				return domain.NewRelayError(
					domain.ErrTransport,
					"relay timeout exceeded",
					context.DeadlineExceeded,
					true, // retryable: a timeout on one endpoint shouldn't block others
				)
			}

			return err
		})
	}
}
