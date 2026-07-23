package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

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
