package middleware

import (
	"context"
	"sync/atomic"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/relay"
)

// hedgeResult carries the outcome of one arm of a hedge race.
type hedgeResult struct {
	err error
	ctx *relay.Context
}

// Hedge returns a middleware that issues a speculative second ("hedge") request
// after HedgeDelay if the primary has not yet completed. The first successful
// response wins; if both fail, the primary error is returned.
// If the "hedge" feature flag is disabled or HedgeDelay==0, the middleware
// passes through to the inner handler without wrapping.
func Hedge(flags featureflag.FlagStore, configFn func(domain.ServiceID) config.RetryConfig) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if !flags.IsEnabled(ctx.Ctx, featureflag.FlagHedge, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			cfg := configFn(ctx.ServiceID)
			if cfg.HedgeDelay == 0 {
				return next.HandleRelay(ctx)
			}

			primaryCh := make(chan hedgeResult, 1)
			hedgeCh := make(chan hedgeResult, 1)

			// Start primary request. Each hedge arm runs on a context detached
			// from the caller's request context (context.WithoutCancel) so that
			// when the winner is chosen and this middleware returns, the losing
			// arm's in-flight *signed* relay still flushes to the supplier
			// cleanly instead of being torn down (TCP RST). A supplier that sees
			// the reset wastes a signed relay and reads it as gateway
			// misbehavior. Each arm cancels its own detached context once its
			// relay completes, so nothing leaks — the relay itself is
			// independently bounded by the protocol's HTTP client timeout.
			primaryCtx := ctx.Clone()
			// Give the primary arm a slot to publish its endpoint into. Reading
			// primaryCtx.Endpoint from this goroutine instead is a data race:
			// the arm's SelectEndpoint writes that field concurrently, and
			// nothing orders the write against the read below.
			primaryCtx.SelectedEndpoint = new(atomic.Pointer[domain.EndpointAddr])
			primaryDetached, primaryCancel := context.WithCancel(context.WithoutCancel(ctx.Ctx))
			primaryCtx.Ctx = primaryDetached
			go func() {
				defer primaryCancel()
				// safego.Call rather than a bare recover: an arm that recovered
				// without sending would leave the select below waiting on a
				// channel nothing will ever write to, which turns a crash into a
				// hung request. Converting the panic to an error lets the race
				// resolve the way it already resolves a failed arm.
				err := safego.Call(primaryCtx.Logger, "hedge.primary", func() error {
					return next.HandleRelay(primaryCtx)
				})
				primaryCh <- hedgeResult{err: err, ctx: primaryCtx}
			}()

			// Wait for HedgeDelay or primary completion.
			timer := acquireTimer(cfg.HedgeDelay)
			defer releaseTimer(timer)

			select {
			case res := <-primaryCh:
				// Primary finished before the hedge delay — return its result.
				mergeContext(ctx, res.ctx)
				return res.err

			case <-timer.C:
				// Hedge delay elapsed; launch speculative second request.
			}

			// Build a clone for the hedge with a different endpoint excluded.
			hedgeCtx := ctx.Clone()
			// Exclude the primary's current endpoint so the hedge picks a
			// different one, and prefer a different OPERATOR: the point of a
			// hedge is a second, independent path to an answer, and two
			// hostnames run by the same provider are not independent. The
			// operator step is a preference — ExcludeOperators leaves the list
			// alone when the primary's operator is the only one left — so a
			// single-operator pool still hedges exactly as before.
			//
			// A nil slot means the primary had not selected yet when the delay
			// elapsed; there is nothing to steer away from, so the hedge simply
			// picks from the full list as it would have anyway.
			if primary := primaryCtx.SelectedEndpoint.Load(); primary != nil && *primary != "" {
				hedgeCtx.Endpoints = hedgeCtx.Endpoints.Exclude(
					map[domain.EndpointAddr]bool{*primary: true},
				)
				if flags.IsEnabled(ctx.Ctx, featureflag.FlagOperatorAwareSelection, ctx.ServiceID) {
					hedgeCtx.Endpoints = hedgeCtx.Endpoints.ExcludeOperators(
						map[string]bool{primary.Operator(): true},
					)
				}
			}
			// Force endpoint re-selection for the hedge.
			hedgeCtx.SelectedEndpoint = new(atomic.Pointer[domain.EndpointAddr])
			hedgeCtx.Endpoint = ""
			hedgeCtx.Response = nil
			hedgeCtx.Err = nil

			// Detached context — same rationale as the primary arm above.
			hedgeDetached, hedgeCancel := context.WithCancel(context.WithoutCancel(ctx.Ctx))
			hedgeCtx.Ctx = hedgeDetached
			go func() {
				defer hedgeCancel()
				err := safego.Call(hedgeCtx.Logger, "hedge.hedge", func() error {
					return next.HandleRelay(hedgeCtx)
				})
				hedgeCh <- hedgeResult{err: err, ctx: hedgeCtx}
			}()

			// Race: first successful result wins.
			var primaryRes, hedgeRes hedgeResult
			primaryDone := false
			hedgeDone := false

			for !primaryDone || !hedgeDone {
				select {
				case res := <-primaryCh:
					primaryRes = res
					primaryDone = true
					if res.err == nil {
						mergeContext(ctx, res.ctx)
						return nil
					}
					// Primary failed — if hedge already succeeded, use it.
					if hedgeDone && hedgeRes.err == nil {
						mergeContext(ctx, hedgeRes.ctx)
						return nil
					}

				case res := <-hedgeCh:
					hedgeRes = res
					hedgeDone = true
					if res.err == nil {
						mergeContext(ctx, res.ctx)
						return nil
					}
					// Hedge failed — if primary already succeeded, use it.
					if primaryDone && primaryRes.err == nil {
						mergeContext(ctx, primaryRes.ctx)
						return nil
					}
				}
			}

			// Both failed — return primary error.
			return primaryRes.err
		})
	}
}

// mergeContext copies the result fields from src into dst so callers see
// the winning response.
func mergeContext(dst, src *relay.Context) {
	dst.Endpoint = src.Endpoint
	dst.Response = src.Response
	dst.Err = src.Err
	dst.Degraded = src.Degraded
	dst.Cached = src.Cached
	dst.Coalesced = src.Coalesced
}
