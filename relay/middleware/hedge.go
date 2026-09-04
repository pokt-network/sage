package middleware

import (
	"context"
	"errors"
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

// HedgeRecorder is notified of the outcome of a hedge race
// (primary_won, hedge_won, both_failed). metrics.Recorder satisfies it.
// Nil disables recording.
type HedgeRecorder interface {
	RecordHedge(serviceID domain.ServiceID, result string)
}

// Hedge returns a middleware that issues a speculative second ("hedge")
// request after HedgeDelay if the primary has not yet completed. The first
// successful response wins; if both fail, the primary error is returned. It is
// HedgeWithRecorder with no metric recorder. If the "hedge" flag is disabled
// or HedgeDelay==0 the middleware passes through.
func Hedge(flags featureflag.FlagStore, configFn func(domain.ServiceID) config.RetryConfig) relay.Middleware {
	return HedgeWithRecorder(flags, configFn, nil)
}

// HedgeWithRecorder returns the hedge middleware, recording sage_hedge_total
// on each resolved race when rec is non-nil.
func HedgeWithRecorder(flags featureflag.FlagStore, configFn func(domain.ServiceID) config.RetryConfig, rec HedgeRecorder) relay.Middleware {
	recordHedge := func(ctx *relay.Context, result string) {
		if rec != nil {
			rec.RecordHedge(ctx.ServiceID, result)
		}
	}
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
				defer safego.Recover(primaryCtx.Logger, "hedge.primary.goroutine")
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
				if res.err == nil {
					recordHedge(ctx, "primary_won")
				}
				return res.err

			case <-ctx.Ctx.Done():
				// The arms are detached so a signed relay in flight still
				// flushes; the WAIT is not. Nobody is listening for the answer
				// any more (the client hung up, or a deadline passed), and
				// holding here until the protocol's own HTTP client gave up
				// would make the per-service relay timeout mean nothing
				// whenever hedging is on. The arm scores itself when it
				// finishes; there is no winner to merge.
				return ctxDoneError(ctx.Ctx)

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
				defer safego.Recover(hedgeCtx.Logger, "hedge.hedge.goroutine")
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
				case <-ctx.Ctx.Done():
					// Same as above: stop waiting, let the arms finish detached.
					return ctxDoneError(ctx.Ctx)

				case res := <-primaryCh:
					primaryRes = res
					primaryDone = true
					if res.err == nil {
						mergeContext(ctx, res.ctx)
						recordHedge(ctx, "primary_won")
						return nil
					}
					// Primary failed — if hedge already succeeded, use it.
					if hedgeDone && hedgeRes.err == nil {
						mergeContext(ctx, hedgeRes.ctx)
						recordHedge(ctx, "hedge_won")
						return nil
					}

				case res := <-hedgeCh:
					hedgeRes = res
					hedgeDone = true
					if res.err == nil {
						mergeContext(ctx, res.ctx)
						recordHedge(ctx, "hedge_won")
						return nil
					}
					// Hedge failed — if primary already succeeded, use it.
					if primaryDone && primaryRes.err == nil {
						mergeContext(ctx, primaryRes.ctx)
						recordHedge(ctx, "primary_won")
						return nil
					}
				}
			}

			// Both failed — return the primary's error, and merge its context
			// the way a win is merged. Retry sits outside and excludes
			// ctx.Endpoint on its next attempt; with nothing merged it would
			// exclude "" and could draw the same dead endpoint again.
			mergeContext(ctx, primaryRes.ctx)
			recordHedge(ctx, "both_failed")
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
	// The winning arm's verdict, or Observe (which sits outside Hedge) never
	// sees one and falls back to grading by HTTP status: a transport timeout
	// would score as a minor error and a client hang-up would be scored
	// against the supplier, both only when hedging is on. Safe to copy by
	// pointer: the arm has already returned through its channel, so nothing
	// writes to it any more.
	dst.HeuristicResult = src.HeuristicResult
}

// ctxDoneError turns a finished request context into the error the hedge race
// returns. A deadline — the per-attempt cap Retry sets, or the request timeout
// — is a RETRYABLE transport error: Retry's budget guard then decides whether
// another attempt fits, so a capped hedged attempt can still reach a healthy
// supplier. A client cancel is nobody's to retry and stays as-is (Retry's own
// ctx.Ctx.Err() check returns without another attempt).
func ctxDoneError(ctx context.Context) error {
	err := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.NewRelayError(domain.ErrTransport, "hedge: attempt deadline exceeded", err, true)
	}
	return err
}
