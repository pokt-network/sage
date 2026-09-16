package middleware

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

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
			if ctx.QuorumArm || !flags.IsEnabled(ctx.Ctx, featureflag.FlagHedge, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			cfg := configFn(ctx.ServiceID)
			if cfg.HedgeDelay == 0 {
				return next.HandleRelay(ctx)
			}
			// An item of a large batch runs unhedged. Every item is its own
			// hedge race, so a 500-item batch that runs past hedge_delay —
			// which large batches do by construction — costs up to 1,000
			// relays in flight, each a goroutine holding a signed relay.
			// Twelve such bursts on the canary in five days, one of them the
			// 2026-09-10 OOM. PATH caps the same way (hedge_max_batch_size).
			if !cfg.HedgesBatchOf(ctx.BatchSize) {
				recordHedge(ctx, "suppressed_large_batch")
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
			primaryDetached, primaryCancel := armContext(ctx.Ctx)
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
			hedgeDetached, hedgeCancel := armContext(ctx.Ctx)
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

// armContext is the context a hedge arm runs on: detached from the caller's
// cancellation, so a losing arm still flushes its signed relay when the race
// resolves (see the primary arm above), but NOT detached from time. Its
// deadline is the attempt's own plus the same again — the caller waited W, the
// arm may run to 2W to flush or to self-score — after which the protocol's
// HTTP client cancels the relay.
//
// Without a deadline the only bound on an arm was that client's timeout,
// which is the GLOBAL relay_timeout (30s when unset) and not the service's.
// A supplier that accepts and hangs on a busy service then stacked
// 2×(max_retries+1) arms per request for 30s each, ~3 goroutines and a signed
// relay apiece, at a normal request rate: the 2026-09-10 canary pod went from
// 552 to 5,257 goroutines inside one minute and was OOM-killed at 1Gi.
//
// Under retry's per-attempt split (relay_timeout / attempts) 2W lands on the
// service's own relay_timeout, which is the bound the config documents for
// one attempt. A parent with no deadline gets none, as before.
//
// ponytail: 2× the caller's remaining wait; use the service relay_timeout
// directly if hedge ever learns it.
func armContext(parent context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(parent)
	dl, ok := parent.Deadline()
	if !ok {
		return context.WithCancel(detached)
	}
	grace := time.Until(dl)
	if grace < 0 {
		grace = 0
	}
	return context.WithDeadline(detached, dl.Add(grace))
}

// mergeContext copies the result fields from src into dst so callers see
// the winning response.
func mergeContext(dst, src *relay.Context) {
	dst.Endpoint = src.Endpoint
	// The candidate pool, which only the arm ever saw. SelectEndpoint runs
	// INSIDE the race, so it fills Endpoints on the arm's clone; Clone is a
	// value copy, so the parent's stays empty. Retry sits outside and derives
	// its retry pool from the parent's Endpoints — an empty pool excludes to
	// an empty candidate list and Retry breaks out of its loop without ever
	// making a second attempt. Retry was therefore inert on every hedged
	// service, silently, since the two middlewares were first ordered this
	// way. Found on 2026-09-06 because sage_retry_resolution_total went to
	// zero series: the counter records only retries that actually run.
	dst.Endpoints = src.Endpoints
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
