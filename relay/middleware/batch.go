package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/relay"
)

// Batch returns a middleware that fans out multi-payload requests into
// individual relays that run in parallel (bounded by maxConcurrentRelays), then
// merges the results into a JSON array response.
//
// Single-payload requests (len(ctx.Payloads) <= 1) pass through unchanged.
//
// If any individual relay fails, its error is captured as a JSON-RPC error
// object in the combined response rather than failing the entire batch.
// The final HTTP status is always 200.
//
// maxConcurrentRelays is a GLOBAL ceiling on sub-relay goroutines in flight —
// the semaphore is built once here, not per request, which is the only way the
// bound means anything. A per-request semaphore bounds one batch and nothing
// else: N concurrent batches each get their own, so the total is N × the
// "limit". (PATH documents max_concurrent_relays as global and implements it
// per-request; that gap is not worth copying.) <= 0 disables the bound.
//
// Acquisition respects the request deadline, so a saturated budget cannot
// outlive the Timeout middleware's context: a shared semaphore couples requests
// together, and without that check one stuck supplier could hold slots past the
// point anyone is still waiting for the answer.
//
// maxPayloads caps how many payloads one request may fan out into. It is
// rejected up front rather than absorbed, because a batch is an amplifier: one
// HTTP request becomes len(Payloads) upstream relays, each with its own retry
// and hedge fan-out. Without a cap the only limit is the request body size, so
// a 1 MiB body of ~50-byte payloads buys roughly 20k relays. <= 0 disables the
// cap.
func Batch(maxConcurrentRelays, maxPayloads int) relay.Middleware {
	// Built once per process, deliberately: see above.
	var sem chan struct{}
	if maxConcurrentRelays > 0 {
		sem = make(chan struct{}, maxConcurrentRelays)
	}

	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if len(ctx.Payloads) <= 1 {
				return next.HandleRelay(ctx)
			}

			if maxPayloads > 0 && len(ctx.Payloads) > maxPayloads {
				ctx.Err = domain.NewRelayError(
					domain.ErrValidation,
					fmt.Sprintf("batch has %d payloads, limit is %d", len(ctx.Payloads), maxPayloads),
					nil,
					false,
				)
				if ctx.Writer != nil {
					ctx.Writer.SetStatusCode(http.StatusRequestEntityTooLarge)
					_ = ctx.Writer.Write(errorJSON(ctx.Err.Error()))
				}
				return ctx.Err
			}

			n := len(ctx.Payloads)
			results := make([]json.RawMessage, n)

			// degraded records whether ANY sub-relay fell back. Each goroutine owns
			// results[i], but this is shared, so it is an atomic rather than a
			// field on the parent context.
			var degraded atomic.Bool

			var wg sync.WaitGroup
			wg.Add(n)

			for i, payload := range ctx.Payloads {
				i, payload := i, payload // capture loop variables

				// Acquire before spawning, so the ceiling bounds goroutines that
				// exist rather than goroutines that have already been created.
				if sem != nil && !acquire(ctx.Ctx, sem) {
					// The request is over; do not queue behind the budget for an
					// answer nobody is waiting for. Report the rest and stop.
					for j := i; j < n; j++ {
						results[j] = errorJSON("batch payload not started: " + ctx.Ctx.Err().Error())
						wg.Done()
					}
					break
				}

				go func() {
					defer func() {
						if sem != nil {
							<-sem
						}
						wg.Done()
					}()

					sub := ctx.Clone()
					sub.Payloads = []domain.Payload{payload}
					sub.Response = nil
					sub.Err = nil

					// A panic here becomes this payload's error rather than the
					// process's exit: wg.Done() already runs on the way out, so
					// an unconverted panic would leave results[i] empty and the
					// client would get a null where an error belongs.
					err := safego.Call(sub.Logger, "batch.payload", func() error {
						return next.HandleRelay(sub)
					})
					if sub.Degraded {
						degraded.Store(true)
					}
					if err != nil {
						results[i] = errorJSON(err.Error())
						return
					}
					if sub.Response != nil && len(sub.Response.Body) > 0 {
						results[i] = json.RawMessage(sub.Response.Body)
					} else {
						results[i] = json.RawMessage(`null`)
					}
				}()
			}

			wg.Wait()

			// Sub-relays run on clones, so a fallback in any of them is invisible
			// to the caller unless it is merged back — the batch response is
			// partly degraded if any part of it was. Mirrors hedge.mergeContext,
			// which does the same for its winning arm.
			if degraded.Load() {
				ctx.Degraded = true
			}

			combined, err := json.Marshal(results)
			if err != nil {
				// Extremely unlikely; fall back to an error response.
				combined = []byte(`{"error":"failed to combine batch responses"}`)
			}

			ctx.Response = &domain.Response{
				Body:           combined,
				HTTPStatusCode: http.StatusOK,
			}
			return nil
		})
	}
}

// acquire takes a slot from the shared budget, reporting false when the
// request's context ended first.
//
// The explicit Err check is not redundant with the select below: select picks a
// *random* ready case, so a cancelled request racing a free slot would take the
// budget half the time — spending a global resource on an answer nobody is
// waiting for, and starting a sub-relay the caller already gave up on.
func acquire(ctx context.Context, sem chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// errorJSON returns a minimal JSON-RPC error object as a raw message.
func errorJSON(msg string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":    -32603,
			"message": msg,
		},
	})
	return json.RawMessage(b)
}
