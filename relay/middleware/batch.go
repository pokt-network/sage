package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// Batch returns a middleware that fans out multi-payload requests into
// individual relays that run in parallel (bounded by max_concurrent_relays), then
// merges the results into a JSON array response.
//
// Single-payload requests (len(ctx.Payloads) <= 1) pass through unchanged.
//
// If any individual relay fails, its error is captured as a JSON-RPC error
// response object — carrying that request's own id — in the combined response
// rather than failing the entire batch. Per JSON-RPC 2.0 every request object
// in a batch gets exactly one response object, and the id is how the client
// matches them; an error without one is unattributable to the client, and the
// N−1 answers beside it were already relayed and paid for. The final HTTP
// status is always 200.
//
// limits is read on every batch, so both bounds can change on a running process
// (a config apply swaps the snapshot it reads).
//
// max_concurrent_relays is a GLOBAL ceiling on sub-relay goroutines in flight —
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
// max_batch_payloads caps how many payloads one request may fan out into. It is
// rejected up front rather than absorbed, because a batch is an amplifier: one
// HTTP request becomes len(Payloads) upstream relays, each with its own retry
// and hedge fan-out. Without a cap the only limit is the request body size, so
// a 1 MiB body of ~50-byte payloads buys roughly 20k relays. <= 0 disables the
// cap.
//
// repSvc and flags exist for reputation only: under featureflag.FlagScoringV2
// the batch installs a relay.ScoreSink so the whole fan-out costs an endpoint
// one signal rather than one per payload (docs/scoring.md §4.3). Both may be
// nil, which disables that and leaves scoring to Observe as before.
//
// recorder may be nil.
func Batch(limits BatchLimits, flags featureflag.FlagStore, repSvc reputation.Service, recorder BatchRecorder) relay.Middleware {
	if recorder == nil {
		recorder = noopBatchRecorder{}
	}
	// Shared by every request, deliberately: see above. Rebuilt only when
	// max_concurrent_relays changes.
	var budget atomic.Pointer[chan struct{}]
	semFor := func(maxConcurrentRelays int) chan struct{} {
		if maxConcurrentRelays <= 0 {
			return nil
		}
		cur := budget.Load()
		if cur != nil && cap(*cur) == maxConcurrentRelays {
			return *cur
		}
		// ponytail: a resized budget is a fresh channel, so until the slots
		// held on the old one are released (one request timeout at most) the
		// true ceiling is old + new. Fine for tuning mid-incident; a shrink
		// that must bind instantly needs a mutex-counted semaphore.
		next := make(chan struct{}, maxConcurrentRelays)
		if budget.CompareAndSwap(cur, &next) {
			return next
		}
		return *budget.Load()
	}

	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if len(ctx.Payloads) <= 1 {
				return next.HandleRelay(ctx)
			}
			start := time.Now()
			maxConcurrentRelays, maxPayloads, maxPerBatch := limits()
			// Before the cap, so a rejected batch still says how large the
			// batches clients send are.
			recorder.RecordBatchPayloads(ctx.ServiceID, len(ctx.Payloads))
			// Released to the channel it was taken from, which a resize may
			// already have replaced.
			sem := semFor(maxConcurrentRelays)

			if maxPayloads > 0 && len(ctx.Payloads) > maxPayloads {
				return rejectRequest(ctx, nil, http.StatusRequestEntityTooLarge, domain.ErrValidation,
					fmt.Sprintf("batch has %d payloads, limit is %d", len(ctx.Payloads), maxPayloads), nil,
					map[string]any{"max_batch_payloads": maxPayloads})
			}

			// One signal per endpoint for the whole batch (docs/scoring.md
			// §4.3): the sink is shared by every sub-relay through the
			// shallow clone, and flushed once every payload has returned.
			// Only under scoring_v2 — with the flag off, Observe scores each
			// sub-relay as before.
			//
			// The sink carries no attribution: dropping client-attributed
			// outcomes is the caller's job, done before Add by the score
			// middleware, which is what makes "a batch of only client errors
			// scores nothing" hold — nothing was added, so the flush is empty.
			if repSvc != nil && scoringV2Enabled(flags, ctx) {
				sink := relay.NewScoreSink()
				ctx.ScoreSink = sink
				defer func() {
					ctx.ScoreSink = nil
					sink.Flush(func(ep domain.EndpointAddr, rpc domain.RPCType, sig reputation.Signal) {
						// context.Background(), not ctx.Ctx: the request's
						// context is routinely done by the time the last
						// payload returns, and a signal dropped for that
						// reason is exactly the failure worth recording.
						_ = repSvc.RecordSignal(context.Background(), ctx.ServiceID, ep, rpc, sig)
					})
				}()
			}

			n := len(ctx.Payloads)
			results := make([]json.RawMessage, n)

			// degraded records whether ANY sub-relay fell back. Each goroutine owns
			// results[i], but this is shared, so it is an atomic rather than a
			// field on the parent context.
			var degraded atomic.Bool

			// Response bytes this batch holds, released from the gauge when
			// it returns. The merged copy made below is not counted, so the
			// true peak per batch is about twice this.
			var heldBytes atomic.Int64
			defer func() { recorder.AddBatchResponseBytes(-heldBytes.Load()) }()

			// A batch's own ceiling, under the global one. The global budget
			// bounds slots and says nothing about bytes: on 2026-09-16 one pod
			// had 514 sub-relays of batches under 1000 payloads reading at once,
			// each body held three to four times over while it is decoded, and
			// was OOM-killed inside one scrape with the 10000-slot budget never
			// engaged. Taken before the global slot, so a batch waiting on its
			// own ceiling holds nothing anyone else needs.
			var local chan struct{}
			if maxPerBatch > 0 && n > maxPerBatch {
				local = make(chan struct{}, maxPerBatch)
				recorder.RecordBatchConcurrencyCapped(ctx.ServiceID, n)
			}

			var wg sync.WaitGroup
			wg.Add(n)

			for i, payload := range ctx.Payloads {
				i, payload := i, payload // capture loop variables

				// Acquire before spawning, so the ceiling bounds goroutines that
				// exist rather than goroutines that have already been created.
				gotLocal := local == nil || acquire(ctx.Ctx, local)
				gotGlobal := gotLocal && (sem == nil || acquire(ctx.Ctx, sem))
				if gotLocal && !gotGlobal && local != nil {
					<-local
				}
				if !gotGlobal {
					// The request is over; do not queue behind the budget for an
					// answer nobody is waiting for. Report the rest and stop.
					for j := i; j < n; j++ {
						results[j] = jsonRPCErrorItem(ctx.Payloads[j].JSONRPCID(),
							"batch payload not started: "+ctx.Ctx.Err().Error())
						wg.Done()
					}
					break
				}

				recorder.AddBatchSubRelays(1)
				go func() {
					// First, so it runs last: safego.Call below converts a
					// panic inside the relay into this payload's error, and
					// this contains one in the clone and result bookkeeping
					// around it — after wg.Done has already run, so the batch
					// still completes.
					defer safego.Recover(ctx.Logger, "batch.payload.goroutine")
					defer func() {
						recorder.AddBatchSubRelays(-1)
						if sem != nil {
							<-sem
						}
						if local != nil {
							<-local
						}
						wg.Done()
					}()

					sub := ctx.Clone()
					sub.Payloads = []domain.Payload{payload}
					sub.BatchSize = n
					sub.Response = nil
					sub.Err = nil
					// A verdict belongs to the attempt that produced it, and
					// Clone() is shallow — without this a sub-relay starts out
					// holding the parent's.
					sub.HeuristicResult = nil

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
					// domain.ClientMessage, not err.Error(): the cause chain
					// names the operator's own infrastructure, exactly as on the
					// single-request path in router.writeRelayError.
					if err != nil {
						results[i] = jsonRPCErrorItem(payload.JSONRPCID(), domain.ClientMessage(err))
						return
					}
					// An empty body is not a response object; a null in its
					// place is not one either. The client asked a question and
					// gets an error it can attribute.
					if sub.Response == nil || len(sub.Response.Body) == 0 {
						results[i] = jsonRPCErrorItem(payload.JSONRPCID(), "endpoint error: empty response")
						return
					}
					results[i] = json.RawMessage(sub.Response.Body)
					heldBytes.Add(int64(len(sub.Response.Body)))
					recorder.AddBatchResponseBytes(int64(len(sub.Response.Body)))
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

			combined, err := mergeBatch(results)
			if err != nil {
				// Extremely unlikely; fall back to an error response.
				combined = []byte(`{"error":"failed to combine batch responses"}`)
			}

			ctx.Response = &domain.Response{
				Body:           combined,
				HTTPStatusCode: http.StatusOK,
			}
			// Merge included; the router's write is not, and is in
			// sage_stage_seconds_total{stage="router_write"}.
			recorder.RecordBatchSeconds(ctx.ServiceID, n, time.Since(start))
			return nil
		})
	}
}

// BatchRecorder is what the batch middleware reports, for sizing
// max_batch_payloads and max_concurrent_relays from data.
type BatchRecorder interface {
	// RecordBatchPayloads observes one multi-payload request's payload count,
	// including one about to be refused for exceeding the cap.
	RecordBatchPayloads(serviceID domain.ServiceID, n int)
	// AddBatchSubRelays moves the count of sub-relays running.
	AddBatchSubRelays(delta int)
	// AddBatchResponseBytes moves the count of sub-relay response bytes held
	// by batches that have not returned.
	AddBatchResponseBytes(delta int64)
	// RecordBatchConcurrencyCapped counts a batch of n payloads larger than
	// max_batch_concurrency, which runs its sub-relays that many at a time.
	RecordBatchConcurrencyCapped(serviceID domain.ServiceID, n int)
	// RecordBatchSeconds observes a batch of n payloads from the middleware's
	// entry to its merged response. A batch refused over max_batch_payloads
	// is not observed.
	RecordBatchSeconds(serviceID domain.ServiceID, n int, d time.Duration)
}

type noopBatchRecorder struct{}

func (noopBatchRecorder) RecordBatchPayloads(domain.ServiceID, int)               {}
func (noopBatchRecorder) AddBatchSubRelays(int)                                   {}
func (noopBatchRecorder) AddBatchResponseBytes(int64)                             {}
func (noopBatchRecorder) RecordBatchConcurrencyCapped(domain.ServiceID, int)      {}
func (noopBatchRecorder) RecordBatchSeconds(domain.ServiceID, int, time.Duration) {}

// BatchLimits reports max_concurrent_relays and max_batch_payloads. <= 0
// disables either bound.
type BatchLimits func() (maxConcurrentRelays, maxPayloads, maxPerBatch int)

// mergeBatch renders the batch response array: byte for byte what
// json.Marshal of the []json.RawMessage produces — each item compacted and
// HTML-escaped, a nil item as null, and an error for an item that is not JSON
// — without Marshal's working copy. Marshal builds the array in a buffer that
// grows by doubling and then returns a copy of it, so the merge of a large
// batch held its response bytes up to three times over at the moment the
// batch was largest. Here the output is sized up front and each item passes
// through one scratch buffer at a time.
//
// Compacting and then escaping is the same as Marshal's single pass: in
// compact JSON the characters HTMLEscape rewrites can only appear inside
// strings, which is where Marshal escapes them.
func mergeBatch(results []json.RawMessage) ([]byte, error) {
	size := len(results) + 1
	for _, r := range results {
		size += len(r)
	}
	out := bytes.NewBuffer(make([]byte, 0, size))
	var scratch bytes.Buffer
	out.WriteByte('[')
	for i, r := range results {
		if i > 0 {
			out.WriteByte(',')
		}
		if r == nil {
			out.WriteString("null")
			continue
		}
		scratch.Reset()
		if err := json.Compact(&scratch, r); err != nil {
			return nil, err
		}
		json.HTMLEscape(out, scratch.Bytes())
	}
	out.WriteByte(']')
	return out.Bytes(), nil
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

// jsonRPCErrorItem builds the response object for one batch item nothing
// answered: a JSON-RPC 2.0 error response carrying the request's own id, with
// its JSON type intact, next to the successes. This is what the single-request
// path returns for the same failure and what a node does.
func jsonRPCErrorItem(id json.RawMessage, msg string) json.RawMessage {
	b, err := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   map[string]any  `json:"error"`
	}{
		JSONRPC: "2.0",
		ID:      id,
		Error:   map[string]any{"code": -32603, "message": msg},
	})
	if err != nil {
		// Only an unmarshalable id could get here, and JSONRPCID returns raw
		// JSON. Fall back to an unattributed error rather than a hole.
		return errorJSON(msg)
	}
	return json.RawMessage(b)
}

// errorJSON returns a minimal JSON error object as a raw message, the
// fallback for an item whose id could not be rendered.
func errorJSON(msg string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":    -32603,
			"message": msg,
		},
	})
	return json.RawMessage(b)
}
