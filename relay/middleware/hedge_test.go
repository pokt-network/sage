package middleware

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/relay"
)

func hedgeCfg(delay time.Duration) func(domain.ServiceID) config.RetryConfig {
	return func(_ domain.ServiceID) config.RetryConfig {
		return config.RetryConfig{HedgeDelay: delay}
	}
}

func TestHedge_PrimaryWinsBeforeDelay(t *testing.T) {
	// Primary completes instantly — hedge should never be launched.
	var calls int32
	fast := relay.HandlerFunc(func(ctx *relay.Context) error {
		atomic.AddInt32(&calls, 1)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(50*time.Millisecond))
	h := mw(fast)

	ctx := baseContext()
	start := time.Now()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	// Should return well before the hedge delay.
	if elapsed := time.Since(start); elapsed >= 40*time.Millisecond {
		t.Errorf("expected fast return, took %v", elapsed)
	}
	if ctx.Response == nil {
		t.Error("expected response to be set")
	}
}

func TestHedge_HedgeWinsAfterDelay(t *testing.T) {
	// Primary is slow (100ms), hedge fires at 20ms and completes quickly.
	var callCount int32

	slow := relay.HandlerFunc(func(ctx *relay.Context) error {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			// Primary: slow
			time.Sleep(100 * time.Millisecond)
		}
		// Hedge (n==2): fast
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(20*time.Millisecond))
	h := mw(slow)

	ctx := baseContext()
	start := time.Now()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected hedge to win, got %v", err)
	}

	elapsed := time.Since(start)
	// Should complete well before the primary's 100ms.
	if elapsed >= 80*time.Millisecond {
		t.Errorf("expected hedge to win quickly, took %v", elapsed)
	}
	if atomic.LoadInt32(&callCount) < 2 {
		t.Error("expected both primary and hedge to have been called")
	}
}

func TestHedge_BothFail_ReturnsPrimaryError(t *testing.T) {
	primaryErr := retryableErr("primary failed")
	hedgeErr := retryableErr("hedge failed")

	var callCount int32
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			// Primary: fail after a short pause
			time.Sleep(5 * time.Millisecond)
			return primaryErr
		}
		// Hedge: fail immediately
		return hedgeErr
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(2*time.Millisecond))
	h := mw(handler)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error when both arms fail")
	}
	if err != primaryErr {
		t.Errorf("expected primary error %v, got %v", primaryErr, err)
	}
}

// TestHedge_LoserContextDetachedFromCaller verifies that the losing arm runs on
// a context detached from the caller's request context: cancelling the caller
// after the winner returns must NOT cancel the loser's in-flight (signed) relay.
// Without detachment the loser would be torn down (TCP RST to the supplier).
func TestHedge_LoserContextDetachedFromCaller(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var loserCtx context.Context
	release := make(chan struct{})
	var n int32

	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		if atomic.AddInt32(&n, 1) == 1 {
			// Primary = loser: capture its context, then block until released.
			mu.Lock()
			loserCtx = ctx.Ctx
			mu.Unlock()
			<-release
			ctx.Response = &domain.Response{HTTPStatusCode: 200}
			return nil
		}
		// Hedge = winner: succeed immediately.
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(10*time.Millisecond))
	h := mw(handler)

	ctx := baseContext()
	ctx.Ctx = parent
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected hedge to win, got %v", err)
	}

	// Caller's request ends — cancel the parent context.
	cancel()
	time.Sleep(20 * time.Millisecond) // allow any (erroneous) propagation

	mu.Lock()
	lc := loserCtx
	mu.Unlock()
	if lc == nil {
		t.Fatal("primary (loser) arm never ran")
	}
	if lc.Err() != nil {
		t.Fatalf("loser context was cancelled by caller (got %v) — drain not applied", lc.Err())
	}
	close(release) // let the loser finish cleanly
}

func TestHedge_FlagDisabled_PassesThrough(t *testing.T) {
	var calls int32
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		atomic.AddInt32(&calls, 1)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Hedge(newFlags( /* no "hedge" flag */ ), hedgeCfg(5*time.Millisecond))
	h := mw(handler)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected exactly 1 call with flag disabled, got %d", calls)
	}
}

func TestHedge_HedgeDelayZero_PassesThrough(t *testing.T) {
	var calls int32
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		atomic.AddInt32(&calls, 1)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(0))
	h := mw(handler)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected exactly 1 call when HedgeDelay==0, got %d", calls)
	}
}

// A hedge is a second, independent attempt. Two hostnames run by the same
// provider are not independent, so the hedge arm should prefer a different
// operator, not merely a different endpoint.
func TestHedge_PrefersADifferentOperator(t *testing.T) {
	var mu sync.Mutex
	var picked []domain.EndpointAddr

	// Stands in for the inner chain's SelectEndpoint: pick, then publish the
	// pick so Hedge can read it without racing the field.
	slow := relay.HandlerFunc(func(ctx *relay.Context) error {
		ep := ctx.Endpoints[0]
		ctx.Endpoint = ep
		if ctx.SelectedEndpoint != nil {
			ctx.SelectedEndpoint.Store(&ep)
		}
		mu.Lock()
		picked = append(picked, ep)
		mu.Unlock()
		time.Sleep(60 * time.Millisecond)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	h := Hedge(newFlags("hedge", "operator_aware_selection"), hedgeCfg(10*time.Millisecond))(slow)

	ctx := baseContext()
	ctx.Endpoints = multiOperatorEndpoints()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(picked) != 2 {
		t.Fatalf("expected primary + hedge, got %d picks: %v", len(picked), picked)
	}
	if picked[0].Operator() == picked[1].Operator() {
		t.Errorf("hedge landed on the primary's operator %q: %v", picked[0].Operator(), picked)
	}
}

// With only one operator available the hedge still runs — on a different
// endpoint, as before. The preference must never cost us the hedge.
func TestHedge_SingleOperatorPoolStillHedges(t *testing.T) {
	var mu sync.Mutex
	var picked []domain.EndpointAddr

	// Stands in for the inner chain's SelectEndpoint: pick, then publish the
	// pick so Hedge can read it without racing the field.
	slow := relay.HandlerFunc(func(ctx *relay.Context) error {
		ep := ctx.Endpoints[0]
		ctx.Endpoint = ep
		if ctx.SelectedEndpoint != nil {
			ctx.SelectedEndpoint.Store(&ep)
		}
		mu.Lock()
		picked = append(picked, ep)
		mu.Unlock()
		time.Sleep(60 * time.Millisecond)
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	h := Hedge(newFlags("hedge", "operator_aware_selection"), hedgeCfg(10*time.Millisecond))(slow)

	ctx := baseContext() // one operator, three hostnames
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(picked) != 2 {
		t.Fatalf("expected primary + hedge, got %d picks: %v", len(picked), picked)
	}
	if picked[0] == picked[1] {
		t.Errorf("hedge reused the primary's endpoint: %v", picked)
	}
}

// A panic on a hedge arm runs on a detached goroutine, where net/http's
// recovery does not reach. Before safego this crashed the process — the same
// panic on the same request cost only a 500 when hedging happened not to fire.
//
// Recovering is only half of it: the arm must still deliver a result, or the
// select waiting on its channel blocks until the request's context expires.
// That is why this asserts on the returned error and on elapsed time, not just
// on the test surviving.
func TestHedge_PanicOnPrimaryDoesNotCrashOrHang(t *testing.T) {
	panicking := relay.HandlerFunc(func(ctx *relay.Context) error {
		panic("supplier response parser hit a nil map")
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(20*time.Millisecond))
	h := mw(panicking)

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- h.HandleRelay(baseContext()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a panicking chain reported success")
		}
		if !errors.Is(err, safego.ErrPanic) {
			t.Errorf("error %v does not wrap safego.ErrPanic", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hedge never returned — the arm recovered without delivering a result")
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v; a recovered arm should resolve the race immediately", elapsed)
	}
}

// The hedge arm panicking must not sink the request either: the primary is
// healthy, so the race still has a winner.
func TestHedge_PanicOnHedgeArmStillLetsPrimaryWin(t *testing.T) {
	var calls int32
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		if atomic.AddInt32(&calls, 1) == 2 {
			panic("hedge arm exploded")
		}
		time.Sleep(60 * time.Millisecond)
		ctx.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"ok":true}`)}
		return nil
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(10*time.Millisecond))
	ctx := baseContext()

	if err := mw(handler).HandleRelay(ctx); err != nil {
		t.Fatalf("primary should have won, got %v", err)
	}
	if ctx.Response == nil {
		t.Error("no response — the panicking arm took the healthy one with it")
	}
}

// TestHedge_BothFail_MergesPrimaryContext: when both arms fail, the primary
// arm's outcome must be merged back like a win is. Retry sits outside Hedge
// and excludes ctx.Endpoint on the next attempt; with nothing merged it
// excludes "" and can draw the same dead endpoint again.
func TestHedge_BothFail_MergesPrimaryContext(t *testing.T) {
	var callCount int32
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			ctx.Endpoint = "primary-ep"
			time.Sleep(5 * time.Millisecond)
			return retryableErr("primary failed")
		}
		ctx.Endpoint = "hedge-ep"
		return retryableErr("hedge failed")
	})

	mw := Hedge(newFlags("hedge"), hedgeCfg(2*time.Millisecond))
	ctx := baseContext()
	if err := mw(handler).HandleRelay(ctx); err == nil {
		t.Fatal("expected error when both arms fail")
	}
	if ctx.Endpoint != "primary-ep" {
		t.Fatalf("both-fail must merge the primary arm's endpoint, got %q", ctx.Endpoint)
	}
}

// TestRetry_AfterHedgeBothFail_ExcludesFailedEndpoint is the chain-level
// version: Retry(Hedge(select-first)) must not re-select the endpoint the
// primary arm just failed on.
func TestRetry_AfterHedgeBothFail_ExcludesFailedEndpoint(t *testing.T) {
	var mu sync.Mutex
	var picked []domain.EndpointAddr
	selectFirst := relay.HandlerFunc(func(ctx *relay.Context) error {
		if len(ctx.Endpoints) == 0 {
			return nonRetryableErr("no endpoints")
		}
		ctx.Endpoint = ctx.Endpoints[0]
		mu.Lock()
		picked = append(picked, ctx.Endpoint)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return retryableErr("down")
	})

	chain := Retry(newFlags("retry", "hedge"), retryCfg(1, 0))(
		Hedge(newFlags("retry", "hedge"), hedgeCfg(1*time.Millisecond))(selectFirst))

	ctx := baseContext()
	if err := chain.HandleRelay(ctx); err == nil {
		t.Fatal("expected the chain to fail")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(picked) < 3 {
		t.Fatalf("expected primary, hedge and a retry, got picks %v", picked)
	}
	first := picked[0]
	for _, ep := range picked[2:] {
		if ep == first {
			t.Fatalf("retry re-selected %q, which the primary arm already failed on; picks: %v", first, picked)
		}
	}
}

// TestHedge_ReturnsWhenRequestContextDone: the arms are detached on purpose
// (a losing arm must flush its signed relay), but the WAIT must not be. With
// the request deadline gone nobody is listening; Hedge must return so the
// timeout middleware can answer, instead of holding the client until the
// protocol's HTTP client gives up.
func TestHedge_ReturnsWhenRequestContextDone(t *testing.T) {
	// Both arms run (the hedge fires at 5ms, well inside the 30ms deadline)
	// and both outlive the test body; wait for them so nothing leaks.
	var wg sync.WaitGroup
	wg.Add(2)
	slow := relay.HandlerFunc(func(_ *relay.Context) error {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		return retryableErr("too late")
	})
	defer wg.Wait()

	chain := Timeout(func(domain.ServiceID) time.Duration { return 30 * time.Millisecond })(
		Hedge(newFlags("hedge"), hedgeCfg(5*time.Millisecond))(slow))

	ctx := baseContext()
	start := time.Now()
	err := chain.HandleRelay(ctx)
	elapsed := time.Since(start)

	if elapsed > 150*time.Millisecond {
		t.Fatalf("hedge held the request for %v past a 30ms deadline", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a deadline error, got %v", err)
	}
}

type recordingHedgeRec struct{ results []string }

func (r *recordingHedgeRec) RecordHedge(_ domain.ServiceID, result string) {
	r.results = append(r.results, result)
}

func TestHedge_RecordsOutcome(t *testing.T) {
	rec := &recordingHedgeRec{}
	// Primary wins before the delay: one primary_won.
	handler := newMockHandler(nil)
	mw := HedgeWithRecorder(newFlags("hedge"), hedgeCfg(50*time.Millisecond), rec)
	if err := mw(handler).HandleRelay(baseContext()); err != nil {
		t.Fatal(err)
	}
	if len(rec.results) != 1 || rec.results[0] != "primary_won" {
		t.Fatalf("got %v, want one primary_won", rec.results)
	}
}

// When the hedge race ends because the (per-attempt) deadline fired, it must
// return a RETRYABLE error so the retry middleware outside it can try another
// supplier with the remaining budget. A raw context.DeadlineExceeded is not
// retryable, so a per-attempt cap on a hedged relay failed fast but never
// recovered — the retry-recovery half was inert on prod traffic.
func TestHedge_DeadlineIsRetryable(t *testing.T) {
	// Both arms hang past the deadline.
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		<-ctx.Ctx.Done()
		return retryableErr("arm hung")
	})
	mw := Hedge(newFlags("hedge", "operator_aware_selection"), hedgeCfg(5*time.Millisecond))
	ctx := baseContext()
	c, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	ctx.Ctx = c

	err := mw(handler).HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if !domain.IsRetryable(err) {
		t.Fatalf("hedge deadline error must be retryable so retry can try another supplier, got %v (retryable=%v)", err, domain.IsRetryable(err))
	}
}

// A client hang-up (context.Canceled) is NOT retryable — nobody is waiting.
func TestHedge_CancelIsNotRetryable(t *testing.T) {
	handler := relay.HandlerFunc(func(ctx *relay.Context) error {
		<-ctx.Ctx.Done()
		return retryableErr("arm hung")
	})
	mw := Hedge(newFlags("hedge"), hedgeCfg(5*time.Millisecond))
	ctx := baseContext()
	c, cancel := context.WithCancel(context.Background())
	ctx.Ctx = c
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	err := mw(handler).HandleRelay(ctx)
	if domain.IsRetryable(err) {
		t.Fatalf("a client cancel must not be retryable, got retryable %v", err)
	}
}

// selectingHandler stands in for the inner chain: SelectEndpoint fills
// ctx.Endpoints and ctx.Endpoint on whatever context reaches it, then the
// relay fails retryably.
type selectingHandler struct{ calls int }

func (h *selectingHandler) HandleRelay(ctx *relay.Context) error {
	h.calls++
	if len(ctx.Endpoints) == 0 {
		ctx.Endpoints = testEndpoints(3)
	}
	if ctx.Endpoint == "" {
		ctx.Endpoint = ctx.Endpoints[0]
	}
	if ctx.SelectedEndpoint != nil {
		ep := ctx.Endpoint
		ctx.SelectedEndpoint.Store(&ep)
	}
	return retryableErr("fail")
}

// Retry must still retry when hedging is on, and for six months it did not.
//
// SelectEndpoint runs INSIDE the race, so it fills Endpoints on an arm's
// clone; Clone is a value copy, so the parent context Retry holds keeps the
// empty list it started with. Retry derives its retry pool from that, excludes
// the endpoint it just tried, gets an empty candidate list and breaks out of
// its loop having made exactly one attempt. Every hedged service therefore had
// retry silently disabled, which no counter showed until
// sage_retry_resolution_total went to zero series on 2026-09-06 — it records
// only retries that actually run.
//
// The assertion is on ATTEMPT COUNT and must match the unhedged case: a merge
// that copied the wrong field, or none, leaves this at one.
func TestHedge_MergesTheCandidatePoolSoRetryCanRetry(t *testing.T) {
	run := func(hedgeOn bool) int {
		h := &selectingHandler{}
		names := []string{"retry"}
		inner := relay.Handler(h)
		if hedgeOn {
			names = append(names, "hedge")
			inner = Hedge(newFlags(names...), hedgeCfg(5*time.Millisecond))(h)
		}
		ctx := &relay.Context{}
		ctx.Ctx = t.Context()
		ctx.ServiceID = "eth"
		ctx.Endpoints = domain.EndpointAddrList{}
		_ = RetryWithRecorder(newFlags(names...), retryCfg(3, 0), nil)(inner).HandleRelay(ctx)
		return h.calls
	}
	off, on := run(false), run(true)
	if off != 3 {
		t.Fatalf("unhedged: want 3 attempts (1 + 2 retries), got %d", off)
	}
	if on != off {
		t.Fatalf("hedging disabled retry: %d attempts hedged vs %d unhedged", on, off)
	}
}

// TestHedge_SuppressedForLargeBatchItems: an item of a batch over
// hedge_max_batch_size runs once, unhedged, and is counted. Every item is its
// own race, so a size-blind hedge doubles a large batch's relays in flight.
func TestHedge_SuppressedForLargeBatchItems(t *testing.T) {
	cases := []struct {
		name      string
		limit     int
		batchSize int
		wantArms  int
		wantRec   string
	}{
		{"over the default cap", 0, 11, 1, "suppressed_large_batch"},
		{"at the default cap", 0, 10, 2, "hedge_won"},
		{"single request", 0, 0, 2, "hedge_won"},
		{"over an explicit cap", 3, 4, 1, "suppressed_large_batch"},
		{"negative disables the cap", -1, 5000, 2, "hedge_won"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var arms atomic.Int32
			// The primary outlives the race when a hedge wins; wait for it so
			// no arm reads the subtest's closure after the subtest returns.
			primaryDone := make(chan struct{})
			slow := relay.HandlerFunc(func(ctx *relay.Context) error {
				if arms.Add(1) == 1 {
					defer close(primaryDone)
					// The primary runs past the delay so a hedge, if any, fires
					// and wins.
					time.Sleep(30 * time.Millisecond)
				}
				ctx.Response = &domain.Response{HTTPStatusCode: 200}
				return nil
			})
			rec := &recordingHedgeRec{}
			cfgFn := func(domain.ServiceID) config.RetryConfig {
				return config.RetryConfig{HedgeDelay: 5 * time.Millisecond, HedgeMaxBatchSize: tc.limit}
			}
			ctx := baseContext()
			ctx.BatchSize = tc.batchSize
			if err := HedgeWithRecorder(newFlags("hedge"), cfgFn, rec)(slow).HandleRelay(ctx); err != nil {
				t.Fatal(err)
			}
			<-primaryDone
			if got := int(arms.Load()); got != tc.wantArms {
				t.Fatalf("arms = %d, want %d", got, tc.wantArms)
			}
			if len(rec.results) != 1 || rec.results[0] != tc.wantRec {
				t.Fatalf("recorded %v, want one %s", rec.results, tc.wantRec)
			}
		})
	}
}
