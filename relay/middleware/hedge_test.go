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
