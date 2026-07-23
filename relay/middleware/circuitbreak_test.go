package middleware

import (
	"testing"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

func TestCircuitBreak_BrokenDomainsFiltered(t *testing.T) {
	breaker := circuitbreaker.New()
	// Mark the domain of the first endpoint as broken.
	eps := testEndpoints(3)
	// eps[0] = "supplierA-https://nodeA.example.com"  → domain = "nodeA.example.com"
	brokenDomain := eps[0].Domain()
	breaker.MarkBroken("eth", brokenDomain, "test")

	var seenEndpoints []domain.EndpointAddrList
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		seenEndpoints = append(seenEndpoints, append(domain.EndpointAddrList{}, ctx.Endpoints...))
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), nil)
	h := mw(inner)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}

	if len(seenEndpoints) == 0 {
		t.Fatal("inner handler was not called")
	}
	for _, ep := range seenEndpoints[0] {
		if ep.Domain() == brokenDomain {
			t.Errorf("broken domain %q should have been filtered from ctx.Endpoints", brokenDomain)
		}
	}
}

func TestCircuitBreak_ShouldCircuitBreak_MarksDomainn(t *testing.T) {
	breaker := circuitbreaker.New()

	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			ShouldCircuitBreak: true,
			Reason:             "fabricated_response",
		}
		return retryableErr("bad supplier")
	})

	mw := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), nil)
	h := mw(inner)

	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx) // error expected

	// The domain of eps[0] should now be broken.
	if !breaker.IsBroken("eth", eps[0].Domain()) {
		t.Errorf("expected domain %q to be marked broken after ShouldCircuitBreak", eps[0].Domain())
	}
}

func TestCircuitBreak_ShouldRetryAlone_DoesNotCircuitBreak(t *testing.T) {
	breaker := circuitbreaker.New()

	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false, // explicitly not circuit-breaking
			Reason:             "rate_limit",
		}
		return retryableErr("rate limited")
	})

	mw := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), nil)
	h := mw(inner)

	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx)

	if breaker.IsBroken("eth", eps[0].Domain()) {
		t.Errorf("ShouldRetry alone must NOT trigger circuit break on domain %q", eps[0].Domain())
	}
}

func TestCircuitBreak_FlagDisabled_PassesThrough(t *testing.T) {
	breaker := circuitbreaker.New()
	// Mark all test endpoint domains as broken.
	eps := testEndpoints(3)
	for _, ep := range eps {
		breaker.MarkBroken("eth", ep.Domain(), "test")
	}

	var innerCalled bool
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		innerCalled = true
		// All endpoints should still be present (flag disabled = no filtering).
		if len(ctx.Endpoints) != len(eps) {
			t.Errorf("expected %d endpoints, got %d", len(eps), len(ctx.Endpoints))
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := CircuitBreak(breaker, nil, newFlags( /* no "circuit_breaker" flag */ ), nil)
	h := mw(inner)

	ctx := baseContext()
	ctx.Endpoints = eps
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if !innerCalled {
		t.Error("inner handler should have been called")
	}
}

func TestCircuitBreak_NoHeuristicResult_DoesNotCircuitBreak(t *testing.T) {
	breaker := circuitbreaker.New()
	eps := testEndpoints(1)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		// No "heuristic_result" key set.
		return retryableErr("transport error")
	})

	mw := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), nil)
	h := mw(inner)

	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx)

	if breaker.IsBroken("eth", eps[0].Domain()) {
		t.Error("missing heuristic result should not trigger circuit break")
	}
}

// --- break recording ---

type spyBreakRecorder struct {
	calls []struct {
		serviceID    domain.ServiceID
		brokenDomain string
	}
}

func (s *spyBreakRecorder) RecordCircuitBreak(serviceID domain.ServiceID, brokenDomain string) {
	s.calls = append(s.calls, struct {
		serviceID    domain.ServiceID
		brokenDomain string
	}{serviceID, brokenDomain})
}

// metrics.Recorder had a RecordCircuitBreak method and nothing called it, so
// circuit_breaks_total was a counter that could only ever read zero. The break
// happens here; this is where it has to be recorded.
func TestCircuitBreak_RecordsTheBreak(t *testing.T) {
	breaker := circuitbreaker.New()
	rec := &spyBreakRecorder{}

	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			ShouldCircuitBreak: true,
			Reason:             "fabricated_response",
		}
		return retryableErr("bad supplier")
	})

	h := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), rec)(inner)
	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx)

	if len(rec.calls) != 1 {
		t.Fatalf("RecordCircuitBreak called %d times, want 1", len(rec.calls))
	}
	if rec.calls[0].serviceID != "eth" {
		t.Errorf("serviceID = %q, want eth", rec.calls[0].serviceID)
	}
	if rec.calls[0].brokenDomain != eps[0].Domain() {
		t.Errorf("domain = %q, want %q", rec.calls[0].brokenDomain, eps[0].Domain())
	}
}

// ShouldRetry alone must never circuit-break, so it must never be counted as
// one either — the split is load-bearing (see the middleware doc).
func TestCircuitBreak_ShouldRetryAlone_RecordsNothing(t *testing.T) {
	breaker := circuitbreaker.New()
	rec := &spyBreakRecorder{}

	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
		}
		return retryableErr("transient")
	})

	h := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), rec)(inner)
	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx)

	if len(rec.calls) != 0 {
		t.Errorf("RecordCircuitBreak called %d times for a retry-only result, want 0", len(rec.calls))
	}
}

// The recorder is optional; a nil one must not panic the relay path.
func TestCircuitBreak_NilRecorderIsSafe(t *testing.T) {
	breaker := circuitbreaker.New()
	eps := testEndpoints(2)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = eps[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{ShouldCircuitBreak: true}
		return retryableErr("bad supplier")
	})

	h := CircuitBreak(breaker, nil, newFlags("circuit_breaker"), nil)(inner)
	ctx := baseContext()
	ctx.Endpoints = eps
	_ = h.HandleRelay(ctx)

	if !breaker.IsBroken("eth", eps[0].Domain()) {
		t.Error("the break must still happen without a recorder")
	}
}
