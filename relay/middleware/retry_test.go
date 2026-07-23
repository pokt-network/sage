package middleware

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

func retryCfg(maxRetries int, maxLatency time.Duration) func(domain.ServiceID) config.RetryConfig {
	return func(_ domain.ServiceID) config.RetryConfig {
		return config.RetryConfig{MaxRetries: maxRetries, MaxLatency: maxLatency}
	}
}

func TestRetry_SucceedsOnFirstTry(t *testing.T) {
	handler := newMockHandler(nil)
	mw := Retry(newFlags("retry"), retryCfg(2, 0))
	h := mw(handler)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if handler.Count() != 1 {
		t.Errorf("expected 1 call, got %d", handler.Count())
	}
}

func TestRetry_SucceedsOnRetry(t *testing.T) {
	handler := newMockHandler(retryableErr("temporary"), nil)
	mw := Retry(newFlags("retry"), retryCfg(2, 0))
	h := mw(handler)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected success on second attempt, got %v", err)
	}
	if handler.Count() != 2 {
		t.Errorf("expected 2 calls, got %d", handler.Count())
	}
}

func TestRetry_ExhaustsRetries(t *testing.T) {
	handler := newMockHandler(
		retryableErr("fail1"),
		retryableErr("fail2"),
		retryableErr("fail3"),
	)
	mw := Retry(newFlags("retry"), retryCfg(2, 0))
	h := mw(handler)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	// MaxRetries=2 means 3 total attempts.
	if handler.Count() != 3 {
		t.Errorf("expected 3 calls (1 + 2 retries), got %d", handler.Count())
	}
}

func TestRetry_RespectsMaxLatencyBudget(t *testing.T) {
	slowHandler := &sleepyMockHandler{
		inner: newMockHandler(
			retryableErr("fail1"),
			retryableErr("fail2"),
			retryableErr("fail3"),
		),
		delay: 5 * time.Millisecond,
	}

	// MaxLatency = 1ms; the first attempt takes 5ms, so budget is immediately gone.
	mw := Retry(newFlags("retry"), retryCfg(5, 1*time.Millisecond))
	h := mw(slowHandler)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error when max latency budget exceeded")
	}
	// Budget exhausted after first attempt; at most 2 calls.
	if slowHandler.inner.Count() > 2 {
		t.Errorf("expected at most 2 calls before budget exhausted, got %d", slowHandler.inner.Count())
	}
}

func TestRetry_NonRetryableErrorStopsImmediately(t *testing.T) {
	handler := newMockHandler(
		nonRetryableErr("bad request"),
		nil, // would succeed if retried
	)
	mw := Retry(newFlags("retry"), retryCfg(3, 0))
	h := mw(handler)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected non-retryable error to be returned")
	}
	if handler.Count() != 1 {
		t.Errorf("expected exactly 1 call for non-retryable error, got %d", handler.Count())
	}
}

func TestRetry_ExcludesTriedEndpoints(t *testing.T) {
	th := &trackingMockHandler{
		responses: []error{retryableErr("fail1"), retryableErr("fail2"), nil},
	}

	mw := Retry(newFlags("retry"), retryCfg(3, 0))
	h := mw(th)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	// All three calls should have used different endpoints.
	seen := make(map[domain.EndpointAddr]bool)
	for _, ep := range th.seenEndpoints {
		if seen[ep] {
			t.Errorf("endpoint %q was used more than once", ep)
		}
		seen[ep] = true
	}
}

func TestRetry_FlagDisabled_PassesThrough(t *testing.T) {
	handler := newMockHandler(nil)
	mw := Retry(newFlags( /* no "retry" flag */ ), retryCfg(3, 0))
	h := mw(handler)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if handler.Count() != 1 {
		t.Errorf("expected 1 call even with flag disabled, got %d", handler.Count())
	}
}

func TestRetry_MaxRetriesZero_PassesThrough(t *testing.T) {
	handler := newMockHandler(retryableErr("fail"))
	mw := Retry(newFlags("retry"), retryCfg(0, 0))
	h := mw(handler)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error to propagate when MaxRetries==0")
	}
	if handler.Count() != 1 {
		t.Errorf("expected exactly 1 call, got %d", handler.Count())
	}
}

// ---------------------------------------------------------------------------
// Helpers used only in retry tests
// ---------------------------------------------------------------------------

// sleepyMockHandler wraps a mockHandler and inserts a delay before each call.
type sleepyMockHandler struct {
	inner *mockHandler
	delay time.Duration
}

func (s *sleepyMockHandler) HandleRelay(ctx *relay.Context) error {
	time.Sleep(s.delay)
	return s.inner.HandleRelay(ctx)
}

// trackingMockHandler records the endpoint used on each call.
type trackingMockHandler struct {
	responses     []error
	seenEndpoints []domain.EndpointAddr
	callIdx       int
}

func (t *trackingMockHandler) HandleRelay(ctx *relay.Context) error {
	if ctx.Endpoint == "" && len(ctx.Endpoints) > 0 {
		ctx.Endpoint = ctx.Endpoints[0]
	}
	t.seenEndpoints = append(t.seenEndpoints, ctx.Endpoint)

	var err error
	if t.callIdx < len(t.responses) {
		err = t.responses[t.callIdx]
	}
	t.callIdx++
	if err == nil {
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
	}
	return err
}
