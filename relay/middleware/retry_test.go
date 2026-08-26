package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
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

// multiOperatorEndpoints returns a pool spanning two operators: two endpoints
// behind alpha.net and one behind beta.net. Note that the default testEndpoints
// pool is nodeA/nodeB/nodeC.example.com — three domains but ONE operator, which
// is exactly the shape operator-awareness has to leave untouched.
func multiOperatorEndpoints() domain.EndpointAddrList {
	return domain.EndpointAddrList{
		"supplier1-https://a1.alpha.net",
		"supplier2-https://a2.alpha.net",
		"supplier3-https://b1.beta.net",
	}
}

// firstEndpointHandler always picks the head of the candidate list and fails,
// so the test observes exactly which pool Retry handed each attempt.
func firstEndpointHandler(seen *[]domain.EndpointAddr) relay.Handler {
	return relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = ctx.Endpoints[0]
		*seen = append(*seen, ctx.Endpoint)
		return retryableErr("boom")
	})
}

// A retry exists to reach different infrastructure. Avoiding only the failed
// endpoint can put the retry on another hostname of the same operator — same
// rack, same upstream, same outage.
func TestRetry_PrefersADifferentOperator(t *testing.T) {
	var seen []domain.EndpointAddr
	h := Retry(newFlags("retry", "operator_aware_selection"), retryCfg(2, 0))(firstEndpointHandler(&seen))

	ctx := baseContext()
	ctx.Endpoints = multiOperatorEndpoints()
	_ = h.HandleRelay(ctx)

	if len(seen) != 3 {
		t.Fatalf("expected 3 attempts, got %d: %v", len(seen), seen)
	}
	if seen[1].Operator() == seen[0].Operator() {
		t.Errorf("retry stayed on operator %q: %v", seen[0].Operator(), seen)
	}
	// The third attempt has no untried operator left, so it falls back to the
	// remaining endpoint rather than giving up — and it must still be one the
	// earlier attempts did not use.
	if seen[2] == seen[0] || seen[2] == seen[1] {
		t.Errorf("third attempt reused an already-tried endpoint: %v", seen)
	}
}

// Operator preference narrows one attempt's pool, not the pool itself. If the
// narrowing compounded, the last attempt would find nothing left to try.
func TestRetry_OperatorPreferenceDoesNotStrandLaterAttempts(t *testing.T) {
	var seen []domain.EndpointAddr
	h := Retry(newFlags("retry", "operator_aware_selection"), retryCfg(2, 0))(firstEndpointHandler(&seen))

	ctx := baseContext()
	ctx.Endpoints = multiOperatorEndpoints()
	_ = h.HandleRelay(ctx)

	distinct := map[domain.EndpointAddr]bool{}
	for _, ep := range seen {
		distinct[ep] = true
	}
	if len(distinct) != 3 {
		t.Errorf("expected all 3 endpoints to be tried, got %v", seen)
	}
}

// Flag off restores per-endpoint-only exclusion.
func TestRetry_OperatorAwarenessIsFlagGated(t *testing.T) {
	var seen []domain.EndpointAddr
	h := Retry(newFlags("retry"), retryCfg(2, 0))(firstEndpointHandler(&seen))

	ctx := baseContext()
	ctx.Endpoints = multiOperatorEndpoints()
	_ = h.HandleRelay(ctx)

	if len(seen) != 3 {
		t.Fatalf("expected 3 attempts, got %d: %v", len(seen), seen)
	}
	if seen[1].Operator() != seen[0].Operator() {
		t.Errorf("with the flag off, retry should take the next endpoint in order: %v", seen)
	}
}

// A single-operator pool must retry exactly as it always did: the preference is
// a preference, and having nowhere else to go is not a reason to stop.
func TestRetry_SingleOperatorPoolStillRetries(t *testing.T) {
	var seen []domain.EndpointAddr
	h := Retry(newFlags("retry", "operator_aware_selection"), retryCfg(2, 0))(firstEndpointHandler(&seen))

	ctx := baseContext() // testEndpoints: three hostnames, one operator
	_ = h.HandleRelay(ctx)

	if len(seen) != 3 {
		t.Fatalf("expected 3 attempts on a single-operator pool, got %d: %v", len(seen), seen)
	}
	distinct := map[domain.EndpointAddr]bool{}
	for _, ep := range seen {
		distinct[ep] = true
	}
	if len(distinct) != 3 {
		t.Errorf("each attempt should use a different endpoint: %v", seen)
	}
}

// A retry is for the client's benefit, and once the request context is done
// there is no client. Every further attempt would select a fresh endpoint and
// sign a relay that fails on arrival with "context canceled" — charged to a
// supplier that did nothing. PATH's single-request loop already stopped here;
// its batch loop did not, and 43 of 43 circuit breaks on one service over two
// hours were client hang-ups stamped on whichever supplier held the item.
func TestRetry_StopsWhenRequestContextIsDone(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls int
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		calls++
		ctx.Endpoint = ctx.Endpoints[0]
		cancel() // the client hangs up while this attempt is in flight
		return retryableErr("context canceled")
	})
	h := Retry(newFlags("retry"), retryCfg(3, 0))(inner)

	ctx := baseContext()
	ctx.Ctx = goCtx
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected the attempt's error to be returned")
	}
	if calls != 1 {
		t.Fatalf("made %d attempts against a dead request context, want 1", calls)
	}
}

// A heuristic verdict belongs to the attempt that produced it. Attempt 1
// sets MethodBlocking and fails; attempt 2 succeeds without touching
// HeuristicResult at all (as happens when Heuristic's flag is off, or there
// is nothing to analyse). Without a reset, a downstream post-relay check
// (MethodBlocks) reading ctx.HeuristicResult after HandleRelay returns would
// see attempt 1's verdict and act on attempt 2's healthy endpoint.
func TestRetry_ResetsHeuristicResultPerAttempt(t *testing.T) {
	var calls int
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		calls++
		ctx.Endpoint = ctx.Endpoints[0]
		if calls == 1 {
			ctx.HeuristicResult = &heuristic.AnalysisResult{MethodBlocking: true}
			return retryableErr("timeout")
		}
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})
	h := Retry(newFlags("retry"), retryCfg(2, 0))(inner)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected success on second attempt, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", calls)
	}
	if ctx.HeuristicResult != nil {
		t.Fatalf("HeuristicResult leaked from attempt 1 into attempt 2: %+v", ctx.HeuristicResult)
	}
}
