package middleware

import (
	"context"
	"sync"
	"sync/atomic"
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

// TestRetry_SkipsAttemptWithNoBudgetLeft: attempts share the request deadline
// (timeout sits outside retry). A retry started with a sliver of that budget
// left cannot succeed and, when the deadline fires on it, is graded
// transport_timeout against an endpoint that had no real chance — a major
// penalty and a method mark for time a different host consumed. Retry must not
// start an attempt the budget cannot cover.
func TestRetry_SkipsAttemptWithNoBudgetLeft(t *testing.T) {
	budget := 200 * time.Millisecond
	failErr := retryableErr("slow 502")
	var calls int32
	slowFail := relay.HandlerFunc(func(_ *relay.Context) error {
		atomic.AddInt32(&calls, 1)
		time.Sleep(170 * time.Millisecond) // eats 85% of the budget, then fails retryably
		return failErr
	})

	deadlineCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	ctx := baseContext()
	ctx.Ctx = deadlineCtx

	err := Retry(newFlags("retry"), retryCfg(2, 0))(slowFail).HandleRelay(ctx)
	if err != failErr {
		t.Fatalf("expected the last attempt's own error, got %v", err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("expected no retry with 15%% of the budget left, got %d attempts", n)
	}
}

// The mirror: with most of the budget left a retry still happens.
func TestRetry_RetriesWhenBudgetRemains(t *testing.T) {
	failErr := retryableErr("fast 502")
	var calls int32
	fastFail := relay.HandlerFunc(func(_ *relay.Context) error {
		atomic.AddInt32(&calls, 1)
		return failErr
	})

	deadlineCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx := baseContext()
	ctx.Ctx = deadlineCtx

	_ = Retry(newFlags("retry"), retryCfg(2, 0))(fastFail).HandleRelay(ctx)
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("expected 3 attempts with budget to spare, got %d", n)
	}
}

// A session rollover between selection and send leaves the whole endpoint
// list pointing at the old session. Retrying its OTHER members is futile —
// they are stale too. The stale signal must refetch: clear ctx.Endpoints so
// the inner chain reselects from the fresh session.
func TestRetry_StaleEndpointsForcesRefetch(t *testing.T) {
	th := &staleThenOKHandler{}
	mw := Retry(newFlags("retry"), retryCfg(2, 0))
	h := mw(th)

	ctx := baseContext()
	ctx.Endpoints = domain.EndpointAddrList{"pokt1old-https://old.example.com", "pokt1old2-https://old2.example.com"}
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("stale-session error should recover after refetch, got %v", err)
	}
	if !th.refetched {
		t.Error("inner chain never saw a cleared endpoint list — the stale list was reused")
	}
}

// staleThenOKHandler fails the first attempt with a stale-endpoints error, and
// on the next attempt expects ctx.Endpoints to have been cleared (so the real
// SelectEndpoint would refetch); it then stands in for that refetch.
type staleThenOKHandler struct {
	calls     int
	refetched bool
}

func (h *staleThenOKHandler) HandleRelay(ctx *relay.Context) error {
	h.calls++
	if h.calls == 1 {
		ctx.Endpoint = ctx.Endpoints[0]
		return domain.NewRelayError(domain.ErrEndpoint, "relay send failed",
			domain.NewRelayError(domain.ErrTransport, "endpoint not in session", domain.ErrEndpointsStale, true), true)
	}
	if len(ctx.Endpoints) == 0 {
		h.refetched = true
		ctx.Endpoints = domain.EndpointAddrList{"pokt1fresh-https://fresh.example.com"}
	}
	ctx.Endpoint = ctx.Endpoints[0]
	return nil
}

type recordingRetryRec struct {
	reasons     []string
	resolutions []string // "reason/outcome"
}

func (r *recordingRetryRec) RecordRetry(_ domain.ServiceID, reason string) {
	r.reasons = append(r.reasons, reason)
}

func (r *recordingRetryRec) RecordRetryResolution(_ domain.ServiceID, reason, outcome string) {
	r.resolutions = append(r.resolutions, reason+"/"+outcome)
}

// A retried request records exactly one resolution, naming the verdict that
// caused the retry and whether a later attempt succeeded. This is the counter
// that answers "is retrying this failure worth anything" without needing a
// matched window or a baseline band — the thing three contradictory readings
// of shentu's 408 share cost an afternoon on 2026-09-05.
func TestRetry_RecordsOneResolutionPerRetriedRequest(t *testing.T) {
	rec := &recordingRetryRec{}
	th := &trackingMockHandler{responses: []error{retryableErr("fail1"), retryableErr("fail2"), nil}}
	mw := RetryWithRecorder(newFlags("retry"), retryCfg(3, 0), rec)
	if err := mw(th).HandleRelay(baseContext()); err != nil {
		t.Fatal(err)
	}
	if len(rec.resolutions) != 1 || rec.resolutions[0] != "transport error/recovered" {
		t.Fatalf("want one transport-error/recovered, got %v", rec.resolutions)
	}
}

// Retries that never succeed are the other half: without them "recovered" is
// a count with no denominator and cannot be read as a rate.
func TestRetry_RecordsExhaustedWhenNoAttemptSucceeds(t *testing.T) {
	rec := &recordingRetryRec{}
	th := &trackingMockHandler{responses: []error{retryableErr("f1"), retryableErr("f2"), retryableErr("f3")}}
	mw := RetryWithRecorder(newFlags("retry"), retryCfg(2, 0), rec)
	if err := mw(th).HandleRelay(baseContext()); err == nil {
		t.Fatal("want the last error")
	}
	if len(rec.resolutions) != 1 || rec.resolutions[0] != "transport error/exhausted" {
		t.Fatalf("want one transport-error/exhausted, got %v", rec.resolutions)
	}
}

// The no-retry path is nearly every request. It must not touch the counter,
// or "recovered" over total would be diluted by requests that never retried.
func TestRetry_FirstAttemptSuccessRecordsNoResolution(t *testing.T) {
	rec := &recordingRetryRec{}
	th := &trackingMockHandler{responses: []error{nil}}
	mw := RetryWithRecorder(newFlags("retry"), retryCfg(3, 0), rec)
	if err := mw(th).HandleRelay(baseContext()); err != nil {
		t.Fatal(err)
	}
	if len(rec.resolutions) != 0 {
		t.Fatalf("a request that never retried recorded %v", rec.resolutions)
	}
}

// The label is the heuristic's own verdict when the attempt produced one, so
// "did retrying a 408 help" is one selector. Without this it is the error
// KIND, which lumps a 408 in with every other transport failure.
func TestRetry_ResolutionLabelIsTheHeuristicVerdict(t *testing.T) {
	rec := &recordingRetryRec{}
	th := &trackingMockHandler{responses: []error{retryableErr("supplier 408"), nil}}
	mw := RetryWithRecorder(newFlags("retry"), retryCfg(2, 0), rec)
	ctx := baseContext()
	ctx.HeuristicResult = &heuristic.AnalysisResult{Reason: "http_408"}
	if err := mw(th).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rec.resolutions) != 1 || rec.resolutions[0] != "http_408/recovered" {
		t.Fatalf("want http_408/recovered, got %v", rec.resolutions)
	}
}

// Every retry attempt increments sage_retry_total with a reason. The metric
// was defined and documented but never emitted — no series in Prometheus.
func TestRetry_RecordsMetricPerRetry(t *testing.T) {
	rec := &recordingRetryRec{}
	th := &trackingMockHandler{responses: []error{retryableErr("fail1"), retryableErr("fail2"), nil}}
	mw := RetryWithRecorder(newFlags("retry"), retryCfg(3, 0), rec)
	if err := mw(th).HandleRelay(baseContext()); err != nil {
		t.Fatal(err)
	}
	if len(rec.reasons) != 2 {
		t.Fatalf("expected 2 retry records (2 retries before success), got %d: %v", len(rec.reasons), rec.reasons)
	}
}

// A rollover retry is labelled so it is distinguishable from an ordinary one.
func TestRetry_RolloverReasonLabel(t *testing.T) {
	rec := &recordingRetryRec{}
	th := &staleThenOKHandler{}
	mw := RetryWithRecorder(newFlags("retry"), retryCfg(2, 0), rec)
	ctx := baseContext()
	ctx.Endpoints = domain.EndpointAddrList{"pokt1old-https://old.example.com"}
	if err := mw(th).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rec.reasons) != 1 || rec.reasons[0] != "rollover" {
		t.Fatalf("expected one 'rollover' reason, got %v", rec.reasons)
	}
}

// blackholeThenOK hangs the first attempt until its context deadline (a
// supplier that accepts the connection but never responds), then succeeds.
type blackholeThenOK struct {
	calls        int
	firstBudget  time.Duration
	secondBudget time.Duration
}

func (h *blackholeThenOK) HandleRelay(ctx *relay.Context) error {
	h.calls++
	if dl, ok := ctx.Ctx.Deadline(); ok {
		if h.calls == 1 {
			h.firstBudget = time.Until(dl)
		} else {
			h.secondBudget = time.Until(dl)
		}
	}
	if h.calls == 1 {
		<-ctx.Ctx.Done() // blackhole: hang until this attempt's deadline
		return retryableErr("attempt deadline exceeded")
	}
	ctx.Endpoint = ctx.Endpoints[0]
	ctx.Response = &domain.Response{HTTPStatusCode: 200}
	return nil
}

// A blackholing supplier must not consume the whole request deadline. Each
// attempt gets a share of the remaining budget, so a hang on the first leaves
// room to retry a healthy supplier — the mainnet robinhood 502s were all
// "awaiting headers" timeouts that ate the full 5s and starved the retry.
func TestRetry_PerAttemptTimeoutLeavesRoomToRetry(t *testing.T) {
	h := &blackholeThenOK{}
	mw := Retry(newFlags("retry"), retryCfg(1, 0)) // 2 attempts
	deadline := 400 * time.Millisecond
	ctx := baseContext()
	c, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	ctx.Ctx = c

	start := time.Now()
	err := h2(mw, h).HandleRelay(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected recovery on the second attempt, got %v", err)
	}
	if h.calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", h.calls)
	}
	if h.firstBudget >= deadline-50*time.Millisecond {
		t.Errorf("first attempt got the full budget %v (want a narrowed share of %v)", h.firstBudget, deadline)
	}
	if elapsed >= deadline+100*time.Millisecond {
		t.Errorf("took %v, expected to finish within the request deadline %v", elapsed, deadline)
	}
}

// h2 wires a middleware around a plain handler for the test.
func h2(mw relay.Middleware, h relay.Handler) relay.Handler { return mw(h) }

// End to end: retry wrapping hedge. On the first attempt both hedge arms
// blackhole (hang past the per-attempt cap); retry must then run a second
// attempt that succeeds. This is the path that was inert on prod traffic —
// hedge returned a non-retryable deadline, so the cap failed fast but never
// recovered.
func TestRetryOverHedge_RecoversAfterBlackhole(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n <= 2 {
			// First retry attempt = a hedge race of two arms; both hang.
			<-ctx.Ctx.Done()
			return retryableErr("blackhole")
		}
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	flags := newFlags("retry", "hedge")
	chain := Retry(flags, retryCfg(1, 0))(Hedge(flags, hedgeCfg(2*time.Millisecond))(inner))

	ctx := baseContext()
	c, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ctx.Ctx = c

	err := chain.HandleRelay(ctx)
	if err != nil {
		t.Fatalf("expected recovery on the retry after a blackholed hedge, got %v", err)
	}
}
