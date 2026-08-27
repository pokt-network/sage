package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

func makeMultiPayloadCtx(payloads []domain.Payload) *relay.Context {
	req, _ := http.NewRequest(http.MethodPost, "/v1", nil)
	ctx := relay.NewContext(context.Background(), req, nil, nil)
	ctx.ServiceID = "eth"
	ctx.Payloads = payloads
	return ctx
}

func TestBatch_SinglePayload_PassThrough(t *testing.T) {
	var called atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Batch(4, 0, nil, nil)
	handler := mw(inner)

	p := domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	ctx := makeMultiPayloadCtx([]domain.Payload{p})

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called.Load() != 1 {
		t.Fatalf("expected inner called once, got %d", called.Load())
	}
	// Should not be wrapped in an array.
	if string(ctx.Response.Body) != `{"result":"ok"}` {
		t.Fatalf("single payload response should not be wrapped: %s", ctx.Response.Body)
	}
}

func TestBatch_MultiplePayloads_FanOut(t *testing.T) {
	var relayCount atomic.Int32

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		method := ctx.Payloads[0].Method()
		ctx.Response = &domain.Response{
			Body:           []byte(`{"method":"` + method + `"}`),
			HTTPStatusCode: 200,
		}
		return nil
	})

	mw := Batch(4, 0, nil, nil)
	handler := mw(inner)

	payloads := []domain.Payload{
		domain.NewPayload([]byte(`p1`), domain.RPCTypeJSONRPC, "m1"),
		domain.NewPayload([]byte(`p2`), domain.RPCTypeJSONRPC, "m2"),
		domain.NewPayload([]byte(`p3`), domain.RPCTypeJSONRPC, "m3"),
	}

	ctx := makeMultiPayloadCtx(payloads)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if relayCount.Load() != 3 {
		t.Fatalf("expected 3 individual relays, got %d", relayCount.Load())
	}
	if ctx.Response == nil {
		t.Fatal("expected combined response")
	}
	if ctx.Response.HTTPStatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", ctx.Response.HTTPStatusCode)
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &arr); err != nil {
		t.Fatalf("combined response should be JSON array: %v\nbody: %s", err, ctx.Response.Body)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements in combined array, got %d", len(arr))
	}
}

func TestBatch_PartialFailure_IncludedInResult(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if ctx.Payloads[0].Method() == "fail" {
			return domain.NewRelayError(domain.ErrEndpoint, "no endpoint answered", errors.New("dial tcp 10.0.0.1:8545: connection refused"), true)
		}
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":` + string(ctx.Payloads[0].JSONRPCID()) + `,"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Batch(4, 0, nil, nil)
	handler := mw(inner)

	payloads := []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"ok"}`), domain.RPCTypeJSONRPC, "ok"),
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":"req-2","method":"fail"}`), domain.RPCTypeJSONRPC, "fail"),
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":3,"method":"ok"}`), domain.RPCTypeJSONRPC, "ok"),
	}

	ctx := makeMultiPayloadCtx(payloads)
	err := handler.HandleRelay(ctx)
	// Batch should NOT propagate the sub-error.
	if err != nil {
		t.Fatalf("batch should not fail the whole request on partial error, got: %v", err)
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &arr); err != nil {
		t.Fatalf("expected JSON array: %v\nbody: %s", err, ctx.Response.Body)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}

	// The second element is a JSON-RPC error RESPONSE for the second REQUEST:
	// the client matches it by id, and the id keeps its JSON type.
	assertBatchErrorItem(t, arr[1], `"req-2"`)
	var errElem struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(arr[1], &errElem)
	if strings.Contains(errElem.Error.Message, "10.0.0.1") {
		t.Errorf("batch item error leaks the cause chain: %q", errElem.Error.Message)
	}
}

// assertBatchErrorItem checks one element of a batch response is a JSON-RPC
// 2.0 error response object carrying wantID verbatim.
func assertBatchErrorItem(t *testing.T, item json.RawMessage, wantID string) {
	t.Helper()
	var elem struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(item, &elem); err != nil {
		t.Fatalf("failed to unmarshal error element: %v\n%s", err, item)
	}
	if elem.JSONRPC != "2.0" {
		t.Errorf("jsonrpc = %q, want \"2.0\": %s", elem.JSONRPC, item)
	}
	if string(elem.ID) != wantID {
		t.Errorf("id = %s, want %s: %s", elem.ID, wantID, item)
	}
	if elem.Error == nil {
		t.Fatalf("expected error member: %s", item)
	}
	if elem.Error.Code != -32603 {
		t.Errorf("error.code = %d, want -32603", elem.Error.Code)
	}
	if elem.Error.Message == "" {
		t.Error("error.message is empty")
	}
}

// An item that came back with no body gets an error response too. A null in
// the array is not a response object, and a client walking the batch by id
// would find nothing for that request.
func TestBatch_EmptyBodyIsAnErrorResponseWithTheRequestID(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if ctx.Payloads[0].Method() == "empty" {
			ctx.Response = &domain.Response{HTTPStatusCode: 200}
			return nil
		}
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})
	handler := Batch(4, 0, nil, nil)(inner)

	payloads := []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"ok"}`), domain.RPCTypeJSONRPC, "ok"),
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":2,"method":"empty"}`), domain.RPCTypeJSONRPC, "empty"),
		// No id at all: answered with id null, as a node would.
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"empty"}`), domain.RPCTypeJSONRPC, "empty"),
	}

	ctx := makeMultiPayloadCtx(payloads)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &arr); err != nil {
		t.Fatalf("expected JSON array: %v\nbody: %s", err, ctx.Response.Body)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	assertBatchErrorItem(t, arr[1], "2")
	assertBatchErrorItem(t, arr[2], "null")
}

// The items a dead request never started are answered the same way.
func TestBatch_NotStartedItemsCarryTheirRequestIDs(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error { return nil })
	handler := Batch(16, 0, nil, nil)(inner)

	payloads := []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":"a","method":"m"}`), domain.RPCTypeJSONRPC, "m"),
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":"b","method":"m"}`), domain.RPCTypeJSONRPC, "m"),
	}
	goCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := makeMultiPayloadCtxWith(goCtx, payloads)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &arr); err != nil {
		t.Fatalf("expected JSON array: %v\nbody: %s", err, ctx.Response.Body)
	}
	assertBatchErrorItem(t, arr[0], `"a"`)
	assertBatchErrorItem(t, arr[1], `"b"`)
}

func TestBatch_EmptyPayloads_PassThrough(t *testing.T) {
	var called atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Batch(4, 0, nil, nil)
	handler := mw(inner)

	ctx := makeMultiPayloadCtx(nil)
	_ = handler.HandleRelay(ctx)

	// Empty (0) payloads <= 1, so passes through.
	if called.Load() != 1 {
		t.Fatalf("expected inner called once for empty payloads, got %d", called.Load())
	}
}

func TestBatch_UnboundedParallelism(t *testing.T) {
	// maxParallel <= 0 should run all sub-relays concurrently.
	var relayCount atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Batch(0, 0, nil, nil) // unbounded
	handler := mw(inner)

	payloads := make([]domain.Payload, 10)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`p`), domain.RPCTypeJSONRPC, "m")
	}

	ctx := makeMultiPayloadCtx(payloads)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if relayCount.Load() != 10 {
		t.Fatalf("expected 10 relays, got %d", relayCount.Load())
	}
}

// A batch is an amplifier: one HTTP request becomes len(Payloads) upstream
// relays, each with its own retry and hedge fan-out. The cap has to reject
// before the fan-out, not bound it afterwards — by then the goroutines and the
// upstream load already exist.
func TestBatch_OversizedBatchRejectedBeforeFanOut(t *testing.T) {
	var relayCount atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Batch(4, 3, nil, nil)
	handler := mw(inner)

	payloads := make([]domain.Payload, 10)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}
	ctx := makeMultiPayloadCtx(payloads)

	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected an error for a batch over the limit")
	}
	if relayCount.Load() != 0 {
		t.Errorf("inner handler ran %d times; an over-limit batch must not reach it at all", relayCount.Load())
	}

	var relayErr *domain.RelayError
	if !errors.As(err, &relayErr) {
		t.Fatalf("err = %T, want *domain.RelayError", err)
	}
	if relayErr.Kind != domain.ErrValidation {
		t.Errorf("Kind = %v, want ErrValidation — an oversized batch is the client's fault", relayErr.Kind)
	}
	if relayErr.Retryable {
		t.Error("an oversized batch is not retryable; retrying it just repeats the amplification")
	}
}

// The cap counts payloads, so a batch exactly at the limit still runs.
func TestBatch_AtLimitIsAllowed(t *testing.T) {
	var relayCount atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	handler := Batch(4, 3, nil, nil)(inner)

	payloads := make([]domain.Payload, 3)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}

	if err := handler.HandleRelay(makeMultiPayloadCtx(payloads)); err != nil {
		t.Fatalf("a batch at the limit must be allowed: %v", err)
	}
	if relayCount.Load() != 3 {
		t.Errorf("relayCount = %d, want 3", relayCount.Load())
	}
}

// maxPayloads <= 0 keeps the previous unbounded behaviour, so the knob is
// opt-out rather than a surprise for anyone who has not configured it.
func TestBatch_ZeroLimitDisablesCap(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})
	handler := Batch(4, 0, nil, nil)(inner)

	payloads := make([]domain.Payload, 50)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}
	if err := handler.HandleRelay(makeMultiPayloadCtx(payloads)); err != nil {
		t.Fatalf("maxPayloads=0 must not cap: %v", err)
	}
}

// Sub-relays run on clones, so a fallback inside one is invisible to the caller
// unless it is merged back. Before the header moved to the router, the shared
// writer carried this signal — racily — so dropping the header without merging
// here would have silently stopped reporting degraded batches.
func TestBatch_MergesDegradedFromSubRelays(t *testing.T) {
	cases := []struct {
		name         string
		degradeIndex int // which sub-relay degrades; -1 for none
		want         bool
	}{
		{"no sub-relay degrades", -1, false},
		{"the first degrades", 0, true},
		{"a later one degrades", 2, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var idx atomic.Int32
			idx.Store(-1)

			inner := relay.HandlerFunc(func(ctx *relay.Context) error {
				if int(idx.Add(1)) == tc.degradeIndex {
					ctx.Degraded = true
				}
				ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
				return nil
			})

			handler := Batch(1, 0, nil, nil)(inner) // maxParallel=1 makes the index deterministic

			payloads := make([]domain.Payload, 3)
			for i := range payloads {
				payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
			}
			ctx := makeMultiPayloadCtx(payloads)

			if err := handler.HandleRelay(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ctx.Degraded != tc.want {
				t.Errorf("ctx.Degraded = %v, want %v", ctx.Degraded, tc.want)
			}
		})
	}
}

// A single-payload request bypasses the fan-out entirely, so degradation set by
// the inner chain is already on the caller's context and must survive.
func TestBatch_SinglePayloadDegradedPassesThrough(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Degraded = true
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})
	handler := Batch(4, 0, nil, nil)(inner)

	p := domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	ctx := makeMultiPayloadCtx([]domain.Payload{p})

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ctx.Degraded {
		t.Error("a degraded single-payload relay must stay degraded")
	}
}

// makeMultiPayloadCtxWith builds a batch context on a caller-supplied context,
// for tests that need to cancel it.
func makeMultiPayloadCtxWith(goCtx context.Context, payloads []domain.Payload) *relay.Context {
	req, _ := http.NewRequest(http.MethodPost, "/v1", nil)
	ctx := relay.NewContext(goCtx, req, nil, nil)
	ctx.ServiceID = "eth"
	ctx.Payloads = payloads
	return ctx
}

// max_concurrent_relays is documented as a GLOBAL ceiling, so the semaphore is
// built once when the middleware is constructed rather than per request. A
// per-request semaphore bounds one batch and nothing else: two concurrent
// batches would each get their own budget and the real total would be double
// the "limit". This is the test that tells the two apart.
func TestBatch_ConcurrencyBoundIsGlobalAcrossRequests(t *testing.T) {
	const budget = 2

	var (
		mu      sync.Mutex
		active  int
		peak    int
		release = make(chan struct{})
	)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		mu.Unlock()

		<-release // hold the slot until the test says otherwise

		mu.Lock()
		active--
		mu.Unlock()

		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	// ONE middleware instance shared by both requests — as in wire.go.
	handler := Batch(budget, 0, nil, nil)(inner)

	payloads := make([]domain.Payload, 4)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}

	var wg sync.WaitGroup
	for r := 0; r < 2; r++ { // two concurrent batch requests, 4 payloads each
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = handler.HandleRelay(makeMultiPayloadCtx(payloads))
		}()
	}

	// Wait until the budget is saturated. Asserting on peak rather than on a
	// momentary active count: with a per-request semaphore, active climbs
	// straight past the budget to 2×, and "active == budget" would be true only
	// in passing on the way up.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return active >= budget
	}, 2*time.Second, 5*time.Millisecond, "the budget should saturate")

	// Give any over-admission a chance to show up before reading the peak.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	gotPeak := peak
	mu.Unlock()
	if gotPeak > budget {
		t.Errorf("peak concurrent sub-relays = %d, want <= %d — a per-request semaphore lets each request have its own budget, so the real ceiling is requests × limit", gotPeak, budget)
	}

	close(release)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if peak > budget {
		t.Errorf("peak concurrent sub-relays = %d across both requests, want <= %d", peak, budget)
	}
}

// A shared semaphore couples requests together, so acquiring it has to respect
// the request deadline. Otherwise a saturated budget parks goroutines waiting
// for an answer nobody is expecting any more.
func TestBatch_SaturatedBudgetRespectsRequestDeadline(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	// started reports that a sub-relay is inside the handler, which means its
	// slot is taken. Without waiting on it the "occupying" request may not have
	// acquired anything yet, and the test would be racing the very saturation
	// it is trying to set up.
	started := make(chan struct{}, 1)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		started <- struct{}{}
		<-block
		return nil
	})

	handler := Batch(1, 0, nil, nil)(inner) // budget of exactly one

	payloads := make([]domain.Payload, 2)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}

	// Occupy the only slot with a request that never finishes.
	go func() { _ = handler.HandleRelay(makeMultiPayloadCtx(payloads)) }()
	<-started

	// A second request whose context is already dead must return, not block.
	goCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		_ = handler.HandleRelay(makeMultiPayloadCtxWith(goCtx, payloads))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled request blocked on the shared budget instead of returning")
	}
}

// The budget being *available* is the interesting case: select picks a random
// ready case, so a cancelled request racing a free slot would take it roughly
// half the time — spending a global resource on a relay nobody is waiting for.
// Only an explicit context check ahead of the select makes this deterministic,
// so this runs enough rounds that the old random-pick behaviour cannot survive.
func TestBatch_CancelledRequestDoesNotSpendBudget(t *testing.T) {
	var called atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called.Add(1)
		return nil
	})

	// A wide-open budget: nothing is contended, only the context is dead.
	handler := Batch(16, 0, nil, nil)(inner)

	payloads := make([]domain.Payload, 2)
	for i := range payloads {
		payloads[i] = domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}

	for i := 0; i < 200; i++ {
		goCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := handler.HandleRelay(makeMultiPayloadCtxWith(goCtx, payloads)); err != nil {
			t.Fatalf("round %d: HandleRelay returned %v", i, err)
		}
	}

	if n := called.Load(); n != 0 {
		t.Errorf("cancelled requests started %d sub-relays, want 0", n)
	}
}

// One payload panicking must cost that payload, not the process and not its
// siblings. wg.Done() already ran on the way out even before this fix, so the
// failure mode without it was a crash — and with a bare recover it would have
// been a silent `null` where an error belongs.
func TestBatch_PanicInOnePayloadIsIsolated(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if ctx.Payloads[0].Method() == "eth_explode" {
			panic("nil map write")
		}
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	ctx := makeMultiPayloadCtx([]domain.Payload{
		domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
		domain.NewPayload([]byte(`{"method":"eth_explode"}`), domain.RPCTypeJSONRPC, "eth_explode"),
		domain.NewPayload([]byte(`{"method":"eth_chainId"}`), domain.RPCTypeJSONRPC, "eth_chainId"),
	})

	if err := Batch(4, 0, nil, nil)(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("batch failed wholesale: %v", err)
	}

	var results []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &results); err != nil {
		t.Fatalf("response is not a batch array: %v (%s)", err, ctx.Response.Body)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	body := string(ctx.Response.Body)
	if strings.Count(body, `"result":"ok"`) != 2 {
		t.Errorf("the healthy payloads did not both succeed: %s", body)
	}
	if !strings.Contains(body, "error") {
		t.Errorf("the panicking payload produced no error entry: %s", body)
	}
	if strings.Contains(string(results[1]), `"result":"ok"`) {
		t.Errorf("the panicking payload reported success: %s", results[1])
	}
}

func TestBatch_SetsSinkAndFlushesWorstOf(t *testing.T) {
	rep := &recordingRepService{}
	flags := newFlags(featureflag.FlagScoringV2)
	// Inner handler: every sub-relay lands on endpoint A; the third one
	// adds a fatal signal to the sink the way the score middleware would.
	var n int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// assert, not require: this runs on the sub-relay goroutine, and
		// require's FailNow there would leave the batch waiting forever.
		if !assert.NotNil(t, ctx.ScoreSink, "batch must hand sub-relays a sink") {
			ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
			return nil
		}
		i := atomic.AddInt32(&n, 1)
		sig := reputation.NewSuccessSignal("ok", 0)
		if i == 3 {
			sig = reputation.NewFatalErrorSignal("fabricated", 0)
		}
		ctx.ScoreSink.Add("pokt1a-https://a", domain.RPCTypeJSONRPC, sig)
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), HTTPStatusCode: 200}
		return nil
	})
	// maxParallel=1: the sub-relays run in order, so "the third one is fatal
	// and the two after it are successes" is deterministic rather than a race
	// that would sometimes let the fatal be the last Add and pass regardless
	// of whether the collapse is worst-of or last-wins.
	h := Batch(1, 0, flags, rep)(inner)
	ctx := baseContext()
	ctx.Payloads = make([]domain.Payload, 5)
	for i := range ctx.Payloads {
		ctx.Payloads[i] = domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}
	require.NoError(t, h.HandleRelay(ctx))
	got := rep.all()
	require.Len(t, got, 1, "one signal per endpoint, not per payload")
	assert.Equal(t, reputation.SignalFatalError, got[0].Signal.Type)
	assert.Equal(t, domain.EndpointAddr("pokt1a-https://a"), got[0].Endpoint)
	assert.Equal(t, domain.RPCTypeJSONRPC, got[0].RPC)
	assert.Nil(t, ctx.ScoreSink, "the sink is cleared after flush so a reused context does not carry it")
}

func TestBatch_NoSinkWhenFlagOff(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		assert.Nil(t, ctx.ScoreSink)
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})
	h := Batch(0, 0, newFlags(), rep)(inner)
	ctx := baseContext()
	ctx.Payloads = make([]domain.Payload, 2)
	for i := range ctx.Payloads {
		ctx.Payloads[i] = domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	}
	require.NoError(t, h.HandleRelay(ctx))
	assert.Empty(t, rep.all())
}

// A single payload is not a batch: it passes straight through, so there is
// nothing to collapse and batch must not install a sink or score anything —
// even with scoring_v2 on and a live reputation service. Scoring that relay is
// the score middleware's job, on the ordinary single-relay path.
func TestBatch_SinglePayload_NoSinkWithFlagOn(t *testing.T) {
	rep := &recordingRepService{}
	var called atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called.Add(1)
		assert.Nil(t, ctx.ScoreSink, "a single-payload pass-through gets no sink")
		ctx.Response = &domain.Response{Body: []byte(`{"result":"ok"}`), HTTPStatusCode: 200}
		return nil
	})

	h := Batch(4, 0, newFlags(featureflag.FlagScoringV2), rep)(inner)
	ctx := baseContext()
	ctx.Payloads = []domain.Payload{
		domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}

	require.NoError(t, h.HandleRelay(ctx))
	require.Equal(t, int32(1), called.Load())
	assert.Nil(t, ctx.ScoreSink, "and none left on the parent either")
	assert.Empty(t, rep.all(), "batch records nothing for a request it only passed through")
}
