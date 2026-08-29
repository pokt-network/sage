package middleware

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/traffic"
)

// trackingRepService records the last signal passed to RecordSignal.
type trackingRepService struct {
	mu     sync.Mutex
	last   reputation.Signal
	called bool
}

func (s *trackingRepService) RecordSignal(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType, sig reputation.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = sig
	s.called = true
	return nil
}

func (s *trackingRepService) GetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) (float64, error) {
	return 100, nil
}

func (s *trackingRepService) GetScores(_ context.Context, _ domain.ServiceID) (map[string]float64, error) {
	return nil, nil
}

func (s *trackingRepService) SelectBest(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	if len(eps) > 0 {
		return eps[0]
	}
	return ""
}

func (s *trackingRepService) SelectSpread(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(eps) > 0 {
		return eps[0]
	}
	return ""
}

func (s *trackingRepService) ResetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr) error {
	return nil
}

func (s *trackingRepService) Vouched(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) bool {
	return true
}

// trackingQueueHandler records observations for assertion.
type trackingQueueHandler struct {
	mu  sync.Mutex
	obs []observe.Observation
}

func (h *trackingQueueHandler) HandleObservation(_ context.Context, obs observe.Observation) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.obs = append(h.obs, obs)
	return nil
}

func (h *trackingQueueHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.obs)
}

func TestObserve_SuccessSignalOnGoodResponse(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags(featureflag.FlagObservationPipeline)
	handler := newTrackingQueueHandler()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: true, WorkerCount: 1, QueueSize: 10, SampleRate: 1.0}, handler, nil)
	queue.Start(context.Background())

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{"result":1}`)}
		return nil
	})

	ctx := baseContext()
	ctx.Endpoint = "supplierA-https://node.example.com"

	mw := Observe(flags, queue, repSvc, nil)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repSvc.mu.Lock()
	sig := repSvc.last
	repSvc.mu.Unlock()

	if !repSvc.called {
		t.Fatal("RecordSignal was not called")
	}
	if sig.Type != reputation.SignalSuccess {
		t.Errorf("expected success signal, got %q", sig.Type)
	}
}

func TestObserve_ErrorSignalOnRelayError(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)

	sentErr := errors.New("backend down")
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		return sentErr
	})

	ctx := baseContext()
	ctx.Endpoint = "supplierA-https://node.example.com"

	mw := Observe(flags, queue, repSvc, nil)
	err := mw(inner).HandleRelay(ctx)
	if !errors.Is(err, sentErr) {
		t.Fatalf("expected sentErr, got %v", err)
	}

	repSvc.mu.Lock()
	sig := repSvc.last
	repSvc.mu.Unlock()

	if !repSvc.called {
		t.Fatal("RecordSignal was not called on error")
	}
	if sig.Type == reputation.SignalSuccess {
		t.Errorf("expected non-success signal on error, got %q", sig.Type)
	}
}

func TestObserve_HeuristicPenaltyRaisesSignalSeverity(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		// Simulate heuristic middleware storing a fatal result.
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			ShouldPenalize:  true,
			PenaltySeverity: heuristic.SeverityFatal,
			Reason:          "fabricated_response",
		}
		return nil
	})

	ctx := baseContext()
	ctx.Endpoint = "supplierA-https://node.example.com"

	mw := Observe(flags, queue, repSvc, nil)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repSvc.mu.Lock()
	sig := repSvc.last
	repSvc.mu.Unlock()

	if sig.Type != reputation.SignalFatalError {
		t.Errorf("expected fatal signal from heuristic result, got %q", sig.Type)
	}
}

func TestObserve_ObservationSubmittedWhenFlagEnabled(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags(featureflag.FlagObservationPipeline)
	handler := newTrackingQueueHandler()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: true, WorkerCount: 1, QueueSize: 10, SampleRate: 1.0}, handler, nil)
	queue.Start(context.Background())

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{}`)}
		return nil
	})

	ctx := baseContext()
	ctx.Endpoint = "supplierA-https://node.example.com"

	mw := Observe(flags, queue, repSvc, nil)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Drain remaining items and stop the worker.
	queue.Stop()

	if handler.count() == 0 {
		t.Error("expected at least one observation to be submitted to the queue")
	}
}

func TestObserve_ObservationNotSubmittedWhenFlagDisabled(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags() // observation_pipeline not enabled
	handler := newTrackingQueueHandler()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: true, WorkerCount: 1, QueueSize: 10, SampleRate: 1.0}, handler, nil)
	queue.Start(context.Background())

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{}`)}
		return nil
	})

	ctx := baseContext()

	mw := Observe(flags, queue, repSvc, nil)
	_ = mw(inner).HandleRelay(ctx)

	queue.Stop()

	if handler.count() > 0 {
		t.Error("expected no observations when flag is disabled")
	}
}

func TestObserve_NoSignalWhenNoEndpoint(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// No endpoint set.
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	ctx := baseContext()
	ctx.Endpoint = "" // Explicitly clear it.

	mw := Observe(flags, queue, repSvc, nil)
	_ = mw(inner).HandleRelay(ctx)

	if repSvc.called {
		t.Error("RecordSignal should not be called when no endpoint is selected")
	}
}

// newTrackingQueueHandler is a helper to create a trackingQueueHandler.
func newTrackingQueueHandler() *trackingQueueHandler {
	return &trackingQueueHandler{}
}

// A client hang-up is nobody's fault. The old fallback turned every relay
// error into a MinorError against whichever supplier held the relay — PATH's
// A/B showed those cancels track the slowest operator's tail latency, a
// latency signal misfiled as a fault.
func TestObserve_ClientCancelRecordsNoSignal(t *testing.T) {
	repSvc := &trackingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{Attribution: heuristic.AttrClient, Reason: "client_cancelled"}
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.Canceled, true)
	})
	mw := Observe(newFlags(), nil, repSvc, nil)
	ctx := baseContext()
	_ = mw(inner).HandleRelay(ctx)

	repSvc.mu.Lock()
	defer repSvc.mu.Unlock()
	if repSvc.called {
		t.Fatalf("a client cancel recorded a %q signal against the supplier", repSvc.last.Type)
	}
}

// The control: a transport timeout graded major must reach reputation as
// major, not as the old undifferentiated minor.
func TestObserve_TransportTimeoutIsMajor(t *testing.T) {
	repSvc := &trackingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = ctx.Endpoints[0]
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			Attribution: heuristic.AttrSupplier, ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMajor, Reason: "transport_timeout",
		}
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.DeadlineExceeded, true)
	})
	mw := Observe(newFlags(), nil, repSvc, nil)
	_ = mw(inner).HandleRelay(baseContext())

	repSvc.mu.Lock()
	defer repSvc.mu.Unlock()
	if !repSvc.called || repSvc.last.Type != reputation.SignalMajorError {
		t.Fatalf("signal = %v (called=%v), want major_error", repSvc.last.Type, repSvc.called)
	}
}

// --- Traffic sampler hook ---

// samplerRelayContext returns a relay context for a configured service:
// ctx.Plugin is set, as it would be by Parse for any Target-Service-Id that
// matches a registered service.
func samplerRelayContext() *relay.Context {
	ctx := baseContext()
	ctx.Endpoint = "supplierA-https://node.example.com"
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")}
	ctx.Plugin = normPlugin{}
	return ctx
}

func TestObserve_SamplerRecordsWhenFlagEnabled(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags(featureflag.FlagRequestSampler)
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)
	sampler := traffic.New(traffic.WithRate(1))

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{"result":"0x1"}`)}
		return nil
	})

	ctx := samplerRelayContext()

	mw := Observe(flags, queue, repSvc, sampler)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	summary, ok := sampler.Summary(ctx.ServiceID, false)
	if !ok {
		t.Fatal("expected the sampler to have observed the service")
	}
	if summary.Sampled != 1 {
		t.Errorf("sampled = %d, want 1", summary.Sampled)
	}
}

func TestObserve_SamplerNotRecordedWhenFlagDisabled(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags() // request_sampler not enabled
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)
	sampler := traffic.New(traffic.WithRate(1))

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{"result":"0x1"}`)}
		return nil
	})

	ctx := samplerRelayContext()

	mw := Observe(flags, queue, repSvc, sampler)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := sampler.Summary(ctx.ServiceID, false); ok {
		t.Error("expected the sampler to have observed nothing while the flag is off")
	}
}

// A nil sampler must not panic, flag on or off.
func TestObserve_NilSamplerIsSafe(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags(featureflag.FlagRequestSampler)
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	ctx := samplerRelayContext()

	mw := Observe(flags, queue, repSvc, nil)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A relay for a service with no plugin attached — Validate lets an unknown
// Target-Service-Id through, but no configured service means no plugin — must
// not reach the sampler even with the flag on: sampling it would let an
// unauthenticated client grow the sampler's per-service state without bound.
func TestObserve_SamplerSkipsServiceWithNoPlugin(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags(featureflag.FlagRequestSampler)
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)
	sampler := traffic.New(traffic.WithRate(1))

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK, Body: []byte(`{"result":"0x1"}`)}
		return nil
	})

	ctx := samplerRelayContext()
	ctx.Plugin = nil // no plugin registered for this service.

	mw := Observe(flags, queue, repSvc, sampler)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := sampler.Summary(ctx.ServiceID, false); ok {
		t.Error("expected the sampler to have observed nothing for a service with no plugin")
	}
}

// Under scoring_v2 the score middleware records one signal per attempt, from
// inside retry and hedge. Observe sits outside both and used to record one per
// client request; doing both would count every request twice.
func TestObserve_DoesNotScoreUnderScoringV2(t *testing.T) {
	rep := &trackingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{}`)}
		return nil
	})
	ctx := baseContext()
	ctx.Endpoint = "pokt1a-https://a"
	if err := Observe(newFlags(featureflag.FlagScoringV2), nil, rep, nil)(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rep.called {
		t.Error("under scoring_v2 the score middleware records; Observe must not double-count")
	}
}

// With scoring_v2 off this middleware is the scorer, and it must make the same
// correction the score middleware makes: a blockchain-attributed verdict that
// carries no penalty is an endpoint answering, even though the Heuristic
// middleware turned it into a relay error to trigger Retry. Grading that error
// a minor supplier penalty would walk an archival query down a pruned pool.
func TestObserve_BlockchainAnswerScoresSuccessOnLegacyPath(t *testing.T) {
	repSvc := &trackingRepService{}
	flags := newFlags()
	queue := observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, nil)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			ShouldRetry:    true,
			ShouldPenalize: false,
			Attribution:    heuristic.AttrBlockchain,
			Reason:         "block_not_found",
		}
		return errors.New("heuristic analysis suggests retry: block_not_found")
	})

	ctx := baseContext()
	mw := Observe(flags, queue, repSvc, nil)
	_ = mw(inner).HandleRelay(ctx)

	repSvc.mu.Lock()
	sig := repSvc.last
	repSvc.mu.Unlock()

	if sig.Type != reputation.SignalSuccess {
		t.Fatalf("blockchain-attributed, unpenalised verdict must score success, got %q (%s)", sig.Type, sig.Reason)
	}
	if sig.Reason != "block_not_found" {
		t.Errorf("signal must carry the analyzer's reason, got %q", sig.Reason)
	}
}
