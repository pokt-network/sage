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

	mw := Observe(flags, queue, repSvc)
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

	mw := Observe(flags, queue, repSvc)
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

	mw := Observe(flags, queue, repSvc)
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

	mw := Observe(flags, queue, repSvc)
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

	mw := Observe(flags, queue, repSvc)
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

	mw := Observe(flags, queue, repSvc)
	_ = mw(inner).HandleRelay(ctx)

	if repSvc.called {
		t.Error("RecordSignal should not be called when no endpoint is selected")
	}
}

// newTrackingQueueHandler is a helper to create a trackingQueueHandler.
func newTrackingQueueHandler() *trackingQueueHandler {
	return &trackingQueueHandler{}
}
