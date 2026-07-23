package observe

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type countingHandler struct {
	count atomic.Int64
}

func (h *countingHandler) HandleObservation(_ context.Context, _ Observation) error {
	h.count.Add(1)
	return nil
}

func TestQueue_SubmitAndProcess(t *testing.T) {
	h := &countingHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     true,
		SampleRate:  1.0,
		WorkerCount: 2,
		QueueSize:   100,
	}, h, slog.Default())

	ctx := context.Background()
	q.Start(ctx)

	for i := 0; i < 10; i++ {
		q.Submit(Observation{ServiceID: "eth", Source: SourceRelay})
	}

	// Wait for processing.
	time.Sleep(50 * time.Millisecond)
	q.Stop()

	if got := h.count.Load(); got != 10 {
		t.Errorf("expected 10 processed, got %d", got)
	}
}

// panickyHandler panics on the first observation, then counts subsequent ones.
// Verifies a panic in one observation cannot kill the worker goroutine (#515).
type panickyHandler struct {
	seen atomic.Int64
}

func (h *panickyHandler) HandleObservation(_ context.Context, _ Observation) error {
	if h.seen.Add(1) == 1 {
		panic("boom: malformed observation")
	}
	return nil
}

func TestQueue_HandlerPanicDoesNotKillWorker(t *testing.T) {
	h := &panickyHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     true,
		SampleRate:  1.0,
		WorkerCount: 1,
		QueueSize:   100,
	}, h, slog.Default())

	ctx := context.Background()
	q.Start(ctx)

	for i := 0; i < 5; i++ {
		q.Submit(Observation{ServiceID: "eth", Source: SourceRelay})
	}

	time.Sleep(50 * time.Millisecond)
	q.Stop()

	// First panicked; the worker must survive and process the remaining 4.
	if got := h.seen.Load(); got != 5 {
		t.Errorf("expected worker to survive panic and see all 5, got %d", got)
	}
}

func TestQueue_Disabled(t *testing.T) {
	h := &countingHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     false,
		SampleRate:  1.0,
		WorkerCount: 1,
		QueueSize:   10,
	}, h, slog.Default())

	ctx := context.Background()
	q.Start(ctx)
	q.Submit(Observation{ServiceID: "eth"})
	time.Sleep(20 * time.Millisecond)
	q.Stop()

	if got := h.count.Load(); got != 0 {
		t.Errorf("expected 0 processed when disabled, got %d", got)
	}
}

func TestQueue_SamplingRate(t *testing.T) {
	h := &countingHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     true,
		SampleRate:  0.0001, // very low sample rate
		WorkerCount: 1,
		QueueSize:   1000,
	}, h, slog.Default())

	ctx := context.Background()
	q.Start(ctx)

	// Submit many relay observations.
	for i := 0; i < 1000; i++ {
		q.Submit(Observation{ServiceID: "eth", Source: SourceRelay})
	}

	time.Sleep(50 * time.Millisecond)
	q.Stop()

	// With 0.01% sample rate and 1000 submissions, we expect very few processed.
	// Allow some statistical variance but it should be much less than 1000.
	if got := h.count.Load(); got > 50 {
		t.Errorf("expected very few processed with low sample rate, got %d", got)
	}
}

func TestQueue_HealthCheckBypassesSampling(t *testing.T) {
	h := &countingHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     true,
		SampleRate:  0.0001, // almost nothing sampled
		WorkerCount: 1,
		QueueSize:   100,
	}, h, slog.Default())

	ctx := context.Background()
	q.Start(ctx)

	// Health check observations should always be submitted.
	for i := 0; i < 10; i++ {
		q.Submit(Observation{ServiceID: "eth", Source: SourceHealthCheck})
	}

	time.Sleep(50 * time.Millisecond)
	q.Stop()

	if got := h.count.Load(); got != 10 {
		t.Errorf("expected all 10 health check observations processed, got %d", got)
	}
}

func TestQueue_StopDrainsRemaining(t *testing.T) {
	h := &countingHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     true,
		SampleRate:  1.0,
		WorkerCount: 1,
		QueueSize:   100,
	}, h, slog.Default())

	// Don't start workers yet — just submit.
	for i := 0; i < 5; i++ {
		q.Submit(Observation{ServiceID: "eth", Source: SourceRelay})
	}

	// Start and immediately stop.
	ctx := context.Background()
	q.Start(ctx)
	q.Stop()

	if got := h.count.Load(); got != 5 {
		t.Errorf("expected 5 drained on Stop, got %d", got)
	}
}

func TestQueue_DropWhenFull(t *testing.T) {
	h := &countingHandler{}
	q := NewQueue(QueueConfig{
		Enabled:     true,
		SampleRate:  1.0,
		WorkerCount: 1,
		QueueSize:   2,
	}, h, slog.Default())

	// Don't start workers - channel will fill up.
	q.Submit(Observation{ServiceID: "eth", Source: SourceRelay})
	q.Submit(Observation{ServiceID: "eth", Source: SourceRelay})
	q.Submit(Observation{ServiceID: "eth", Source: SourceRelay}) // should be dropped

	// Start and drain.
	ctx := context.Background()
	q.Start(ctx)
	q.Stop()

	if got := h.count.Load(); got != 2 {
		t.Errorf("expected 2 (queue size), got %d", got)
	}
}
