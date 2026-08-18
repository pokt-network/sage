package observe

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"

	"github.com/pokt-network/sage/internal/safego"
)

// QueueConfig controls the async observation worker pool.
type QueueConfig struct {
	Enabled     bool    `yaml:"enabled"`
	SampleRate  float64 `yaml:"sample_rate"` // 0.0-1.0, fraction of relays deep-parsed
	WorkerCount int     `yaml:"worker_count"`
	QueueSize   int     `yaml:"queue_size"`
}

// Handler processes a single observation asynchronously.
type Handler interface {
	HandleObservation(ctx context.Context, obs Observation) error
}

// Queue is an async worker pool that processes observations off the hot path.
type Queue struct {
	cfg     QueueConfig
	ch      chan Observation
	handler Handler
	logger  *slog.Logger
	wg      sync.WaitGroup
	done    chan struct{}
}

// NewQueue creates a new observation queue. Call Start to begin processing.
func NewQueue(cfg QueueConfig, handler Handler, logger *slog.Logger) *Queue {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 1.0
	}
	if cfg.SampleRate > 1.0 {
		cfg.SampleRate = 1.0
	}
	return &Queue{
		cfg:     cfg,
		ch:      make(chan Observation, cfg.QueueSize),
		handler: handler,
		logger:  logger,
		done:    make(chan struct{}),
	}
}

// Submit enqueues an observation for async processing.
// Non-blocking: drops the observation if the queue is full.
// Health check observations bypass sampling and are always submitted.
func (q *Queue) Submit(obs Observation) {
	if !q.cfg.Enabled {
		return
	}

	// Health check observations bypass sampling.
	if obs.Source != SourceHealthCheck {
		if q.cfg.SampleRate < 1.0 && rand.Float64() >= q.cfg.SampleRate {
			return
		}
	}

	select {
	case q.ch <- obs:
	default:
		q.logger.Warn("observation queue full, dropping observation",
			"service_id", obs.ServiceID,
			"endpoint_addr", obs.EndpointAddr,
		)
	}
}

// Start launches worker goroutines that process observations from the queue.
func (q *Queue) Start(ctx context.Context) {
	for i := 0; i < q.cfg.WorkerCount; i++ {
		q.wg.Add(1)
		safego.GoCtx(ctx, q.logger, "observe.worker", q.worker)
	}
}

// Stop signals workers to drain remaining items and stop. Blocks until all
// workers have finished.
func (q *Queue) Stop() {
	close(q.done)
	q.wg.Wait()
}

func (q *Queue) worker(ctx context.Context) {
	defer q.wg.Done()
	for {
		select {
		case obs := <-q.ch:
			q.handle(ctx, obs)
		case <-q.done:
			// Drain remaining items.
			for {
				select {
				case obs := <-q.ch:
					q.handle(ctx, obs)
				default:
					return
				}
			}
		}
	}
}

// handle dispatches a single observation to the handler. It runs in a detached
// worker goroutine, so a panic here (e.g. a metrics library rejecting a label,
// or a malformed response body reaching a parser) would otherwise be uncaught
// and crash the whole process. Recover so a single bad observation can never
// take down request serving — the observation pipeline is best-effort.
func (q *Queue) handle(ctx context.Context, obs Observation) {
	if q.handler == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			q.logger.Error("recovered from panic while handling observation; dropping it",
				"panic", r,
				"service_id", obs.ServiceID,
			)
		}
	}()
	if err := q.handler.HandleObservation(ctx, obs); err != nil {
		q.logger.Error("failed to handle observation",
			"error", err,
			"service_id", obs.ServiceID,
		)
	}
}
