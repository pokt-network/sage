package reputation

import (
	"context"
	"sync"

	"github.com/pokt-network/sage/domain"
)

// Service defines the contract for recording signals and querying endpoint
// reputation scores.
type Service interface {
	// RecordSignal records an observation about an endpoint's behavior.
	RecordSignal(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, signal Signal) error
	// GetScore returns the current reputation score for an endpoint.
	GetScore(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) (float64, error)
	// GetScores returns all scores for a given service.
	GetScores(ctx context.Context, serviceID domain.ServiceID) (map[domain.EndpointAddr]float64, error)
	// SelectBest returns the best endpoint from the given list based on reputation.
	SelectBest(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList) domain.EndpointAddr
	// SelectSpread picks an endpoint by tier cascade with weighted-random
	// within the top tier, biased away from endpoints carrying higher active
	// load (e.g., open WS bridges). Used when many concurrent connections
	// must be distributed to prevent supplier concentration.
	SelectSpread(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, activeLoad map[domain.EndpointAddr]int) domain.EndpointAddr
	// ResetScore resets an endpoint's score to the initial value.
	ResetScore(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) error
}

// ServiceConfig holds configuration for the reputation service.
type ServiceConfig struct {
	// InitialScore is the score assigned to newly seen endpoints. Default: 100.
	InitialScore float64
	// MaxScore is the upper bound for scores. Default: 100.
	MaxScore float64
	// WriteQueueSize is the buffer size for async storage writes. Default: 4096.
	WriteQueueSize int
}

// DefaultServiceConfig returns a ServiceConfig with sensible defaults.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		InitialScore:   100,
		MaxScore:       100,
		WriteQueueSize: 4096,
	}
}

// scoreKey produces the storage key for a service+endpoint pair.
func scoreKey(serviceID domain.ServiceID, endpoint domain.EndpointAddr) string {
	return string(serviceID) + ":" + string(endpoint)
}

// writeOp represents an asynchronous score write.
type writeOp struct {
	key   string
	score float64
}

// scoreShards stripes the in-memory score map so concurrent relays recording
// signals for different endpoints don't serialize on one process-wide mutex.
const scoreShards = 32

// scoreShard holds scores keyed by serviceID then endpoint. Nested maps
// (rather than a concatenated string key) keep the per-relay selector read
// path allocation-free: lookups never build a key string.
type scoreShard struct {
	mu    sync.RWMutex
	cache map[domain.ServiceID]map[domain.EndpointAddr]float64
}

// serviceImpl is the default implementation of Service.
type serviceImpl struct {
	cfg      ServiceConfig
	storage  Storage
	timeline *Timeline
	selector *TieredSelector

	// In-memory score cache, striped by key hash.
	shards [scoreShards]scoreShard

	// Async write queue.
	writeCh chan writeOp
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewService creates a new reputation service with the given storage backend
// and configuration. Call Start() to begin processing async writes.
func NewService(storage Storage, timeline *Timeline, cfg ServiceConfig) *serviceImpl {
	if cfg.InitialScore == 0 {
		cfg.InitialScore = 100
	}
	if cfg.MaxScore == 0 {
		cfg.MaxScore = 100
	}
	if cfg.WriteQueueSize == 0 {
		cfg.WriteQueueSize = 4096
	}
	s := &serviceImpl{
		cfg:      cfg,
		storage:  storage,
		timeline: timeline,
		writeCh:  make(chan writeOp, cfg.WriteQueueSize),
		stopCh:   make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].cache = make(map[domain.ServiceID]map[domain.EndpointAddr]float64)
	}
	s.selector = NewTieredSelector(DefaultSelectorConfig(), s.scoreForSelector)
	return s
}

// shard returns the score shard for an endpoint. Sharding by endpoint (not
// service) spreads load even when one service dominates traffic.
func (s *serviceImpl) shard(endpoint domain.EndpointAddr) *scoreShard {
	return &s.shards[fnv32a(string(endpoint))%scoreShards]
}

// scoreForSelector is the per-endpoint score lookup handed to TieredSelector.
// It reads from the in-memory cache under a read lock; unseen endpoints are
// returned as the configured initial score so new endpoints are not filtered
// out on the first request. Zero allocations — runs per endpoint per relay.
func (s *serviceImpl) scoreForSelector(_ context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr) (float64, bool) {
	sh := s.shard(ep)
	sh.mu.RLock()
	v, ok := sh.cache[serviceID][ep]
	sh.mu.RUnlock()
	if !ok {
		return s.cfg.InitialScore, true
	}
	return v, true
}

// Start begins the background goroutine that flushes writes to storage.
func (s *serviceImpl) Start() {
	s.wg.Add(1)
	go s.drainWrites()
}

// Stop signals the background goroutine to exit and waits for it to finish.
func (s *serviceImpl) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// RecordSignal applies a signal's impact to the endpoint's score.
func (s *serviceImpl) RecordSignal(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, signal Signal) error {
	impact := float64(DefaultImpact(signal.Type))

	sh := s.shard(endpoint)
	sh.mu.Lock()
	svcScores := sh.cache[serviceID]
	if svcScores == nil {
		svcScores = make(map[domain.EndpointAddr]float64)
		sh.cache[serviceID] = svcScores
	}
	current, ok := svcScores[endpoint]
	if !ok {
		current = s.cfg.InitialScore
	}
	newScore := current + impact
	newScore = s.clamp(newScore)
	svcScores[endpoint] = newScore
	sh.mu.Unlock()

	// Storage and timeline are keyed by the concatenated string form.
	key := scoreKey(serviceID, endpoint)

	// Record timeline event. Structured fields only — Detail is rendered on
	// the admin read path, not here on the relay hot path.
	if s.timeline != nil {
		s.timeline.Record(key, TimelineEvent{
			Timestamp:  signal.Timestamp,
			Event:      "signal",
			SignalType: string(signal.Type),
			Reason:     signal.Reason,
			OldScore:   current,
			Score:      newScore,
		})
	}

	// Enqueue async write (non-blocking: drop if queue full).
	select {
	case s.writeCh <- writeOp{key: key, score: newScore}:
	default:
	}

	return nil
}

// GetScore returns the cached score for the endpoint. If the endpoint has not
// been seen, the initial score is returned.
func (s *serviceImpl) GetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) (float64, error) {
	sh := s.shard(endpoint)
	sh.mu.RLock()
	score, ok := sh.cache[serviceID][endpoint]
	sh.mu.RUnlock()
	if !ok {
		return s.cfg.InitialScore, nil
	}
	return score, nil
}

// GetScores returns all cached scores for endpoints of the given service.
func (s *serviceImpl) GetScores(_ context.Context, serviceID domain.ServiceID) (map[domain.EndpointAddr]float64, error) {
	result := make(map[domain.EndpointAddr]float64)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for ep, v := range sh.cache[serviceID] {
			result[ep] = v
		}
		sh.mu.RUnlock()
	}
	return result, nil
}

// SelectBest returns an endpoint chosen by tier cascade (T1 → T2 → T3), with
// random-within-tier selection to spread load across similarly-scored peers.
// This prevents deterministic-max-score from concentrating all traffic on a
// single endpoint when several are performing equivalently well.
//
// Returns "" when the endpoints list is empty or all endpoints are below the
// configured minimum threshold.
func (s *serviceImpl) SelectBest(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList) domain.EndpointAddr {
	if len(endpoints) == 0 {
		return ""
	}
	list := s.selector.Select(ctx, serviceID, endpoints)
	if len(list) == 0 {
		return ""
	}
	// TieredSelector may prepend a probation endpoint; for SelectBest callers
	// (the HTTP path) we want the healthy pick, which is always the last
	// element of the returned list.
	return list[len(list)-1]
}

// SelectSpread selects an endpoint using tier cascade and load-aware
// weighted-random within the top tier. Endpoints absent from activeLoad are
// treated as load=0 (equivalent to uniform weighting).
func (s *serviceImpl) SelectSpread(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, activeLoad map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(endpoints) == 0 {
		return ""
	}
	candidates := s.selector.TopTierCandidates(ctx, serviceID, endpoints)
	if len(candidates) == 0 {
		return ""
	}
	return pickWeightedByInverseLoad(candidates, activeLoad)
}

// ResetScore resets the endpoint's score to the initial value.
func (s *serviceImpl) ResetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) error {
	sh := s.shard(endpoint)
	sh.mu.Lock()
	svcScores := sh.cache[serviceID]
	if svcScores == nil {
		svcScores = make(map[domain.EndpointAddr]float64)
		sh.cache[serviceID] = svcScores
	}
	svcScores[endpoint] = s.cfg.InitialScore
	sh.mu.Unlock()

	select {
	case s.writeCh <- writeOp{key: scoreKey(serviceID, endpoint), score: s.cfg.InitialScore}:
	default:
	}
	return nil
}

// clamp constrains a score to [0, MaxScore].
func (s *serviceImpl) clamp(score float64) float64 {
	if score < 0 {
		return 0
	}
	if score > s.cfg.MaxScore {
		return s.cfg.MaxScore
	}
	return score
}

// drainWrites processes the async write queue until Stop is called.
func (s *serviceImpl) drainWrites() {
	defer s.wg.Done()
	for {
		select {
		case op := <-s.writeCh:
			_ = s.storage.SetScore(context.Background(), op.key, op.score)
		case <-s.stopCh:
			// Drain remaining writes.
			for {
				select {
				case op := <-s.writeCh:
					_ = s.storage.SetScore(context.Background(), op.key, op.score)
				default:
					return
				}
			}
		}
	}
}
