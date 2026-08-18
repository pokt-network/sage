package reputation

import (
	"context"
	"sync"

	"github.com/pokt-network/sage/domain"

	"github.com/pokt-network/sage/internal/safego"
)

// Service defines the contract for recording signals and querying endpoint
// reputation scores.
type Service interface {
	// RecordSignal records an observation about an endpoint's behavior over the
	// given RPC type. Scores are per (endpoint-identity, RPC type) — see key.go.
	RecordSignal(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, signal Signal) error
	// GetScore returns the current reputation score for an endpoint on the
	// given RPC type.
	GetScore(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) (float64, error)
	// GetScores returns all scores for a given service, keyed by reputation
	// key rather than by endpoint address: at the default per-URL granularity
	// one key covers every supplier fronting that URL, so there is no single
	// endpoint to attribute it to. See key.go.
	GetScores(ctx context.Context, serviceID domain.ServiceID) (map[string]float64, error)
	// SelectBest returns the best endpoint from the given list based on its
	// reputation for the RPC type the request will be relayed over.
	SelectBest(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType) domain.EndpointAddr
	// SelectSpread picks an endpoint by tier cascade with weighted-random
	// within the top tier, biased away from endpoints carrying higher active
	// load (e.g., open WS bridges). Used when many concurrent connections
	// must be distributed to prevent supplier concentration.
	SelectSpread(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType, activeLoad map[domain.EndpointAddr]int) domain.EndpointAddr
	// ResetScore resets an endpoint's score to the initial value across every
	// RPC type. An operator resetting an endpoint means the endpoint, not one
	// of the protocols it happens to serve.
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
	// KeyGranularity selects what a score is attached to — see key.go. Empty
	// means the default, per-URL.
	KeyGranularity string
}

// DefaultServiceConfig returns a ServiceConfig with sensible defaults.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		InitialScore:   100,
		MaxScore:       100,
		WriteQueueSize: 4096,
	}
}

// scoreKey produces the storage key for a service plus an already-derived
// reputation key.
func scoreKey(serviceID domain.ServiceID, key string) string {
	return string(serviceID) + ":" + key
}

// writeOp represents an asynchronous score write.
type writeOp struct {
	key   string
	score float64
}

// scoreShards stripes the in-memory score map so concurrent relays recording
// signals for different endpoints don't serialize on one process-wide mutex.
const scoreShards = 32

// scoreShard holds scores keyed by serviceID then reputation key. Nested maps
// (rather than a concatenated string key) keep the per-relay selector read
// path allocation-free: lookups never build a key string. The reputation key
// itself is a substring of the endpoint address, so deriving it allocates
// nothing either.
type scoreShard struct {
	mu    sync.RWMutex
	cache map[domain.ServiceID]map[string]float64
}

// serviceImpl is the default implementation of Service.
type serviceImpl struct {
	cfg      ServiceConfig
	storage  Storage
	timeline *Timeline
	selector *TieredSelector
	// key maps an endpoint address to the identity its score lives under.
	key KeyFn

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
		s.shards[i].cache = make(map[domain.ServiceID]map[string]float64)
	}
	s.key = memoize(keyFnFor(cfg.KeyGranularity))
	s.selector = NewTieredSelector(DefaultSelectorConfig(), s.scoreForSelector)
	return s
}

// shard returns the score shard for a reputation key. Sharding by key (not
// service) spreads load even when one service dominates traffic.
func (s *serviceImpl) shard(key string) *scoreShard {
	return &s.shards[fnv32a(key)%scoreShards]
}

// scoreForSelector is the per-endpoint score lookup handed to TieredSelector.
// It reads from the in-memory cache under a read lock; unseen endpoints are
// returned as the configured initial score so new endpoints are not filtered
// out on the first request. Zero allocations — runs per endpoint per relay.
func (s *serviceImpl) scoreForSelector(_ context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (float64, bool) {
	key := s.key(ep, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	v, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		return s.cfg.InitialScore, true
	}
	return v, true
}

// SetCollapseHook registers a callback fired whenever the selector's
// pool-collapse guard has to serve an endpoint scoring below the minimum
// threshold because no endpoint cleared it. Call at wire time.
func (s *serviceImpl) SetCollapseHook(fn func(domain.ServiceID)) {
	s.selector.SetCollapseHook(fn)
}

// SetOperatorCap enables the per-operator concentration cap on both selection
// paths, gated per relay by gate. Call at wire time.
func (s *serviceImpl) SetOperatorCap(cfg OperatorCapConfig, gate func(context.Context, domain.ServiceID) bool) {
	s.selector.SetOperatorCap(cfg, gate)
}

// Start begins the background goroutine that flushes writes to storage.
func (s *serviceImpl) Start() {
	s.wg.Add(1)
	safego.Go(nil, "reputation.drain", s.drainWrites)
}

// Stop signals the background goroutine to exit and waits for it to finish.
func (s *serviceImpl) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// RecordSignal applies a signal's impact to the endpoint's score.
func (s *serviceImpl) RecordSignal(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, signal Signal) error {
	impact := float64(DefaultImpact(signal.Type))

	repKey := s.key(endpoint, rpcType)
	sh := s.shard(repKey)
	sh.mu.Lock()
	svcScores := sh.cache[serviceID]
	if svcScores == nil {
		svcScores = make(map[string]float64)
		sh.cache[serviceID] = svcScores
	}
	current, ok := svcScores[repKey]
	if !ok {
		current = s.cfg.InitialScore
	}
	newScore := current + impact
	newScore = s.clamp(newScore)
	svcScores[repKey] = newScore
	sh.mu.Unlock()

	// Storage and timeline are keyed by the concatenated string form.
	key := scoreKey(serviceID, repKey)

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
func (s *serviceImpl) GetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) (float64, error) {
	key := s.key(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	score, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		return s.cfg.InitialScore, nil
	}
	return score, nil
}

// GetScores returns all cached scores for the given service, keyed by
// reputation key at the configured granularity.
func (s *serviceImpl) GetScores(_ context.Context, serviceID domain.ServiceID) (map[string]float64, error) {
	result := make(map[string]float64)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, v := range sh.cache[serviceID] {
			result[key] = v
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
// Returns "" only when the endpoints list is empty. A pool in which every
// endpoint scores below the minimum threshold still yields the least-bad
// endpoint — see the pool-collapse guard on TieredSelector.Select.
func (s *serviceImpl) SelectBest(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType) domain.EndpointAddr {
	if len(endpoints) == 0 {
		return ""
	}
	list := s.selector.Select(ctx, serviceID, endpoints, rpcType)
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
func (s *serviceImpl) SelectSpread(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType, activeLoad map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(endpoints) == 0 {
		return ""
	}
	candidates := s.selector.TopTierCandidates(ctx, serviceID, endpoints, rpcType)
	if len(candidates) == 0 {
		return ""
	}

	// Two-step when the concentration cap is on: choose the operator under the
	// cap, then spread by connection load within it. Composing them this way
	// keeps both properties exact — the operator's share of new connections is
	// what the cap says, and within that operator the least-loaded endpoints
	// still win. Folding the cap into the inverse-load weights instead would
	// let one factor silently cancel the other.
	//
	// This runs on the WebSocket open path, not per relay, so narrowing the
	// list is affordable here in a way it would not be in Select.
	if s.selector.capActive(ctx, serviceID) {
		if operator, _, ok := cappedPick(s.selector.operatorCap, candidates, nil); ok {
			withinOperator := make(domain.EndpointAddrList, 0, len(candidates))
			for _, ep := range candidates {
				if ep.Operator() == operator {
					withinOperator = append(withinOperator, ep)
				}
			}
			if len(withinOperator) > 0 {
				candidates = withinOperator
			}
		}
	}

	return pickWeightedByInverseLoad(candidates, activeLoad)
}

// ResetScore resets the score the endpoint maps to. At a coarser granularity
// than per-endpoint this necessarily resets every endpoint sharing that key —
// resetting one supplier on a shared backend cannot mean anything else, since
// the shared backend is the thing being scored.
func (s *serviceImpl) ResetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) error {
	for _, rpcType := range domain.AllRPCTypes() {
		repKey := s.key(endpoint, rpcType)
		sh := s.shard(repKey)
		sh.mu.Lock()
		svcScores := sh.cache[serviceID]
		if svcScores == nil {
			svcScores = make(map[string]float64)
			sh.cache[serviceID] = svcScores
		}
		svcScores[repKey] = s.cfg.InitialScore
		sh.mu.Unlock()

		select {
		case s.writeCh <- writeOp{key: scoreKey(serviceID, repKey), score: s.cfg.InitialScore}:
		default:
		}
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
