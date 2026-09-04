package reputation

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	// Vouched reports whether an endpoint has a recorded score for this RPC
	// type, and that score clears the selector's probation threshold. An
	// endpoint with no recorded score is not vouched: unknown passes a
	// filter, but a method block must not divert traffic onto hosts nothing
	// has measured yet — right after boot every dead host still carries the
	// initial score.
	Vouched(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool
}

// OnceRecorder is the optional extension a reputation service implements when
// it can collapse a fan-out of endpoint addresses to one signal per reputation
// key. It is not part of Service: only the health-check executor has a list of
// addresses that all describe the same observation, and every other caller
// scores the one endpoint that served the attempt.
//
// A caller that cannot type-assert its way to this interface must fall back to
// RecordSignal per address, which is what SAGE did before ruling F1.
type OnceRecorder interface {
	// RecordSignalOnce records signal once per DISTINCT reputation key among
	// endpoints. See serviceImpl.RecordSignalOnce for what that means at each
	// granularity.
	RecordSignalOnce(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType, signal Signal) error
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
	// Impacts is the additive score delta per signal type. A zero field takes
	// that type's default; see SignalImpacts.
	Impacts SignalImpacts
	// Rate parameterises the chronic-failure term. Zero fields take the
	// defaults; a negative HalfLifeAttempts turns the term off.
	Rate RateConfig
	// Selector holds the tier thresholds. The zero value — every field zero —
	// means DefaultSelectorConfig(); a partially set struct is used as-is, so
	// a caller that sets any field must set all of them (wire.go does).
	Selector SelectorConfig
	// StateIdleTTL is how long a key's entry in Storage outlives its last
	// write before the sweep deletes it. Zero means DefaultIdleTTL; negative
	// disables the sweep. Only matters when Storage implements StaleDeleter.
	StateIdleTTL time.Duration
	// StateSweepInterval is how often the write-behind goroutine runs the
	// sweep. Zero means defaultStateSweepInterval.
	StateSweepInterval time.Duration
}

// defaultStateSweepInterval paces the storage sweep. The sweep is one HSCAN
// over the hash on the leader; every few minutes is far more often than the
// TTL needs and cheap enough not to think about.
const defaultStateSweepInterval = 5 * time.Minute

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

// writeOp represents an asynchronous state write.
type writeOp struct {
	key   string
	state State
	// force writes through a leader-only storage gate. An operator's reset is
	// a decision about the fleet's view, not this replica's, and a follower
	// dropping it left the leader's next signal to restore the old score.
	force bool
}

// scoreShards stripes the in-memory score map so concurrent relays recording
// signals for different endpoints don't serialize on one process-wide mutex.
const scoreShards = 32

// scoreShard holds per-key state keyed by serviceID then reputation key.
// Nested maps (rather than a concatenated string key) keep the selector read
// path allocation-free: lookups never build a key string. The reputation key
// itself is a substring of the endpoint address, so deriving it allocates
// nothing either.
type scoreShard struct {
	mu    sync.RWMutex
	cache map[domain.ServiceID]map[string]State
}

// maxScoresPerServiceShard bounds one service's score map within one shard.
//
// At the default per-URL granularity this never binds: keys are backend URLs,
// a set the size of the real infrastructure. It exists for per-endpoint, where
// the key carries the supplier address — a staked registration that rotates
// every session, so the key set grows with the network rather than with SAGE's
// traffic, and this map is written on the relay path and never otherwise
// shrinks.
//
// 4096 per shard across 32 shards is ~131k keys per service, far above any
// real endpoint population and low enough to matter before a pod does.
const maxScoresPerServiceShard = 4096

// serviceImpl is the default implementation of Service.
type serviceImpl struct {
	cfg      ServiceConfig
	storage  Storage
	timeline *Timeline
	selector *TieredSelector
	// key maps an endpoint address to the identity its score lives under.
	key KeyFn
	// impacts and rate are the two halves of the score: the additive delta per
	// signal, and the chronic-failure penalty. Both normalized at construction.
	impacts SignalImpacts
	rate    RateConfig
	// lambda is rate.Lambda(), hoisted out of the per-signal path under lock.
	lambda float64
	// signalHook, when set, runs on every recorded signal. Wire time only.
	signalHook func(domain.ServiceID, domain.RPCType, SignalType, bool)

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
	if cfg.StateIdleTTL == 0 {
		cfg.StateIdleTTL = DefaultIdleTTL
	}
	if cfg.StateSweepInterval <= 0 {
		cfg.StateSweepInterval = defaultStateSweepInterval
	}
	s := &serviceImpl{
		cfg:      cfg,
		storage:  storage,
		timeline: timeline,
		impacts:  cfg.Impacts.Normalized(),
		rate:     cfg.Rate.Normalized(),
		writeCh:  make(chan writeOp, cfg.WriteQueueSize),
		stopCh:   make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].cache = make(map[domain.ServiceID]map[string]State)
	}
	s.lambda = s.rate.Lambda()
	s.key = memoize(keyFnFor(cfg.KeyGranularity))
	selCfg := cfg.Selector
	if selCfg == (SelectorConfig{}) {
		selCfg = DefaultSelectorConfig()
	}
	s.selector = NewTieredSelector(selCfg, s.scoreForSelector)
	return s
}

// effective is the score every reader sees: the additive term plus the
// chronic rate penalty, clamped. docs/scoring.md §7.3.
func (s *serviceImpl) effective(st State) float64 {
	return s.clamp(st.Score + s.rate.Penalty(st.Rate))
}

// latencyAlpha is the traffic-latency EWMA step. Reporting only.
const latencyAlpha = 0.05

// SetSignalHook registers a callback run on every recorded signal, after the
// state is updated. Wire time only; used for the attempts counter.
func (s *serviceImpl) SetSignalHook(fn func(domain.ServiceID, domain.RPCType, SignalType, bool)) {
	s.signalHook = fn
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
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		return s.cfg.InitialScore, true
	}
	return s.effective(st), true
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

// pruneUninformative drops the scores that say nothing, and only those.
//
// This cache is also the read path: a miss returns InitialScore without
// consulting storage. Dropping entries wholesale would therefore not reclaim
// memory so much as silently reset reputation, forgiving exactly the endpoints
// worth remembering. An entry sitting at InitialScore is the one case where
// that is not true — evicting it and re-reading it produce the same number, so
// it is free to drop.
//
// In practice that is most of the map: healthy endpoints clamp to the ceiling,
// which at the default configuration is InitialScore. If a service really is
// holding this many *penalized* keys, the entries stay and the map exceeds the
// bound — keeping a real penalty is worth more than the bytes, and a pool that
// size is its own alert.
//
// With the chronic term the test is on the *effective* score, not on the two
// raw fields: an entry goes only when evicting it and re-reading it produce the
// same number — a full additive score and a rate that carries no penalty. A
// rate above the onset does carry one, so a chronically-flaky endpoint sitting
// at the ceiling stays, which is exactly the key the rate term exists to catch.
//
// A rate *below* the onset is latent information — it would have grown into a
// penalty had the failures continued — and this is where we agree to forget it.
// Testing v.Rate == 0 instead would forget nothing: the EWMA decays towards
// zero but never reaches it, so one major error would pin a key for the life of
// the process and the bound below would stop bounding anything. What is lost
// with an evicted key is also the attempt counters, which are reporting-only —
// the key comes back at zero attempts even though it served traffic.
//
// Must be called with the shard locked.
func (s *serviceImpl) pruneUninformative(svcStates map[string]State) {
	for k, v := range svcStates {
		if v.Score == s.cfg.InitialScore && s.rate.Penalty(v.Rate) == 0 {
			delete(svcStates, k)
		}
	}
}

// RecordSignal applies a signal's impact to the endpoint's score.
func (s *serviceImpl) RecordSignal(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, signal Signal) error {
	impact := s.impacts.Impact(signal.Type)

	repKey := s.key(endpoint, rpcType)
	sh := s.shard(repKey)
	sh.mu.Lock()
	svcStates := sh.cache[serviceID]
	if svcStates == nil {
		svcStates = make(map[string]State)
		sh.cache[serviceID] = svcStates
	}
	st, ok := svcStates[repKey]
	if !ok {
		st = State{Score: s.cfg.InitialScore}
		if len(svcStates) >= maxScoresPerServiceShard {
			s.pruneUninformative(svcStates)
		}
	}
	prev := st
	st.Score = s.clamp(st.Score + impact)
	// An endpoint the additive term has already floored is in an outage, not
	// exhibiting a rate; letting a day of probes against a dead host drive the
	// chronic term to its cap would cost weeks of probe-only recovery (final
	// review 2026-08-27). One fact, one power — docs/scoring.md §3 principle 3.
	//
	// The test is on the score BEFORE this signal: the attempt that floors the
	// key still feeds the rate, and only what happens to an already-floored key
	// is discounted.
	if s.rate.Enabled() && prev.Score != 0 {
		st.Rate += s.lambda * (FailureWeight(signal.Type) - st.Rate)
	}
	st.Attempts++
	if !signal.Probe {
		st.TrafficAttempts++
		if signal.Latency > 0 {
			ms := float64(signal.Latency) / float64(time.Millisecond)
			if st.LatencyMS == 0 {
				st.LatencyMS = ms
			} else {
				st.LatencyMS += latencyAlpha * (ms - st.LatencyMS)
			}
		}
	}
	svcStates[repKey] = st
	newScore := s.effective(st)
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
			OldScore:   s.effective(prev),
			Score:      newScore,
		})
	}

	// Enqueue async write (non-blocking: drop if queue full).
	select {
	case s.writeCh <- writeOp{key: key, state: st}:
	default:
	}

	if s.signalHook != nil {
		s.signalHook(serviceID, rpcType, signal.Type, signal.Probe)
	}

	return nil
}

// The health-check executor reaches this method through OnceRecorder; pin the
// implementation here so a signature change breaks the build, not the fan-out.
var _ OnceRecorder = (*serviceImpl)(nil)

// RecordSignalOnce records signal once per DISTINCT reputation key among
// endpoints.
//
// One probe is one attempt, and what it is an attempt against is a key, not an
// address. At the default per-URL granularity a backend's N staked
// registrations collapse to one key, so one probe moves that key once however
// many suppliers front it — recording it N times would charge one observation
// N attempts, inflate the chronic term's denominator and multiply the additive
// delta by the registration count, which is a property of the stake table and
// not of the machine. At per-endpoint each registration is its own key and each
// one gets the attempt, because there the registration is the thing being
// scored (docs/scoring.md §3 principle 4).
//
// Errors from the individual records are collapsed to the first non-nil one;
// the loop always finishes, since a failure on one key says nothing about the
// next.
func (s *serviceImpl) RecordSignalOnce(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType, signal Signal) error {
	if len(endpoints) == 0 {
		return nil
	}
	// The common case is a set of siblings on one backend, which at per-URL is
	// a single key: compare the keys directly rather than building a set for
	// what is nearly always one entry. Keys are memoized, so this is a map
	// lookup and a string compare per sibling.
	first := s.key(endpoints[0], rpcType)
	oneKey := true
	for _, ep := range endpoints[1:] {
		if s.key(ep, rpcType) != first {
			oneKey = false
			break
		}
	}
	if oneKey {
		return s.RecordSignal(ctx, serviceID, endpoints[0], rpcType, signal)
	}

	seen := make(map[string]struct{}, len(endpoints))
	var firstErr error
	for _, ep := range endpoints {
		k := s.key(ep, rpcType)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if err := s.RecordSignal(ctx, serviceID, ep, rpcType, signal); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GetScore returns the cached score for the endpoint. If the endpoint has not
// been seen, the initial score is returned.
func (s *serviceImpl) GetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) (float64, error) {
	key := s.key(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		return s.cfg.InitialScore, nil
	}
	return s.effective(st), nil
}

// GetScores returns all cached scores for the given service, keyed by
// reputation key at the configured granularity.
func (s *serviceImpl) GetScores(_ context.Context, serviceID domain.ServiceID) (map[string]float64, error) {
	result := make(map[string]float64)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, st := range sh.cache[serviceID] {
			result[key] = s.effective(st)
		}
		sh.mu.RUnlock()
	}
	return result, nil
}

// The admin state listing reaches the service through StateLister; pin the
// implementation here so a signature change breaks the build, not the route.
var _ StateLister = (*serviceImpl)(nil)

// GetStates returns the full per-key state for a service, with the derived
// effective score and penalty. Admin read path; allocates.
func (s *serviceImpl) GetStates(_ context.Context, serviceID domain.ServiceID) (map[string]StateView, error) {
	// Copy the raw states out under the lock and derive afterwards: the two
	// logarithms per key in Penalty have no business running while relays are
	// blocked on this shard.
	states := make(map[string]State)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, st := range sh.cache[serviceID] {
			states[key] = st
		}
		sh.mu.RUnlock()
	}
	out := make(map[string]StateView, len(states))
	for key, st := range states {
		out[key] = StateView{
			Score: s.effective(st), Additive: st.Score, Rate: st.Rate,
			Penalty: s.rate.Penalty(st.Rate), Attempts: st.Attempts,
			TrafficAttempts: st.TrafficAttempts, ProbeOnly: st.TrafficAttempts == 0,
			LatencyMS: st.LatencyMS,
		}
	}
	return out, nil
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
	// The first element is the endpoint to try: the tier-cascade pick, or —
	// on the configured share of relays — a probation or tier-2 endpoint the
	// selector put in front of it so that it is measured by traffic. The
	// healthy pick behind it is what Retry reaches for when the first try
	// fails; SelectBest does not need to carry it.
	//
	// This used to return the LAST element, "the healthy pick", which made
	// probation.traffic_percent inert on the HTTP path from the day it was
	// wired: the selector prepended, nothing read the front, and a probation
	// endpoint earned its way back through health checks alone. The scoring
	// spec (docs/scoring.md §7.4, §7.7) assumes the share exists; now it does.
	return list[0]
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
		svcStates := sh.cache[serviceID]
		if svcStates == nil {
			svcStates = make(map[string]State)
			sh.cache[serviceID] = svcStates
		}
		fresh := State{Score: s.cfg.InitialScore}
		svcStates[repKey] = fresh
		sh.mu.Unlock()

		select {
		case s.writeCh <- writeOp{key: scoreKey(serviceID, repKey), state: fresh, force: true}:
		default:
			// Said, not swallowed: the local cache is reset either way, but
			// the other replicas learn of it through storage.
			return fmt.Errorf("reset of %s applied locally, but the storage write was dropped (write queue full); other replicas keep the old score", endpoint)
		}
	}
	return nil
}

// Vouched reports whether an endpoint has a recorded score for the given RPC
// type, and that score clears the selector's probation threshold. It reads
// the cache directly rather than through scoreForSelector, which substitutes
// InitialScore for an unseen endpoint — the exact case Vouched must say no
// to: right after boot, before the first health-check cycle, every dead host
// still carries the initial score, and a method block diverting traffic must
// not treat that as a vouch.
func (s *serviceImpl) Vouched(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool {
	key := s.key(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	return ok && s.effective(st) >= s.selector.cfg.ProbationThreshold
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
	sweeper, canSweep := s.storage.(StaleDeleter)
	canSweep = canSweep && s.cfg.StateIdleTTL > 0
	var sweep <-chan time.Time
	if canSweep {
		ticker := time.NewTicker(s.cfg.StateSweepInterval)
		defer ticker.Stop()
		sweep = ticker.C
	}
	for {
		select {
		case op := <-s.writeCh:
			s.write(op)
		case now := <-sweep:
			// Errors are dropped like write errors are: storage is write-behind
			// that nothing reads back, and a sweep that failed runs again next
			// tick. safego.Run keeps one bad sweep from stopping the drain.
			safego.Run(nil, "reputation.sweep", func() {
				_, _ = sweeper.DeleteStale(context.Background(), now.Add(-s.cfg.StateIdleTTL))
			})
		case <-s.stopCh:
			// Drain remaining writes.
			for {
				select {
				case op := <-s.writeCh:
					s.write(op)
				default:
					return
				}
			}
		}
	}
}

// write stamps the state and hands it to storage. The stamp is what the
// sweep keys on; it is set here, at write time, rather than at enqueue, so
// it says when storage last heard about the key.
func (s *serviceImpl) write(op writeOp) {
	op.state.UpdatedAt = time.Now().Unix()
	if op.force {
		if f, ok := s.storage.(forcedWriter); ok {
			_ = f.ForceSetState(context.Background(), op.key, op.state)
			return
		}
	}
	_ = s.storage.SetState(context.Background(), op.key, op.state)
}

// forcedWriter is a storage that can be told to write regardless of its
// gate. LeaderOnlyStorage is the one.
type forcedWriter interface {
	ForceSetState(ctx context.Context, key string, st State) error
}
