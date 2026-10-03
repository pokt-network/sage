package reputation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	// ResetScore resets an endpoint's recorded scores to the initial value
	// across every RPC type. An operator resetting an endpoint means the
	// endpoint, not one of the protocols it happens to serve. Only keys that
	// exist are touched; ErrNoScore says none matched.
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
	key atomic.Pointer[KeyFn]
	// scoring is the additive delta per signal and the chronic-failure
	// penalty, swapped whole by Retune while signals are being recorded.
	scoring atomic.Pointer[scoring]
	// signalHook, when set, runs on every recorded signal. Wire time only.
	signalHook SignalHook
	// relativeGate turns on the pool-relative chronic penalty per service and
	// operatorGate the per-operator rate; chronic is what refreshBaselines last
	// computed from both. See penaltyFor and operator.go.
	relativeGate atomic.Pointer[func(domain.ServiceID) bool]
	operatorGate atomic.Pointer[func(domain.ServiceID) bool]
	chronic      atomic.Pointer[chronicView]
	// staleGate turns on the stale-share penalty per service; heads is the
	// per-party head-answer evidence it prices (staleshare.go). Not persisted:
	// it rebuilds in minutes.
	staleGate atomic.Pointer[func(domain.ServiceID) bool]
	heads     *opTracker
	// trustGate turns on the trust penalty per service; refusals counts each
	// (service, party)'s refused_recent verdicts (trust.go).
	trustGate atomic.Pointer[func(domain.ServiceID) bool]
	// peerParties reads a peer instance's priced parties (peerparties.go).
	peerParties atomic.Pointer[func(context.Context) (PartyPenalties, error)]
	refusals    *opTracker
	// ops is the per-operator evidence the chronic term actually reads: an
	// identity that does not rotate with the session draw (operator.go,
	// opstats.go). Persisted through Storage.
	ops *opTracker

	// In-memory score cache, striped by key hash.
	shards [scoreShards]scoreShard

	// pending is the write-behind: the latest state of each key changed
	// since the last flush, under pendMu. A map, not a queue, so it holds at
	// most one entry per key however many signals arrive (see enqueue).
	pendMu  sync.Mutex
	pending map[string]State
	// dropHook, when set, is told of each write that never reached storage.
	// Atomic because wire installs it after Start.
	dropHook atomic.Pointer[func(reason string)]
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewService creates a new reputation service with the given storage backend
// and configuration. Call Start() to begin processing async writes.
func NewService(storage Storage, timeline *Timeline, cfg ServiceConfig) *serviceImpl {
	if cfg.InitialScore == 0 {
		cfg.InitialScore = 100
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
		pending:  make(map[string]State),
		stopCh:   make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].cache = make(map[domain.ServiceID]map[string]State)
	}
	s.scoring.Store(newScoring(cfg.Impacts, cfg.Rate))
	s.ops = newOpTracker(DefaultOperatorHalfLife)
	s.heads = newOpTracker(staleShareHalfLife)
	s.heads.dirty = nil // never persisted, so nothing takes the marks
	s.refusals = newOpTracker(refusalHalfLife)
	s.refusals.dirty = nil
	s.setKeyFn(memoize(keyFnFor(cfg.KeyGranularity, nil)))
	selCfg := cfg.Selector
	if selCfg == (SelectorConfig{}) {
		selCfg = DefaultSelectorConfig()
	}
	s.selector = NewTieredSelector(selCfg, s.scoreForSelector)
	return s
}

// scoring is the half of a ServiceConfig Retune can change on a running
// service, normalized once.
type scoring struct {
	impacts SignalImpacts
	rate    RateConfig
	// lambda is rate.Lambda(), hoisted out of the per-signal path.
	lambda float64
}

func newScoring(impacts SignalImpacts, rate RateConfig) *scoring {
	rate = rate.Normalized()
	return &scoring{impacts: impacts.Normalized(), rate: rate, lambda: rate.Lambda()}
}

// Retune replaces the scoring constants of a running service: signal
// impacts, the chronic-rate curve, the tier thresholds and the operator
// cap's shares. Scores already recorded are kept and read under the new
// constants — the same thing a restart does, since state outlives the
// process in storage.
//
// InitialScore and KeyGranularity are not here: one decides which stored
// states count as untouched and the other what a key is, and changing either
// under live state would misread what is already recorded.
func (s *serviceImpl) Retune(impacts SignalImpacts, rate RateConfig, sel SelectorConfig, operatorCap OperatorCapConfig) {
	s.scoring.Store(newScoring(impacts, rate))
	s.selector.SetConfig(sel, operatorCap)
}

// effectiveFor is the score every reader sees: the additive term plus the
// chronic rate penalty (pool-relative when that is on, see penaltyFor),
// clamped. docs/scoring.md §7.3.
//
// The rate term demotes, it never removes: it may take a key down to
// MinThreshold, the bottom of probation, and no further. Only the additive
// term — the outage detector — takes a key out of selection. Without the
// floor, a pool whose every operator shares one timeout tail (mainnet sei,
// 2026-09-15: every operator at 1.2–1.8%, penalty -43 to -48) had keys with a
// working additive score of 30–50 read as 0, so the whole service fell into
// the pool-collapse fallback while the term ranked nobody above anybody.
func (s *serviceImpl) effectiveFor(serviceID domain.ServiceID, key string, st State) float64 {
	return s.scoreWith(serviceID, key, st, s.chronic.Load().partyPenalty(serviceID, key))
}

// scoreWith is effectiveFor with the party penalty given rather than looked
// up, so OwnScore can leave it out.
func (s *serviceImpl) scoreWith(serviceID domain.ServiceID, key string, st State, party float64) float64 {
	score := st.Score + s.penaltyFor(serviceID, key, st.Rate) + party
	if floor := min(st.Score, s.selector.cfg.Load().MinThreshold); score < floor {
		score = floor
	}
	return s.clamp(score)
}

// penaltyFor is the chronic penalty for a key's rate, measured from its
// pool's baseline when the relative term is on for the service: the best
// failure rate among the pool's well-attempted keys. A timeout tail every
// operator of a service shares is the chain's or the network's, not one
// operator's, and charging it to all of them ranked nobody above anybody
// while it pinned them to the floor (mainnet sei, 2026-09-15: seven keys at
// -44 to -49 on rates of 1.2-1.8%). A key worse than the best still pays the
// difference.
func (s *serviceImpl) penaltyFor(serviceID domain.ServiceID, key string, rate float64) float64 {
	v := s.chronic.Load()
	// Where the operator term is on, every key of an operator is charged the
	// operator's corrected rate: a rate per key measures how widely an operator
	// spread its traffic as much as how well it answered (operator.go).
	if v != nil && v.opOn[serviceID] {
		if r, ok := v.byKey[keyID{serviceID, key}]; ok {
			rate = r
		}
	}
	sc := s.scoring.Load()
	p := sc.rate.Penalty(rate)
	if p == 0 {
		return 0
	}
	if v != nil {
		if base, ok := v.baseline[poolID{serviceID, rpcOfKey(key)}]; ok {
			p = min(0, p-sc.rate.Penalty(base))
		}
	}
	return p
}

// poolID is one (service, RPC type) pool, the unit a baseline is taken over.
type poolID struct {
	svc domain.ServiceID
	rpc string
}

// rpcOfKey is the RPC type half of a reputation key ("<identity>|<rpc_type>").
func rpcOfKey(key string) string {
	if i := strings.LastIndexByte(key, '|'); i >= 0 {
		return key[i+1:]
	}
	return ""
}

const (
	// timeoutCoalesce is the window in which a key's transport timeouts cost
	// the additive score once. See RecordSignal.
	timeoutCoalesce = time.Second
	// reasonTransportTimeout is the heuristic's reason for an attempt that
	// timed out on the wire (heuristic.AnalyzeTransportError).
	reasonTransportTimeout = "transport_timeout"
	// reasonRefusedRecent is the heuristic's reason for a missing-state answer
	// about a block too recent to be gone (heuristic.RefusedRecent).
	reasonRefusedRecent = "refused_recent"
	// probeDefer is how recent traffic must be for it, not a probe, to have
	// the last word on a key's score. See RecordSignal.
	probeDefer = 10 * time.Minute
	// baselineMinAttempts is how much evidence a key needs before its rate
	// can be a pool's baseline: a fresh key's rate of 0 says nothing yet.
	baselineMinAttempts = 1000
	// baselineRefresh is how often the pool baselines are recomputed; a
	// relative_chronic flag change takes effect within it.
	baselineRefresh = 30 * time.Second
)

// RebaseAfterDrain restarts the keys of endpoints whose drain just ended at
// the bottom of probation (MinThreshold), when they sat above it. A drained
// endpoint receives neither traffic nor probes, so its score is frozen at what
// it was when benched — 100 for an operator benched for answering 408 — and at
// the drain's end it went straight back into tier 1 (mainnet sei, 2026-09-15
// 18:57Z). From probation it earns tier 1 again on traffic. It returns the
// number of keys lowered.
func (s *serviceImpl) RebaseAfterDrain(serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType) int {
	floor := s.selector.cfg.Load().MinThreshold
	seen := make(map[string]bool, len(endpoints))
	n := 0
	for _, ep := range endpoints {
		key := s.keyOf(ep, rpcType)
		if seen[key] {
			continue
		}
		seen[key] = true
		sh := s.shard(key)
		sh.mu.Lock()
		states := sh.cache[serviceID]
		if states == nil {
			states = make(map[string]State)
			sh.cache[serviceID] = states
		}
		st, ok := states[key]
		if !ok {
			st = State{Score: s.cfg.InitialScore}
		}
		if st.Score > floor {
			st.Score = floor
			states[key] = st
			n++
			s.enqueue(writeOp{key: scoreKey(serviceID, key), state: st})
		}
		sh.mu.Unlock()
	}
	return n
}

// Noter is the optional half of the reputation service that records a
// timeline-only event: an attempt deliberately not scored.
type Noter interface {
	RecordNote(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, reason, detail string)
}

var _ Noter = (*serviceImpl)(nil)

// RecordNote adds a timeline event for an attempt that is retried but not
// scored — a node's own -32000 answer (server_error) — so which host said
// what stays answerable after the verdict stopped leaving a signal. It
// changes no score, rate or attempt count. Detail is capped at 200 bytes.
func (s *serviceImpl) RecordNote(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, reason, detail string) {
	if s.timeline == nil {
		return
	}
	key := s.keyOf(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	score := s.cfg.InitialScore
	if ok {
		score = s.effectiveFor(serviceID, key, st)
	}
	detail = truncateDetail(detail)
	s.timeline.Record(scoreKey(serviceID, key), TimelineEvent{
		Timestamp: time.Now(),
		Event:     "unscored",
		Reason:    reason,
		OldScore:  score,
		Score:     score,
		Detail:    "unscored: " + reason + ": " + detail,
	})
}

// truncateDetail bounds a timeline detail, like RecordNote does.
func truncateDetail(d string) string {
	if len(d) > 200 {
		return d[:200]
	}
	return d
}

// SetRelativeChronic turns on the pool-relative chronic penalty, per service,
// behind gate. Call at wire time; the gate is read on each baseline refresh.
func (s *serviceImpl) SetRelativeChronic(gate func(domain.ServiceID) bool) {
	s.relativeGate.Store(&gate)
}

// refreshBaselines recomputes everything the chronic term reads: each
// operator's corrected rate, the key-to-operator-rate map scoring charges from,
// and each pool's baseline — the best rate among its members, measured per
// operator where the operator term is on and per key otherwise. Off the relay
// path, every baselineRefresh, swapped in whole.
func (s *serviceImpl) refreshBaselines() {
	relative := gateOf(&s.relativeGate)
	operator := gateOf(&s.operatorGate)

	// One walk of the cache collects both bases: per-key states for the pool
	// baseline, and the attempt-weighted sum per operator for the operator rate.
	type keyState struct {
		id   keyID
		pool poolID
		op   opID
		rate float64
		// wellAttempted is whether this key alone carries enough evidence to
		// set a pool baseline. Every key is charged its operator's rate; only
		// these vote on what the pool's best rate is.
		wellAttempted bool
	}
	var keys []keyState
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for svc, states := range sh.cache {
			for key, st := range states {
				if st.Score == 0 {
					continue
				}
				rpc := rpcOfKey(key)
				ks := keyState{
					id:            keyID{svc, key},
					pool:          poolID{svc, rpc},
					op:            opID{svc, operatorOfKey(key), rpc},
					rate:          st.Rate,
					wellAttempted: st.Attempts >= baselineMinAttempts,
				}
				keys = append(keys, ks)
			}
		}
		sh.mu.RUnlock()
	}

	// The operator rates come from the tracker, not from these keys: a key
	// lives one session and an operator does not (opstats.go).
	now := time.Now()
	stats := s.ops.snapshot(now)
	byOp := make(map[opID]OperatorRateView, len(stats))
	v := chronicView{
		byKey:    map[keyID]float64{},
		baseline: map[poolID]float64{},
		opOn:     map[domain.ServiceID]bool{},
	}
	for id, st := range stats {
		// WebSocket evidence recorded before it was kept out (RecordSignal)
		// lives on in storage for hours; it is not charged either.
		if id.rpc == string(domain.RPCTypeWebSocket) {
			continue
		}
		if rate := st.RateAt(now); rate > 0 {
			byOp[id] = OperatorRateView{Rate: rate, Attempts: uint64(st.Attempts)}
		}
	}
	// A service is measured in one basis or the other, never a mix: charging
	// one key an operator rate and its pool-mate a key rate would compare two
	// different measurements through the baseline.
	for _, ks := range keys {
		if operator != nil && operator(ks.id.svc) {
			v.opOn[ks.id.svc] = true
		}
	}
	type acc struct {
		min float64
		n   int
	}
	pools := map[poolID]*acc{}
	seen := map[opID]bool{}
	for _, ks := range keys {
		rate := ks.rate
		if v.opOn[ks.id.svc] {
			r, ok := byOp[ks.op]
			if !ok {
				continue
			}
			// Every key of the operator is charged the operator's rate, however
			// little traffic that key itself has seen — diluting a rate across
			// keys is the thing this measures around.
			rate = r.Rate
			v.byKey[ks.id] = r.Rate
			if seen[ks.op] || r.Attempts < baselineMinAttempts {
				continue // one vote per operator, and only a well-evidenced one
			}
			seen[ks.op] = true
		} else if !ks.wellAttempted {
			continue
		}
		if a := pools[ks.pool]; a == nil {
			pools[ks.pool] = &acc{min: rate, n: 1}
		} else {
			a.min = min(a.min, rate)
			a.n++
		}
	}
	// The stale-share term, charged to every key of a priced party.
	var prev []PartyStale
	if old := s.chronic.Load(); old != nil {
		prev = old.stale
	}
	v.stale = partyStale(s.heads.snapshot(now), gateOf(&s.staleGate), prev, now)
	var prevTrust []PartyTrust
	if old := s.chronic.Load(); old != nil {
		prevTrust = old.trust
	}
	v.trust = partyTrust(v.stale, s.refusals.snapshot(now), prevTrust, now, gateOf(&s.staleGate))
	if peer := s.readPeerParties(); len(peer.Stale)+len(peer.Trust) > 0 {
		served := map[domain.ServiceID]bool{}
		for _, ks := range keys {
			served[ks.id.svc] = true
		}
		v.stale, v.trust = mergePeerParties(v.stale, v.trust, peer, func(svc domain.ServiceID) bool { return served[svc] }, gateOf(&s.staleGate), now)
	}
	v.stalePen = map[opID]float64{}
	for _, p := range v.stale {
		if p.Penalty < 0 {
			v.stalePen[opID{svc: p.ServiceID, op: p.Party}] = p.Penalty
		}
	}
	v.trustPen = map[string]float64{}
	for _, t := range v.trust {
		if t.Penalty < 0 {
			v.trustPen[t.Party] = t.Penalty
		}
	}
	// Party lookups only matter while some party is charged.
	if len(v.stalePen)+len(v.trustPen) > 0 {
		// The flag per service is read here, once a refresh, for every
		// service with a key; a service with none yet falls back to the
		// gate on lookup (the penalty reaches a party's first key there).
		v.trustGate = gateOf(&s.trustGate)
		v.trustOn = map[domain.ServiceID]bool{}
		v.keyParty = make(map[string]string, len(keys))
		for _, ks := range keys {
			v.keyParty[ks.id.key] = domain.PartyOfOperator(ks.op.op)
			if _, seen := v.trustOn[ks.id.svc]; !seen && v.trustGate != nil {
				v.trustOn[ks.id.svc] = v.trustGate(ks.id.svc)
			}
		}
	}
	s.storePartyPenalties(&v)
	for id, a := range pools {
		// The relative term is what a baseline is for; without it a key is
		// charged from zero, as it was before pool-relative scoring.
		if a.n >= 2 && relative != nil && relative(id.svc) {
			v.baseline[id] = a.min
		}
	}
	s.chronic.Store(&v)
}

// gateOf reads a per-service gate pointer, nil when unset.
func gateOf(p *atomic.Pointer[func(domain.ServiceID) bool]) func(domain.ServiceID) bool {
	gp := p.Load()
	if gp == nil {
		return nil
	}
	return *gp
}

// latencyAlpha is the traffic-latency EWMA step. Reporting only.
const latencyAlpha = 0.05

// SignalHook is told about every recorded signal: which service, RPC type and
// endpoint it was charged to, its type, and whether a probe produced it.
type SignalHook func(serviceID domain.ServiceID, rpcType domain.RPCType, endpoint domain.EndpointAddr, signal Signal)

// SetSignalHook registers a callback run on every recorded signal, after the
// state is updated. Wire time only; used for the attempts counter and the
// auto-drain engine.
func (s *serviceImpl) SetSignalHook(fn SignalHook) {
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
	key := s.keyOf(ep, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		// A key with no state yet still carries its party's penalties: a
		// party's hosts rotate in fresh every session, and each started at
		// the initial score, uncharged, until its first signal.
		st = State{Score: s.cfg.InitialScore}
	}
	return s.effectiveFor(serviceID, key, st), true
}

// latencyForSelector is the LatencyFn the selector's tie-break reads: the
// per-key traffic latency EWMA, in milliseconds, when one exists.
func (s *serviceImpl) latencyForSelector(_ context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (float64, bool) {
	key := s.keyOf(ep, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok || st.LatencyMS <= 0 {
		return 0, false
	}
	return st.LatencyMS, true
}

// SetLatencyTieBreak enables the selector's latency tie-break inside the
// winning tier, gated per relay. Call at wire time.
func (s *serviceImpl) SetLatencyTieBreak(gate func(context.Context, domain.ServiceID) bool) {
	s.selector.SetLatencyTieBreak(s.latencyForSelector, gate)
}

// SetCollapseHook registers a callback fired whenever the selector's
// pool-collapse guard has to serve an endpoint scoring below the minimum
// threshold because no endpoint cleared it. Call at wire time.
func (s *serviceImpl) SetCollapseHook(fn CollapseHook) {
	s.selector.SetCollapseHook(fn)
}

// SetOperatorCap enables the per-operator concentration cap on both selection
// paths, gated per relay by gate. Call at wire time.
func (s *serviceImpl) SetOperatorCap(cfg OperatorCapConfig, gate func(context.Context, domain.ServiceID) bool) {
	s.selector.SetOperatorCap(cfg, gate)
}

// Start begins the background goroutine that flushes writes to storage.
func (s *serviceImpl) Start() {
	s.wg.Add(2)
	safego.Go(nil, "reputation.drain", s.drainWrites)
	safego.Go(nil, "reputation.baselines", func() {
		defer s.wg.Done()
		t := time.NewTicker(baselineRefresh)
		defer t.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-t.C:
				safego.Run(nil, "reputation.baselines.tick", s.refreshBaselines)
			}
		}
	})
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
	rate := s.scoring.Load().rate
	for k, v := range svcStates {
		if v.Score == s.cfg.InitialScore && rate.Penalty(v.Rate) == 0 {
			delete(svcStates, k)
		}
	}
}

// RecordSignal applies a signal's impact to the endpoint's score.
func (s *serviceImpl) RecordSignal(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, signal Signal) error {
	// One load for the whole signal: a Retune landing halfway through must
	// not score it with the old impact and the new rate.
	sc := s.scoring.Load()
	impact := sc.impacts.Impact(signal.Type)

	repKey := s.keyOf(endpoint, rpcType)
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
	ts := signal.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	// A probe is how a benched endpoint earns its way back; it is not evidence
	// against what traffic is saying right now. A probe success on a key that
	// served traffic within probeDefer changes nothing: on mainnet sei
	// (2026-09-15) one operator passed eth_blockNumber every cycle while it
	// answered 408 to real calls, and the probes' +5 cancelled the 408s' -5,
	// holding it at 100 in tier 1. Probe failures still count.
	//
	// Only for a key that is selectable (additive at or above MinThreshold):
	// a benched key still climbs back to probation on probes. Without that
	// bound, the collapse fallback's occasional pick kept a floored key's
	// traffic "recent" forever and its probes never counted — mainnet
	// solana's floored keys sat at 0 answering most of what they were sent.
	// Probes bring a key back into probation; traffic takes it from there.
	deferred := signal.Probe && signal.Type == SignalSuccess &&
		st.Score >= s.selector.cfg.Load().MinThreshold &&
		st.LastTraffic > 0 && ts.Unix()-st.LastTraffic < int64(probeDefer/time.Second)
	prev := st
	// Timeouts that land together are one event: the requests in flight when
	// a backend stalls all time out at once. Charged one by one, six of them
	// inside a second took mainnet solana's second operator from 100 to 0
	// (2026-09-29), and it healed in tens of seconds — its share saw-toothed
	// with every stall. One hit per timeoutCoalesce: a host that keeps timing
	// out still pays every window and floors from 100 in about 20 seconds.
	// The rate below still counts every one.
	timeout := signal.Type == SignalMajorError && signal.Reason == reasonTransportTimeout
	coalesced := timeout && prev.TimeoutHitAt != 0 && ts.UnixMilli()-prev.TimeoutHitAt < timeoutCoalesce.Milliseconds()
	if !deferred && !coalesced {
		st.Score = s.clamp(st.Score + impact)
		if timeout {
			st.TimeoutHitAt = ts.UnixMilli()
		}
	}
	// An endpoint the additive term has already floored is in an outage, not
	// exhibiting a rate; letting a day of probes against a dead host drive the
	// chronic term to its cap would cost weeks of probe-only recovery (final
	// review 2026-08-27). One fact, one power — docs/scoring.md §3 principle 3.
	//
	// The test is on the score BEFORE this signal: the attempt that floors the
	// key still feeds the rate, and only what happens to an already-floored key
	// is discounted.
	//
	// Retries and hedges score the key but do not move its rate, as they do
	// not move its operator's (Signal.Leftover). Which relays reach them was
	// decided by another host's failure: on mainnet sei (2026-10-01) one
	// owner refused heavy calls with an unscored -32000, the retry sent each
	// of them to the other operator, whose busy node failed them, and that
	// operator's key rate read 2.46% against 0.14% on its own first attempts
	// — a -49 penalty that took its first attempts from 10/s to 0.1/s.
	if rpcType == domain.RPCTypeWebSocket {
		st.Rate = decayWSRate(st.Rate, prev.UpdatedAt, ts)
	}
	if sc.rate.Enabled() && prev.Score != 0 && !deferred && !signal.Leftover {
		st.Rate += sc.lambda * (FailureWeight(signal.Type) - st.Rate)
	}
	st.Attempts++
	// When the key last heard anything, probes included: what "active
	// recently" means to the per-operator key count. The write-behind stamps
	// it again on the way to storage.
	st.UpdatedAt = ts.Unix()
	if !signal.Probe {
		st.TrafficAttempts++
		st.LastTraffic = ts.Unix()
		// Successes only: the EWMA now steers selection (latency tie-break),
		// and a host that fails fast must not read as a fast host. On the
		// 2026-09-14 canary the first hour of the tie-break fed every
		// signal in and raised 500s on robinhood, solana and poly.
		if signal.Type == SignalSuccess && signal.Latency > 0 {
			ms := float64(signal.Latency) / float64(time.Millisecond)
			if st.LatencyMS == 0 {
				st.LatencyMS = ms
			} else {
				st.LatencyMS += latencyAlpha * (ms - st.LatencyMS)
			}
		}
	}
	svcStates[repKey] = st
	newScore := s.effectiveFor(serviceID, repKey, st)
	sh.mu.Unlock()

	// The same evidence, charged to the operator instead of the key. Outside
	// the shard lock: the tracker has its own, and nesting them would put two
	// mutexes on the relay path where one will do.
	//
	// Deliberately NOT gated on the key's score, unlike the per-key rate
	// above. That gate exists so a day of probes against a dead host cannot
	// drive a key's chronic term to its cap, which would cost weeks of
	// probe-only recovery — an attempt-decayed term has no other way back. It
	// does not apply here: these counters decay on a clock, so evidence ages
	// out on its own, and the traffic a floored key still receives is exactly
	// what this measures. The pool-collapse fallback keeps feeding floored
	// keys, and dropping those attempts would make the operator the fallback
	// is feeding look better the worse it got (mainnet sei, 2026-09-16).
	// Those fallback picks are first attempts, so they still count here.
	//
	// Retries and hedges do not (Signal.Leftover). An operator already
	// demoted gets mostly those — the relays another host just failed or was
	// slow on — so counting them held its rate up by the very demotion it
	// caused (mainnet sei and opbnb, 2026-09-28). Probation attempts are
	// first attempts on a share of relays, a fair sample, and count.
	//
	// WebSocket does not either. Its attempts are connections and probes, a
	// denominator of hundreds against which the probe failures every operator
	// shares read as 2-11%: on mainnet (2026-09-30) that charged the in-sync
	// operators -50 to -70 on base, blast, opbnb and zksync-era while their
	// JSON-RPC rates were near zero. WebSocket keys keep their own rate.
	if sc.rate.Enabled() && !deferred && !signal.Leftover && rpcType != domain.RPCTypeWebSocket {
		if op := endpoint.Operator(); op != "" {
			s.ops.record(opID{serviceID, op, string(rpcType)}, FailureWeight(signal.Type), ts)
		}
	}
	// A refusal worded as a prune is trust evidence against the party
	// (trust.go), counted outside the shard lock like the operator counters.
	if signal.Reason == reasonRefusedRecent && !signal.Probe {
		if party := endpoint.Party(); party != "" {
			s.refusals.record(opID{svc: serviceID, op: party}, 0, ts)
		}
	}

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
			OldScore:   s.effectiveFor(serviceID, repKey, prev),
			Score:      newScore,
			// A penalty's own explanation, rendered with the rest on read.
			Note: truncateDetail(signal.Detail),
		})
	}

	// Write-behind: the latest state of the key goes out on the next flush.
	s.enqueue(writeOp{key: key, state: st})

	if s.signalHook != nil {
		s.signalHook(serviceID, rpcType, endpoint, signal)
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
	first := s.keyOf(endpoints[0], rpcType)
	oneKey := true
	for _, ep := range endpoints[1:] {
		if s.keyOf(ep, rpcType) != first {
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
		k := s.keyOf(ep, rpcType)
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
	key := s.keyOf(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		st = State{Score: s.cfg.InitialScore}
	}
	return s.effectiveFor(serviceID, key, st), nil
}

// OwnScore implements OwnScorer: an endpoint's score without its party's
// penalties, for a decision about the endpoint itself (whether a recovery
// probe can still raise it).
func (s *serviceImpl) OwnScore(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) float64 {
	key := s.keyOf(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		st = State{Score: s.cfg.InitialScore}
	}
	return s.scoreWith(serviceID, key, st, 0)
}

// OwnScorer is implemented by a reputation service that can report an
// endpoint's score without its party's stale-share or trust penalty. A party
// penalty holds whatever one endpoint does, so a key carrying one can never
// read as fully healthy: WebSocket recovery probes, sent to every key below
// full, would otherwise probe a distrusted party's every key at every slot
// for the whole day it is held, and could not change its score.
type OwnScorer interface {
	OwnScore(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) float64
}

// GetScoresSince is GetScores restricted to keys that received a signal at or
// after since. The score cache never forgets a key, so without this a count of
// keys grows with every URL a pod has ever scored.
func (s *serviceImpl) GetScoresSince(_ context.Context, serviceID domain.ServiceID, since time.Time) map[string]float64 {
	cut := since.Unix()
	result := make(map[string]float64)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, st := range sh.cache[serviceID] {
			if st.UpdatedAt >= cut {
				result[key] = s.effectiveFor(serviceID, key, st)
			}
		}
		sh.mu.RUnlock()
	}
	return result
}

// ScoreOf is an endpoint's effective score for an RPC type, and whether one
// has been recorded: an unscored endpoint reports false, not InitialScore.
func (s *serviceImpl) ScoreOf(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) (float64, bool) {
	key := s.keyOf(endpoint, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return s.effectiveFor(serviceID, key, st), true
}

// GetScores returns all cached scores for the given service, keyed by
// reputation key at the configured granularity.
func (s *serviceImpl) GetScores(_ context.Context, serviceID domain.ServiceID) (map[string]float64, error) {
	result := make(map[string]float64)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, st := range sh.cache[serviceID] {
			result[key] = s.effectiveFor(serviceID, key, st)
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
		view := StateView{
			Score: s.effectiveFor(serviceID, key, st), Additive: st.Score, Rate: st.Rate,
			Penalty: s.penaltyFor(serviceID, key, st.Rate), Attempts: st.Attempts,
			TrafficAttempts: st.TrafficAttempts, ProbeOnly: st.TrafficAttempts == 0,
			LatencyMS: st.LatencyMS,
		}
		view.StalePenalty, view.TrustPenalty = s.chronic.Load().partyPenalties(serviceID, key)
		// The rate a young key shows and the rate its operator is charged are
		// different numbers; a reader comparing keys needs both (operator.go).
		if r, ok := s.OperatorRate(serviceID, domain.RPCType(rpcOfKey(key)), operatorOfKey(key)); ok {
			view.OperatorRate = r.Rate
		}
		out[key] = view
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
		if operator, _, ok := cappedPick(*s.selector.operatorCap.Load(), candidates, nil); ok {
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

// ProbationChecker is the optional half of Service that says whether an
// endpoint currently sits in the probation band (MinThreshold up to the
// probation threshold) for an RPC type. SelectEndpoint asks it so a probation
// first try can be held to a short deadline: a probation pick is there to be
// measured, and a host demoted for timing out would otherwise spend the
// client's budget timing out again before Retry or Hedge reaches a healthy one.
type ProbationChecker interface {
	OnProbation(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool
}

var _ ProbationChecker = (*serviceImpl)(nil)

// OnProbation implements ProbationChecker.
func (s *serviceImpl) OnProbation(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool {
	tier, _ := s.selector.classify(ctx, serviceID, endpoint, rpcType)
	return tier == probationIdx
}

// OperatorResetter is the optional half of Service the admin operator-reset
// route asks for: forget an operator's failure evidence in one service, every
// RPC type, and report how many counters that touched.
type OperatorResetter interface {
	ResetOperator(serviceID domain.ServiceID, operator string) int
}

var _ OperatorResetter = (*serviceImpl)(nil)

// ResetOperator implements OperatorResetter. Per replica: the counters live in
// memory and only the leader writes them to storage, so an operator clears
// them on every pod. The chronic view picks the change up at its next
// refresh.
func (s *serviceImpl) ResetOperator(serviceID domain.ServiceID, operator string) int {
	return s.ops.reset(serviceID, operator, time.Now())
}

// ErrNoScore is returned by a reset that matched no recorded score. Nothing
// is created for an unmatched target: until 2026-09-14 a reset named by the
// key string an operator had copied from the listing ("https://host|rest")
// was pushed through the key function once per RPC type and left four
// phantom keys ("https://host|rest|json_rpc", …) at the initial score.
var ErrNoScore = errors.New("no recorded score matches the target")

// KeyResetter is the optional half of Service the admin reset route prefers:
// the same reset as ResetScore, reporting which keys it touched.
type KeyResetter interface {
	ResetMatching(ctx context.Context, serviceID domain.ServiceID, target string) ([]string, error)
}

// ResetScore resets every recorded score the endpoint reaches (see
// resetTargets). At a coarser granularity than per-endpoint this necessarily
// resets every endpoint sharing that key — resetting one supplier on a shared
// backend cannot mean anything else, since the shared backend is the thing
// being scored.
func (s *serviceImpl) ResetScore(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) error {
	_, err := s.ResetMatching(ctx, serviceID, string(endpoint))
	return err
}

// ResetMatching implements KeyResetter: every recorded key of the service
// that resetTargets says target names goes back to the initial score, in
// the cache and, forced past the leader gate, in storage. The keys are
// returned sorted; ErrNoScore when there were none.
func (s *serviceImpl) ResetMatching(_ context.Context, serviceID domain.ServiceID, target string) ([]string, error) {
	var reset []string
	dropped := false
	fresh := State{Score: s.cfg.InitialScore}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for key := range sh.cache[serviceID] {
			if !resetTargets(key, target) {
				continue
			}
			sh.cache[serviceID][key] = fresh
			reset = append(reset, key)
		}
		sh.mu.Unlock()
	}
	if len(reset) == 0 {
		return nil, fmt.Errorf("%w: %q on %s", ErrNoScore, target, serviceID)
	}
	// Written after the shard locks are released: a forced write is a storage
	// round trip, and relays on those shards must not wait for it.
	for _, key := range reset {
		if !s.enqueue(writeOp{key: scoreKey(serviceID, key), state: fresh, force: true}) {
			dropped = true
		}
	}
	sort.Strings(reset)
	if dropped {
		// Said, not swallowed: the local cache is reset either way, but the
		// other replicas learn of it through storage.
		return reset, fmt.Errorf("reset of %s applied locally, but a storage write failed; other replicas may keep the old score", target)
	}
	return reset, nil
}

// sameURL compares two identities ignoring one trailing slash.
func sameURL(a, b string) bool {
	return strings.TrimSuffix(a, "/") == strings.TrimSuffix(b, "/")
}

// resetTargets reports whether a reset naming target reaches key. A key is
// "<identity>|<rpc type>", the identity being the dialed URL at the default
// granularity, a host or a supplier address at the coarser ones, or the
// whole endpoint address at per-endpoint. target may be:
//   - the key itself, as the listing shows it;
//   - the identity ("https://node.example.org");
//   - the identity's host ("node.example.org"), with or without a port;
//   - an endpoint address ("pokt1abc-https://node.example.org"), matched
//     by its URL, its host, or its supplier.
//
// An RPC type in the target ("…|rest") is honoured through the exact form
// only, so a listing key resets one face and a URL resets all of them.
func resetTargets(key, target string) bool {
	if target == "" {
		return false
	}
	if key == target {
		return true
	}
	ident := key
	if i := strings.LastIndexByte(key, '|'); i >= 0 {
		ident = key[:i]
	}
	// A trailing slash is not part of what a URL names: the key holds the
	// dialed URL as the supplier staked it, and "https://host/" and
	// "https://host" are the same backend. Ops reset one operator's sei hosts
	// by URL on 2026-09-14 and matched none of the ones staked with a slash.
	if sameURL(ident, target) {
		return true
	}
	host := hostOf(ident)
	if host != "" && host == hostOf(target) && !strings.ContainsAny(target, "/|") {
		return true
	}
	// An endpoint address: "<supplier>-<url>". A bare URL also contains a
	// dash on occasion (eu-s-01…), so only treat the target as an address
	// when the dash sits before the scheme, or there is no scheme at all.
	scheme := strings.Index(target, "://")
	if scheme >= 0 && !strings.Contains(target[:scheme], "-") {
		return false
	}
	ep := domain.EndpointAddr(target)
	if url, err := ep.URL(); err == nil {
		if sameURL(url, ident) || (host != "" && hostOf(url) == host) {
			return true
		}
	}
	return ep.Supplier() == ident
}

// hostOf is the host of a URL or a bare host, scheme, path and port removed.
func hostOf(s string) string {
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexByte(s, '/'); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	return s
}

// Vouched reports whether an endpoint has a recorded score for the given RPC
// type, and that score clears the selector's probation threshold. It reads
// the cache directly rather than through scoreForSelector, which substitutes
// InitialScore for an unseen endpoint — the exact case Vouched must say no
// to: right after boot, before the first health-check cycle, every dead host
// still carries the initial score, and a method block diverting traffic must
// not treat that as a vouch.
func (s *serviceImpl) Vouched(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool {
	score, ok := s.ScoreOf(serviceID, endpoint, rpcType)
	return ok && score >= s.selector.cfg.Load().ProbationThreshold
}

// RuledOut reports whether an endpoint has a recorded score for the RPC type
// below the selector's MinThreshold: ranked out, not merely unproven. An
// endpoint with no score is not ruled out.
func (s *serviceImpl) RuledOut(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool {
	score, ok := s.ScoreOf(serviceID, endpoint, rpcType)
	return ok && score < s.selector.cfg.Load().MinThreshold
}

// maxScore is the upper bound for scores.
const maxScore = 100

// clamp constrains a score to [0, maxScore].
func (s *serviceImpl) clamp(score float64) float64 {
	return max(0, min(score, maxScore))
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
	// Operator evidence is written on its own cadence rather than per signal:
	// it is one row per (service, operator, RPC type), so a flush is tens of
	// writes, not one per relay.
	flush := time.NewTicker(operatorFlushInterval)
	defer flush.Stop()
	tick := time.NewTicker(writeFlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			s.flushPending()
		case now := <-sweep:
			// Errors are dropped like write errors are: storage is write-behind
			// that nothing reads back, and a sweep that failed runs again next
			// tick. safego.Run keeps one bad sweep from stopping the drain.
			safego.Run(nil, "reputation.sweep", func() {
				_, _ = sweeper.DeleteStale(context.Background(), now.Add(-s.cfg.StateIdleTTL))
			})
		case now := <-flush.C:
			safego.Run(nil, "reputation.opstats", func() { s.flushOperatorStats(now) })
		case <-s.stopCh:
			// What is pending goes out in one pass: a round trip or two, not
			// a 10s shutdown budget spent on writes.
			s.flushPending()
			return
		}
	}
}

// writeFlushInterval paces the write-behind. A second's worth of changes per
// key is one write; a hard kill loses at most that much, which the 1h idle
// TTL and the next signal both absorb.
const writeFlushInterval = time.Second

// operatorFlushInterval paces the operator write-behind. Losing at most this
// much evidence to a hard kill is acceptable; the counters decay over hours.
const operatorFlushInterval = 15 * time.Second

// flushOperatorStats writes the operator counters that changed since the last
// flush. Errors are dropped like every other write-behind error: the dirty
// mark is cleared when the value is taken, so a failed row waits for its next
// change, and the counters decay over hours.
func (s *serviceImpl) flushOperatorStats(now time.Time) {
	for id, st := range s.ops.takeDirty(now) {
		_ = s.storage.SetOperatorStat(context.Background(),
			OperatorField(id.svc, id.op, domain.RPCType(id.rpc)), st)
	}
}

// Write-behind drop reasons, the reason label of
// sage_reputation_writes_dropped_total. A follower's write that
// LeaderOnlyStorage discards is not a drop: only the leader writes, by design.
// (queue_full, a write the old fixed queue had no room for, ended with the
// per-key write-behind on 2026-10-02.)
const (
	// WriteDropStorageError is a write storage refused, a Redis error.
	WriteDropStorageError = "storage_error"
)

// SetWriteDropHook installs fn, called once per write-behind write that never
// reached storage, with its reason. Nil clears it.
func (s *serviceImpl) SetWriteDropHook(fn func(reason string)) {
	if fn == nil {
		s.dropHook.Store(nil)
		return
	}
	s.dropHook.Store(&fn)
}

// WriteQueueDepth is how many keys are waiting for the next write-behind
// flush.
func (s *serviceImpl) WriteQueueDepth() int {
	s.pendMu.Lock()
	defer s.pendMu.Unlock()
	return len(s.pending)
}

// enqueue hands a write to the write-behind without blocking the caller,
// reporting whether it will reach storage.
//
// It keeps the latest state per key until the next flush rather than queueing
// every change. A fixed queue of 4,096 writes, one per signal, filled under
// bursts on the leader and dropped 4,315 writes in a day (mainnet,
// 2026-10-01), though only the last state of each key ever matters. A map
// holds one entry per key changed in a second, whatever the signal rate.
//
// On a follower nothing is kept: storage discards a follower's writes
// (LeaderOnlyStorage), so holding them only spent memory and a flush. A
// forced write (an operator's reset) is a decision about the fleet's view and
// goes straight through, past the gate.
func (s *serviceImpl) enqueue(op writeOp) bool {
	if op.force {
		return s.write(op)
	}
	if g, ok := s.storage.(leaderGate); ok && !g.IsLeader() {
		return true
	}
	s.pendMu.Lock()
	s.pending[op.key] = op.state
	s.pendMu.Unlock()
	return true
}

// leaderGate is a storage that writes only on the leader (LeaderOnlyStorage).
type leaderGate interface {
	IsLeader() bool
}

// flushPending takes everything pending and writes it, in batches of at most
// maxWriteBatch where storage can batch.
func (s *serviceImpl) flushPending() {
	s.pendMu.Lock()
	if len(s.pending) == 0 {
		s.pendMu.Unlock()
		return
	}
	batch := s.pending
	s.pending = make(map[string]State, len(batch))
	s.pendMu.Unlock()

	now := time.Now().Unix()
	chunk := make(map[string]State, min(len(batch), maxWriteBatch))
	for key, st := range batch {
		st.UpdatedAt = now
		chunk[key] = st
		if len(chunk) == maxWriteBatch {
			s.flush(chunk)
			clear(chunk)
		}
	}
	s.flush(chunk)
}

// dropped reports one lost write to the hook, if any.
func (s *serviceImpl) dropped(reason string) {
	if fn := s.dropHook.Load(); fn != nil {
		(*fn)(reason)
	}
}

// maxWriteBatch bounds one batched write: one HSET of 4,096 fields is around
// half a megabyte, one round trip.
const maxWriteBatch = 4096

// flush hands the batch to storage in one operation where storage can, and one
// per key where it cannot. A failed batch is counted per key it carried: the
// drop counter says how many signals storage never heard, not how many calls
// failed.
func (s *serviceImpl) flush(batch map[string]State) {
	if len(batch) == 0 {
		return
	}
	if bw, ok := s.storage.(BatchWriter); ok {
		if err := bw.SetStates(context.Background(), batch); err != nil {
			for range batch {
				s.dropped(WriteDropStorageError)
			}
		}
		return
	}
	for key, st := range batch {
		if err := s.storage.SetState(context.Background(), key, st); err != nil {
			s.dropped(WriteDropStorageError)
		}
	}
}

// write stamps the state and hands it to storage. The stamp is what the
// sweep keys on; it is set here, at write time, rather than at enqueue, so
// it says when storage last heard about the key.
func (s *serviceImpl) write(op writeOp) bool {
	op.state.UpdatedAt = time.Now().Unix()
	var err error
	if f, ok := s.storage.(forcedWriter); ok && op.force {
		err = f.ForceSetState(context.Background(), op.key, op.state)
	} else {
		err = s.storage.SetState(context.Background(), op.key, op.state)
	}
	if err != nil {
		s.dropped(WriteDropStorageError)
		return false
	}
	return true
}

// forcedWriter is a storage that can be told to write regardless of its
// gate. LeaderOnlyStorage is the one.
type forcedWriter interface {
	ForceSetState(ctx context.Context, key string, st State) error
}

func (s *serviceImpl) setKeyFn(fn KeyFn) { s.key.Store(&fn) }

// keyOf is the reputation key for an endpoint's face, memoized.
func (s *serviceImpl) keyOf(ep domain.EndpointAddr, rpcType domain.RPCType) string {
	return (*s.key.Load())(ep, rpcType)
}

// SetURLResolver installs the resolver per-URL keys use and drops the key
// memo, so keys computed before it (none in normal wiring: Build installs
// it before the server listens) are recomputed. Existing scores stored under
// the old spelling of a split-host operator's key are not migrated; they
// age out, and the host that actually served the face starts at the
// initial score.
func (s *serviceImpl) SetURLResolver(fn URLResolverFn) {
	s.setKeyFn(memoize(keyFnFor(s.cfg.KeyGranularity, fn)))
}
