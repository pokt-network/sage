package reputation

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/pokt-network/sage/domain"
)

// SelectorConfig holds thresholds for tiered endpoint selection.
type SelectorConfig struct {
	// Tier1Threshold is the minimum score for tier 1 (best). Default: 80.
	Tier1Threshold float64
	// Tier2Threshold is the minimum score for tier 2 (good). Default: 50.
	Tier2Threshold float64
	// MinThreshold is the minimum score to be considered at all. Default: 10.
	MinThreshold float64
	// ProbationThreshold is the upper bound for probation routing. Endpoints with
	// scores between MinThreshold and ProbationThreshold are probation endpoints.
	// Default: 30.
	ProbationThreshold float64
	// ProbationPct is the percentage (0-100) of requests that include a
	// probation endpoint prepended to the healthy list. Default: 10.
	ProbationPct int
	// Tier2Pct is the percentage (0-100) of tier-1 selections that instead
	// try a tier-2 endpoint first, with the tier-1 pick behind it as the
	// retry fallback. Default: 5.
	//
	// Without it tier 2 sees no traffic while tier 1 has a member, and a
	// tier-2 endpoint is then measured by health-check probes alone: a good
	// host that took one critical waits a full probe cycle to earn its way
	// back, and a chronic violator parks at the tier boundary where its
	// failure rate stops being measured. docs/scoring.md §7.7 has the soak
	// that showed both. Probation's share is the same mechanism one tier
	// down.
	Tier2Pct int
}

// DefaultSelectorConfig returns a SelectorConfig with default thresholds.
func DefaultSelectorConfig() SelectorConfig {
	return SelectorConfig{
		Tier1Threshold:     80,
		Tier2Threshold:     50,
		MinThreshold:       10,
		ProbationThreshold: 30,
		ProbationPct:       10,
		Tier2Pct:           5,
	}
}

// ScoreFn looks up one endpoint's reputation score. The second return is
// false when the endpoint is unknown to the implementation (it is then
// treated as tier 3). Per-endpoint lookup — rather than returning a map for
// the whole list — keeps the per-relay selection path allocation-free.
type ScoreFn func(ctx context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (float64, bool)

// Tier indices used by classify. -1 means filtered out entirely.
const (
	tier1Idx = iota
	tier2Idx
	tier3Idx
	probationIdx
	numTiers
)

// LatencyFn reports an endpoint's traffic latency EWMA in milliseconds for a
// service and RPC type; ok is false when nothing has measured it.
type LatencyFn func(ctx context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (float64, bool)

// TieredSelector selects endpoints by cascading through reputation tiers.
// Tier 1 (best) is tried first; if empty, tier 2; then tier 3. Within each
// tier a random endpoint is chosen. Probation endpoints may be prepended.
type TieredSelector struct {
	// cfg and operatorCap are swapped whole by SetConfig while relays select,
	// so a reload never shows a selection half of the old thresholds and half
	// of the new.
	cfg    atomic.Pointer[SelectorConfig]
	scores ScoreFn

	// latency and latencyGate drive the tie-break inside the winning tier;
	// see SetLatencyTieBreak.
	latency     LatencyFn
	latencyGate func(context.Context, domain.ServiceID) bool

	// onCollapse, when set, is invoked once per selection in which every
	// endpoint scored below MinThreshold and the pool-collapse guard had to
	// serve a sub-threshold endpoint. See Select.
	onCollapse CollapseHook

	// operatorCap bounds any single operator's share of selections within the
	// winning tier. See concentration.go.
	operatorCap atomic.Pointer[OperatorCapConfig]
	// capGate decides per relay whether the cap applies. Nil means never — the
	// cap is opt-in at wire time, behind a feature flag, so it can be turned
	// off at runtime without a deploy.
	capGate func(context.Context, domain.ServiceID) bool
}

// NewTieredSelector creates a selector that uses the provided score lookup
// function to classify endpoints into tiers.
func NewTieredSelector(cfg SelectorConfig, scoreFn ScoreFn) *TieredSelector {
	s := &TieredSelector{scores: scoreFn}
	s.cfg.Store(&cfg)
	s.operatorCap.Store(&OperatorCapConfig{})
	return s
}

// SetConfig replaces the tier thresholds and the operator cap's shares on a
// running selector. The cap's gate is untouched: whether the cap applies is a
// feature flag, and only its numbers are config.
func (s *TieredSelector) SetConfig(cfg SelectorConfig, operatorCap OperatorCapConfig) {
	s.cfg.Store(&cfg)
	s.operatorCap.Store(&operatorCap)
}

// CollapseHook is told each time the pool-collapse guard serves sub-threshold
// endpoints: the service, the RPC type, and what it served. The served list
// is what lets a reader tell which operator the fallback is feeding.
type CollapseHook func(serviceID domain.ServiceID, rpcType domain.RPCType, served domain.EndpointAddrList)

// SetCollapseHook registers a callback invoked whenever the pool-collapse guard
// fires. Nil clears it. Not safe to call concurrently with selection; call it
// at wire time.
func (s *TieredSelector) SetCollapseHook(fn CollapseHook) {
	s.onCollapse = fn
}

// SetOperatorCap enables the per-operator concentration cap, gated per relay by
// gate (nil gate = never applied). Not safe to call concurrently with
// selection; call it at wire time.
func (s *TieredSelector) SetOperatorCap(cfg OperatorCapConfig, gate func(context.Context, domain.ServiceID) bool) {
	s.operatorCap.Store(&cfg)
	s.capGate = gate
}

// SetLatencyTieBreak installs the latency source for the tie-break inside
// the winning tier, gated per relay. Within tier 1 (and only there) an
// endpoint is picked with probability proportional to 1/latency, floored
// at latencyFloorMS so a fast host does not monopolise the tier; an
// endpoint nothing has measured is given the tier's mean, so a newcomer
// still gets traffic and is measured. The operator cap, when active, still
// chooses the operator; the tie-break then chooses within that operator.
//
// This is the one place latency touches selection. On the 2026-09-14
// canary SAGE's upstream p50 on osmosis was 0.074 s against PATH's 0.044 s
// for the same suppliers, because SAGE picked uniformly inside a tier while
// PATH's selection bands excluded slow hosts. Scores stay latency-blind
// (docs/scoring.md §7.2): a slow correct host keeps its score and its tier,
// it is just asked less often than a fast one.
func (s *TieredSelector) SetLatencyTieBreak(fn LatencyFn, gate func(context.Context, domain.ServiceID) bool) {
	s.latency = fn
	s.latencyGate = gate
}

// latencyFloorMS bounds the weight of a very fast host: below this, hosts
// are treated as equally fast.
const latencyFloorMS = 20

func (s *TieredSelector) tieBreakActive(ctx context.Context, serviceID domain.ServiceID) bool {
	return s.latency != nil && s.latencyGate != nil && s.latencyGate(ctx, serviceID)
}

// capActive reports whether the concentration cap should shape this selection.
func (s *TieredSelector) capActive(ctx context.Context, serviceID domain.ServiceID) bool {
	return s.capGate != nil && s.capGate(ctx, serviceID)
}

// classify maps an endpoint to its tier index, or -1 when it's below the
// minimum threshold and must be skipped. The endpoint's score is returned
// alongside so callers can rank the endpoints classify rejected — the
// pool-collapse guard needs the least-bad of them.
func (s *TieredSelector) classify(ctx context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (int, float64) {
	score, ok := s.scores(ctx, serviceID, ep, rpcType)
	if !ok {
		// Unknown endpoints default to tier 3 (they'll get a score after first relay).
		return tier3Idx, score
	}
	cfg := s.cfg.Load()
	switch {
	case score < cfg.MinThreshold:
		return -1, score // filtered out entirely
	case score < cfg.ProbationThreshold:
		return probationIdx, score
	case score >= cfg.Tier1Threshold:
		return tier1Idx, score
	case score >= cfg.Tier2Threshold:
		return tier2Idx, score
	default:
		return tier3Idx, score
	}
}

// Select returns an ordered list of endpoints: one healthy endpoint chosen by
// tier cascade, optionally prepended with a probation endpoint.
//
// It reservoir-samples one endpoint per tier in a single pass (uniform within
// each tier), so no per-tier slices are allocated — this runs once per relay.
//
// POOL-COLLAPSE GUARD: when every endpoint scores below MinThreshold, this
// returns the least-bad one rather than nothing. Returning nothing hands
// SelectBest an empty result, which surfaces to the client as "no endpoint for
// service" — a total outage produced by reputation alone, on a service whose
// suppliers are all still reachable. Reputation exists to rank a pool, not to
// empty it: the only defensible answer when the whole pool is bad is the least
// bad member of it. The guard fires the onCollapse hook so the condition is
// visible rather than silently absorbed.
func (s *TieredSelector) Select(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType) domain.EndpointAddrList {
	if len(endpoints) == 0 {
		return nil
	}

	// When the concentration cap is on it needs to know which endpoints are in
	// the winning tier. Recording the tier here rather than re-classifying is
	// worth a pooled buffer: classify is a score lookup per endpoint, and doing
	// it twice per relay doubles the cost of selection on the hot path.
	var tiers []int8
	capOn := s.capActive(ctx, serviceID)
	tieBreak := s.tieBreakActive(ctx, serviceID)
	if capOn || tieBreak {
		buf := getTierBuf(len(endpoints))
		defer putTierBuf(buf)
		tiers = *buf
	}

	var pick [numTiers]domain.EndpointAddr
	var count [numTiers]int
	// Least-bad rejected endpoint, for the pool-collapse guard. Reservoir-
	// sampled among ties so a collapsed pool still spreads load.
	var fallback domain.EndpointAddr
	var fallbackScore float64
	var fallbackTies int
	for i, ep := range endpoints {
		t, score := s.classify(ctx, serviceID, ep, rpcType)
		if tiers != nil {
			tiers[i] = int8(t)
		}
		if t < 0 {
			switch {
			case fallback == "" || score > fallbackScore:
				fallback, fallbackScore, fallbackTies = ep, score, 1
			case score == fallbackScore:
				fallbackTies++
				if rand.IntN(fallbackTies) == 0 {
					fallback = ep
				}
			}
			continue
		}
		count[t]++
		if rand.IntN(count[t]) == 0 {
			pick[t] = ep
		}
	}

	// Cascade: pick from highest available tier.
	var selected domain.EndpointAddr
	best := -1
	switch {
	case count[tier1Idx] > 0:
		best, selected = tier1Idx, pick[tier1Idx]
	case count[tier2Idx] > 0:
		best, selected = tier2Idx, pick[tier2Idx]
	case count[tier3Idx] > 0:
		best, selected = tier3Idx, pick[tier3Idx]
	case count[probationIdx] > 0:
		// All endpoints are in probation; pick one.
		return domain.EndpointAddrList{pick[probationIdx]}
	case fallback != "":
		// Pool collapse: every endpoint is below MinThreshold.
		served := domain.EndpointAddrList{fallback}
		if s.onCollapse != nil {
			s.onCollapse(serviceID, rpcType, served)
		}
		return served
	default:
		return nil
	}

	// Concentration cap: re-pick within the winning tier so no single operator
	// takes more than its capped share of selections. Only meaningful with more
	// than one candidate in the tier, and the pick is left alone when the cap
	// cannot apply (one operator holds everything, cap disabled).
	// Latency tie-break, tier 1 only: the winning tier is the set of hosts
	// reputation calls equally good; within it, ask the faster ones more.
	// The weights feed the operator cap too, so a fast operator earns share
	// across operators up to the cap, and the cap's within-operator pick is
	// drawn by the same weights.
	var weights []float64
	if tieBreak && best == tier1Idx && count[tier1Idx] > 1 {
		bufp := weightBufPool.Get().(*[]float64)
		defer weightBufPool.Put(bufp)
		if w, ok := s.tier1Weights(ctx, serviceID, endpoints, tiers, rpcType, bufp); ok {
			weights = w
		}
	}

	if capOn && count[best] > 1 {
		var weightFn func(i int) float64
		if weights != nil {
			weightFn = func(i int) float64 { return weights[i] }
		}
		if _, capped, ok := cappedPickWeighted(*s.operatorCap.Load(), endpoints, func(i int) bool {
			return int(tiers[i]) == best
		}, weightFn); ok {
			selected = capped
			weights = nil // the cap's pick already honoured the weights
		}
	}
	if weights != nil {
		if ep, ok := weightedPick(endpoints, weights); ok {
			selected = ep
		}
	}

	// Probation routing: prepend a probation endpoint with configured probability.
	cfg := s.cfg.Load()
	if count[probationIdx] > 0 && cfg.ProbationPct > 0 && rand.IntN(100) < cfg.ProbationPct {
		// Prepend: probation endpoint first, healthy endpoint second.
		return domain.EndpointAddrList{pick[probationIdx], selected}
	}

	// Tier-2 trickle: when tier 1 won, a small share of relays try a tier-2
	// endpoint first so that tier is measured by traffic and not only by
	// probes. The tier-1 pick stays behind it for Retry. Only when tier 1 won:
	// if tier 2 is the winning tier it already carries everything.
	if best == tier1Idx && count[tier2Idx] > 0 && cfg.Tier2Pct > 0 && rand.IntN(100) < cfg.Tier2Pct {
		return domain.EndpointAddrList{pick[tier2Idx], selected}
	}

	return domain.EndpointAddrList{selected}
}

// TopTierCandidates returns every endpoint in the highest non-empty tier.
// Cascades T1 → T2 → T3; if only probation endpoints qualify, returns them.
// Used by callers that want to weight within a tier themselves (e.g.,
// SelectSpread for connection-count-aware load spreading).
//
// Carries the same pool-collapse guard as Select: when no endpoint clears
// MinThreshold, the least-bad ones are returned rather than an empty list.
func (s *TieredSelector) TopTierCandidates(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList, rpcType domain.RPCType) domain.EndpointAddrList {
	if len(endpoints) == 0 {
		return nil
	}
	// Two passes: find the best populated tier, then collect only that tier.
	// Used by the WS open path (not per-relay), so the extra score pass is fine.
	best := -1
	bestRejected := 0.0
	haveRejected := false
	for _, ep := range endpoints {
		t, score := s.classify(ctx, serviceID, ep, rpcType)
		if t < 0 {
			if !haveRejected || score > bestRejected {
				bestRejected, haveRejected = score, true
			}
			continue
		}
		if best < 0 || t < best {
			best = t
		}
	}

	var out domain.EndpointAddrList
	if best < 0 {
		if !haveRejected {
			return nil
		}
		// Pool collapse: return every endpoint tied at the least-bad score.
		for _, ep := range endpoints {
			if _, score := s.classify(ctx, serviceID, ep, rpcType); score == bestRejected {
				out = append(out, ep)
			}
		}
		if s.onCollapse != nil {
			s.onCollapse(serviceID, rpcType, out)
		}
		return out
	}

	for _, ep := range endpoints {
		if t, _ := s.classify(ctx, serviceID, ep, rpcType); t == best {
			out = append(out, ep)
		}
	}
	return out
}

var weightBufPool = sync.Pool{New: func() any { return new([]float64) }}

// tier1Weights fills buf with 1/latency for every tier-1 endpoint (zero for
// the rest): latency from the EWMA, floored at latencyFloorMS; an unmeasured
// endpoint is weighed as twice the mean of the measured ones, so it draws
// half a typical host's share. That is enough traffic to measure it within
// a minute at canary volume, and not so much that a host whose only
// attempts failed — the EWMA reads success only, so it stays unmeasured —
// keeps a full share while it fails. ok is false when fewer than two
// endpoints are in tier 1 or none is measured, and the uniform pick stands.
func (s *TieredSelector) tier1Weights(
	ctx context.Context,
	serviceID domain.ServiceID,
	endpoints domain.EndpointAddrList,
	tiers []int8,
	rpcType domain.RPCType,
	buf *[]float64,
) ([]float64, bool) {
	if cap(*buf) < len(endpoints) {
		*buf = make([]float64, len(endpoints))
	}
	weights := (*buf)[:len(endpoints)]

	var sum float64
	known, candidates := 0, 0
	for i, ep := range endpoints {
		weights[i] = 0
		if int(tiers[i]) != tier1Idx {
			continue
		}
		candidates++
		if ms, ok := s.latency(ctx, serviceID, ep, rpcType); ok && ms > 0 {
			weights[i] = ms
			sum += ms
			known++
		} else {
			weights[i] = -1 // unmeasured, filled below
		}
	}
	if candidates < 2 || known == 0 {
		return nil, false
	}
	mean := sum / float64(known)
	for i := range weights {
		switch {
		case weights[i] == 0:
			continue
		case weights[i] < 0:
			weights[i] = 2 * mean
		}
		if weights[i] < latencyFloorMS {
			weights[i] = latencyFloorMS
		}
		weights[i] = 1 / weights[i]
	}
	return weights, true
}

// weightedPick draws one endpoint with probability proportional to its
// weight; zero-weight endpoints are not candidates.
func weightedPick(endpoints domain.EndpointAddrList, weights []float64) (domain.EndpointAddr, bool) {
	var total float64
	for _, w := range weights {
		total += w
	}
	if total <= 0 {
		return "", false
	}
	r := rand.Float64() * total
	for i, ep := range endpoints {
		if weights[i] == 0 {
			continue
		}
		r -= weights[i]
		if r <= 0 {
			return ep, true
		}
	}
	for i := len(endpoints) - 1; i >= 0; i-- {
		if weights[i] != 0 {
			return endpoints[i], true
		}
	}
	return "", false
}
