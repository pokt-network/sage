package reputation

import (
	"context"
	"math/rand/v2"

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

// TieredSelector selects endpoints by cascading through reputation tiers.
// Tier 1 (best) is tried first; if empty, tier 2; then tier 3. Within each
// tier a random endpoint is chosen. Probation endpoints may be prepended.
type TieredSelector struct {
	cfg    SelectorConfig
	scores ScoreFn

	// onCollapse, when set, is invoked once per selection in which every
	// endpoint scored below MinThreshold and the pool-collapse guard had to
	// serve a sub-threshold endpoint. See Select.
	onCollapse func(domain.ServiceID)

	// operatorCap bounds any single operator's share of selections within the
	// winning tier. See concentration.go.
	operatorCap OperatorCapConfig
	// capGate decides per relay whether the cap applies. Nil means never — the
	// cap is opt-in at wire time, behind a feature flag, so it can be turned
	// off at runtime without a deploy.
	capGate func(context.Context, domain.ServiceID) bool
}

// NewTieredSelector creates a selector that uses the provided score lookup
// function to classify endpoints into tiers.
func NewTieredSelector(cfg SelectorConfig, scoreFn ScoreFn) *TieredSelector {
	return &TieredSelector{
		cfg:    cfg,
		scores: scoreFn,
	}
}

// SetCollapseHook registers a callback invoked whenever the pool-collapse guard
// fires. Nil clears it. Not safe to call concurrently with selection; call it
// at wire time.
func (s *TieredSelector) SetCollapseHook(fn func(domain.ServiceID)) {
	s.onCollapse = fn
}

// SetOperatorCap enables the per-operator concentration cap, gated per relay by
// gate (nil gate = never applied). Not safe to call concurrently with
// selection; call it at wire time.
func (s *TieredSelector) SetOperatorCap(cfg OperatorCapConfig, gate func(context.Context, domain.ServiceID) bool) {
	s.operatorCap = cfg
	s.capGate = gate
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
	switch {
	case score < s.cfg.MinThreshold:
		return -1, score // filtered out entirely
	case score < s.cfg.ProbationThreshold:
		return probationIdx, score
	case score >= s.cfg.Tier1Threshold:
		return tier1Idx, score
	case score >= s.cfg.Tier2Threshold:
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
	if capOn {
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
		if capOn {
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
		if s.onCollapse != nil {
			s.onCollapse(serviceID)
		}
		return domain.EndpointAddrList{fallback}
	default:
		return nil
	}

	// Concentration cap: re-pick within the winning tier so no single operator
	// takes more than its capped share of selections. Only meaningful with more
	// than one candidate in the tier, and the pick is left alone when the cap
	// cannot apply (one operator holds everything, cap disabled).
	if capOn && count[best] > 1 {
		if _, capped, ok := cappedPick(s.operatorCap, endpoints, func(i int) bool {
			return int(tiers[i]) == best
		}); ok {
			selected = capped
		}
	}

	// Probation routing: prepend a probation endpoint with configured probability.
	if count[probationIdx] > 0 && s.cfg.ProbationPct > 0 && rand.IntN(100) < s.cfg.ProbationPct {
		// Prepend: probation endpoint first, healthy endpoint second.
		return domain.EndpointAddrList{pick[probationIdx], selected}
	}

	// Tier-2 trickle: when tier 1 won, a small share of relays try a tier-2
	// endpoint first so that tier is measured by traffic and not only by
	// probes. The tier-1 pick stays behind it for Retry. Only when tier 1 won:
	// if tier 2 is the winning tier it already carries everything.
	if best == tier1Idx && count[tier2Idx] > 0 && s.cfg.Tier2Pct > 0 && rand.IntN(100) < s.cfg.Tier2Pct {
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
		if s.onCollapse != nil {
			s.onCollapse(serviceID)
		}
		for _, ep := range endpoints {
			if _, score := s.classify(ctx, serviceID, ep, rpcType); score == bestRejected {
				out = append(out, ep)
			}
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
