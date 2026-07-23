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
}

// DefaultSelectorConfig returns a SelectorConfig with default thresholds.
func DefaultSelectorConfig() SelectorConfig {
	return SelectorConfig{
		Tier1Threshold:     80,
		Tier2Threshold:     50,
		MinThreshold:       10,
		ProbationThreshold: 30,
		ProbationPct:       10,
	}
}

// ScoreFn looks up one endpoint's reputation score. The second return is
// false when the endpoint is unknown to the implementation (it is then
// treated as tier 3). Per-endpoint lookup — rather than returning a map for
// the whole list — keeps the per-relay selection path allocation-free.
type ScoreFn func(ctx context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr) (float64, bool)

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
}

// NewTieredSelector creates a selector that uses the provided score lookup
// function to classify endpoints into tiers.
func NewTieredSelector(cfg SelectorConfig, scoreFn ScoreFn) *TieredSelector {
	return &TieredSelector{
		cfg:    cfg,
		scores: scoreFn,
	}
}

// classify maps an endpoint to its tier index, or -1 when it's below the
// minimum threshold and must be skipped.
func (s *TieredSelector) classify(ctx context.Context, serviceID domain.ServiceID, ep domain.EndpointAddr) int {
	score, ok := s.scores(ctx, serviceID, ep)
	if !ok {
		// Unknown endpoints default to tier 3 (they'll get a score after first relay).
		return tier3Idx
	}
	switch {
	case score < s.cfg.MinThreshold:
		return -1 // filtered out entirely
	case score < s.cfg.ProbationThreshold:
		return probationIdx
	case score >= s.cfg.Tier1Threshold:
		return tier1Idx
	case score >= s.cfg.Tier2Threshold:
		return tier2Idx
	default:
		return tier3Idx
	}
}

// Select returns an ordered list of endpoints: one healthy endpoint chosen by
// tier cascade, optionally prepended with a probation endpoint.
//
// It reservoir-samples one endpoint per tier in a single pass (uniform within
// each tier), so no per-tier slices are allocated — this runs once per relay.
func (s *TieredSelector) Select(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList) domain.EndpointAddrList {
	if len(endpoints) == 0 {
		return nil
	}

	var pick [numTiers]domain.EndpointAddr
	var count [numTiers]int
	for _, ep := range endpoints {
		t := s.classify(ctx, serviceID, ep)
		if t < 0 {
			continue
		}
		count[t]++
		if rand.IntN(count[t]) == 0 {
			pick[t] = ep
		}
	}

	// Cascade: pick from highest available tier.
	var selected domain.EndpointAddr
	switch {
	case count[tier1Idx] > 0:
		selected = pick[tier1Idx]
	case count[tier2Idx] > 0:
		selected = pick[tier2Idx]
	case count[tier3Idx] > 0:
		selected = pick[tier3Idx]
	case count[probationIdx] > 0:
		// All endpoints are in probation; pick one.
		return domain.EndpointAddrList{pick[probationIdx]}
	default:
		return nil
	}

	// Probation routing: prepend a probation endpoint with configured probability.
	if count[probationIdx] > 0 && s.cfg.ProbationPct > 0 && rand.IntN(100) < s.cfg.ProbationPct {
		// Prepend: probation endpoint first, healthy endpoint second.
		return domain.EndpointAddrList{pick[probationIdx], selected}
	}

	return domain.EndpointAddrList{selected}
}

// TopTierCandidates returns every endpoint in the highest non-empty tier.
// Cascades T1 → T2 → T3; if only probation endpoints qualify, returns them.
// Used by callers that want to weight within a tier themselves (e.g.,
// SelectSpread for connection-count-aware load spreading).
func (s *TieredSelector) TopTierCandidates(ctx context.Context, serviceID domain.ServiceID, endpoints domain.EndpointAddrList) domain.EndpointAddrList {
	if len(endpoints) == 0 {
		return nil
	}
	// Two passes: find the best populated tier, then collect only that tier.
	// Used by the WS open path (not per-relay), so the extra score pass is fine.
	best := -1
	for _, ep := range endpoints {
		t := s.classify(ctx, serviceID, ep)
		if t >= 0 && (best < 0 || t < best) {
			best = t
		}
	}
	if best < 0 {
		return nil
	}
	var out domain.EndpointAddrList
	for _, ep := range endpoints {
		if s.classify(ctx, serviceID, ep) == best {
			out = append(out, ep)
		}
	}
	return out
}
