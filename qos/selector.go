package qos

import "github.com/pokt-network/sage/domain"

// FilterFunc returns an error if an endpoint should be excluded.
// The error describes the reason for exclusion (used for logging/debugging).
type FilterFunc func(endpoint domain.EndpointAddr) error

// SelectResult holds the result of endpoint selection.
type SelectResult struct {
	Endpoints domain.EndpointAddrList
	Degraded  bool
	Tier      int
}

// Select applies filters to the endpoint list with tiered degradation.
//
// Degradation tiers:
//   - Tier 1: apply all filters normally
//   - Tier 2: relax sync allowance by 2x (re-run filters with relaxedFilters)
//   - Tier 3: skip block height filter entirely (only run non-block-height filters)
//   - If still empty after tier 3: return original list (degraded=true, tier=3)
//
// The relaxedFilters and nonBlockHeightFilters parameters support the tiered fallback.
// If they are nil, Select falls back to the original list when tier 1 yields nothing.
//
// fallbackRanker, when non-nil, narrows the *degraded* fallback sets (tier 3 and
// the all-exhausted case) to the least-stale endpoints. Block-height filtering
// has already been abandoned by that point, so without this the candidate pool —
// from which downstream reputation selection picks — can include arbitrarily
// stale endpoints. Tiers 1 and 2 are left untouched (they already passed a
// block-height bound). Pass nil to keep the full unranked fallback.
func Select(
	endpoints domain.EndpointAddrList,
	filters []FilterFunc,
	relaxedFilters []FilterFunc,
	nonBlockHeightFilters []FilterFunc,
	fallbackRanker func(domain.EndpointAddrList) domain.EndpointAddrList,
) SelectResult {
	if len(endpoints) == 0 {
		return SelectResult{Endpoints: nil, Degraded: false, Tier: 0}
	}

	// Tier 1: all filters.
	if result := applyFilters(endpoints, filters); len(result) > 0 {
		return SelectResult{Endpoints: result, Degraded: false, Tier: 1}
	}

	// Tier 2: relaxed sync allowance.
	if relaxedFilters != nil {
		if result := applyFilters(endpoints, relaxedFilters); len(result) > 0 {
			return SelectResult{Endpoints: result, Degraded: true, Tier: 2}
		}
	}

	// Tier 3: skip block height filters entirely.
	if nonBlockHeightFilters != nil {
		if result := applyFilters(endpoints, nonBlockHeightFilters); len(result) > 0 {
			return SelectResult{Endpoints: rankFallback(result, fallbackRanker), Degraded: true, Tier: 3}
		}
	}

	// All tiers exhausted: return original list.
	return SelectResult{Endpoints: rankFallback(endpoints, fallbackRanker), Degraded: true, Tier: 3}
}

// rankFallback applies the optional fallback ranker, guarding against a ranker
// that returns nothing (never hand back an empty candidate set).
func rankFallback(eps domain.EndpointAddrList, ranker func(domain.EndpointAddrList) domain.EndpointAddrList) domain.EndpointAddrList {
	if ranker == nil {
		return eps
	}
	if ranked := ranker(eps); len(ranked) > 0 {
		return ranked
	}
	return eps
}

// LeastStaleFallback returns a fallback ranker for qos.Select that narrows a
// degraded candidate set to its least-stale band: the endpoints whose observed
// block height is closest to perceived. All endpoints tied at the minimum lag
// are kept (so downstream load-spreading still has room to distribute), while
// deeply-stale endpoints are dropped. Endpoints with a known height always win
// over no-data endpoints; if none have a known height (or perceived is 0, i.e.
// cold start), the set is returned unchanged.
//
// This is the SAGE adaptation of PATH's least-stale fallback (PR #512): because
// reputation selection picks from the candidate *list* and ignores staleness,
// the staleness preference has to be expressed as list membership, not order.
func LeastStaleFallback(getHeight func(domain.EndpointAddr) (uint64, bool), perceived uint64) func(domain.EndpointAddrList) domain.EndpointAddrList {
	return func(eps domain.EndpointAddrList) domain.EndpointAddrList {
		if perceived == 0 || len(eps) <= 1 {
			return eps
		}

		lags := make(map[domain.EndpointAddr]uint64, len(eps))
		bestLag := ^uint64(0)
		known := 0
		for _, ep := range eps {
			h, ok := getHeight(ep)
			if !ok {
				continue
			}
			known++
			var lag uint64
			if perceived > h {
				lag = perceived - h
			}
			lags[ep] = lag
			if lag < bestLag {
				bestLag = lag
			}
		}

		// No endpoint has a known height — can't rank; keep the original set.
		if known == 0 {
			return eps
		}

		out := make(domain.EndpointAddrList, 0, known)
		for _, ep := range eps {
			if lag, ok := lags[ep]; ok && lag == bestLag {
				out = append(out, ep)
			}
		}
		return out
	}
}

// applyFilters runs all filters against each endpoint and returns those that pass all.
func applyFilters(endpoints domain.EndpointAddrList, filters []FilterFunc) domain.EndpointAddrList {
	if len(filters) == 0 {
		return endpoints
	}
	out := make(domain.EndpointAddrList, 0, len(endpoints))
	for _, ep := range endpoints {
		passed := true
		for _, f := range filters {
			if err := f(ep); err != nil {
				passed = false
				break
			}
		}
		if passed {
			out = append(out, ep)
		}
	}
	return out
}

// BlockHeightFilter returns a FilterFunc that excludes endpoints below the minimum block height.
// minHeight is typically perceived - syncAllowance.
func BlockHeightFilter(getHeight func(domain.EndpointAddr) (uint64, bool), minHeight uint64) FilterFunc {
	return func(endpoint domain.EndpointAddr) error {
		height, ok := getHeight(endpoint)
		if !ok {
			// Unknown endpoint — let through (eventual consistency).
			return nil
		}
		if height < minHeight {
			return &domain.RelayError{
				Kind:      domain.ErrCapability,
				Message:   "endpoint block height too low",
				Retryable: true,
			}
		}
		return nil
	}
}
