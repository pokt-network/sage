package qos

import (
	"time"

	"github.com/pokt-network/sage/domain"
)

// ChainView is what a service currently believes about its chain, and how
// recently it learned it.
//
// It exists because none of that was observable. SAGE exported nothing about
// block height, consensus or QoS state, so the mechanism endpoint selection
// tiers on could only be inspected by reading the process's memory in a
// debugger. That gap hid a real defect: traffic-informed probing guarantees
// how MANY observations arrive and not what is in them, so a service whose
// height source lapses goes stale in silence, and nothing in the metric set
// could have shown it (mainnet canary, 2026-09-03).
//
// Staleness is the field that answers that. A service being probed has a
// fresh observation every cycle; one relying on sampled client traffic for its
// height has one only when a client happens to ask for it.
type ChainView struct {
	// Perceived is the height consensus settled on — the number the height
	// filters compare against.
	Perceived uint64
	// Highest and Lowest bound the endpoint heights still inside the
	// consensus window. Spread is their difference.
	Highest uint64
	Lowest  uint64
	// Endpoints is how many distinct endpoints contributed an observation
	// inside the window. One is not a consensus; zero means the service is
	// flying on whatever Perceived last was.
	Endpoints int
	// Newest is when the most recent observation arrived, zero if there is
	// none inside the window.
	Newest time.Time
	// BlockRate is how many blocks this chain produces per second, and
	// BlockRateKnown whether that could be derived at all. See
	// BlockConsensus.BlockRate: a stalled chain has no rate, and it is
	// reported as unknown rather than as zero.
	BlockRate      float64
	BlockRateKnown bool
}

// SpreadSeconds converts the block spread into time, and reports whether that
// conversion was possible.
//
// Blocks are not comparable across chains and reading them as if they were
// inverts the answer. On the mainnet canary on 2026-09-03, arb-one showed 534
// blocks of spread against eth's 11 — a 48x difference that looks damning
// until the block times go in: arb-one at roughly a quarter-second a block is
// 133 seconds, eth at roughly twelve is 132. The same number. An operator
// without both block times in their head reads the block figure backwards,
// which is why this exists next to it.
func (v ChainView) SpreadSeconds() (float64, bool) {
	if !v.BlockRateKnown || v.BlockRate <= 0 {
		return 0, false
	}
	return float64(v.Spread()) / v.BlockRate, true
}

// Spread is how far apart the endpoints in the window are. Zero with no
// observations, which reads the same as perfect agreement — Endpoints is what
// separates the two, and a caller exporting this must report both.
func (v ChainView) Spread() uint64 {
	if v.Endpoints == 0 || v.Highest < v.Lowest {
		return 0
	}
	return v.Highest - v.Lowest
}

// ChainViewer is the optional extension interface a QoS plugin implements to
// report its chain view. A plugin that tracks no block height does not
// implement it, and nothing is exported for that service.
type ChainViewer interface {
	ChainView() ChainView
}

// ChainView reports what this consensus currently holds, over the same window
// the perceived height is computed from.
//
// It prunes nothing: a read must not mutate what a concurrent write is
// computing against, and an observation just outside the window is excluded
// here by the same cutoff computeperceived would apply. That means a quiet
// service reports Endpoints=0 rather than reporting its last known spread
// forever, which is the honest answer and the one staleness is derived from.
func (bc *BlockConsensus) ChainView() ChainView {
	now := time.Now()
	cutoff := now.Add(-bc.windowDuration)

	bc.mu.RLock()
	defer bc.mu.RUnlock()

	view := ChainView{Perceived: bc.perceived.Load()}
	view.BlockRate, view.BlockRateKnown = blockRate(bc.rateSamples)
	seen := make(map[domain.EndpointAddr]struct{}, len(bc.observations))
	first := true
	for _, obs := range bc.observations {
		if !obs.Timestamp.After(cutoff) {
			continue
		}
		seen[obs.Endpoint] = struct{}{}
		switch {
		case first:
			view.Highest, view.Lowest = obs.Height, obs.Height
			first = false
		case obs.Height > view.Highest:
			view.Highest = obs.Height
		case obs.Height < view.Lowest:
			view.Lowest = obs.Height
		}
		if obs.Timestamp.After(view.Newest) {
			view.Newest = obs.Timestamp
		}
	}
	view.Endpoints = len(seen)
	return view
}
