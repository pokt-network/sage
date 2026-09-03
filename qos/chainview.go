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

	// adjustedSpread is Spread with the time between observations taken out.
	// See DisagreementSeconds. Only meaningful when BlockRateKnown.
	adjustedSpread float64
}

// DisagreementSeconds is how far apart the endpoints are once the time between
// their observations is removed, and whether that could be computed.
//
// Spread cannot answer "do my endpoints agree?" on its own, and on a moving
// chain it mostly does not. Observations inside the consensus window are taken
// at different moments — a probe sweep visits each backend once per cycle, so
// two endpoints can be observed a whole cycle apart — and the chain advances in
// between. On the mainnet canary on 2026-09-03, nearly every service showed
// 100-140 seconds of spread, which at a 74-second cycle is very close to being
// entirely the age of the observations rather than any disagreement at all.
//
// This projects every observation forward to one instant at the chain's own
// rate before measuring, so what is left is endpoints that genuinely differ.
// A service where the figure collapses toward zero was never disagreeing; one
// where it does not has an endpoint on a different chain, a stalled node, or a
// liar.
//
// Unknown when the block rate is: with no rate there is nothing to project at,
// and projecting at a guessed rate would manufacture agreement or disagreement
// out of the guess.
func (v ChainView) DisagreementSeconds() (float64, bool) {
	if !v.BlockRateKnown || v.BlockRate <= 0 || v.Endpoints == 0 {
		return 0, false
	}
	return v.adjustedSpread / v.BlockRate, true
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

// HeightObserver reports when a backend last supplied a block height, from any
// source — a probe or a client relay that happened to ask for one.
//
// It exists so the health-check executor can tell whether its own probe would
// learn anything. The plugin's height check is normally unskippable, because
// the traffic threshold guarantees how many observations arrive and not what
// is in them: only one method per chain yields a height, and a client sends
// whatever it likes. But that is an argument about uncertainty, and this
// removes the uncertainty — if a height for this backend arrived within the
// probe's own interval, the probe is buying a second copy of it.
//
// Takes the whole sibling set rather than one address because a height is a
// fact about the BACKEND, not about the staked registration used to reach it.
// Probe results already fan out to siblings for the same reason, and client
// traffic reaches whichever registration selection picked, which is rarely the
// one the probe rotation would have used.
type HeightObserver interface {
	LastHeightObservation(endpoints domain.EndpointAddrList) (time.Time, bool)
}

// ChainView reports what this consensus currently holds, over the same window
// the perceived height is computed from.
//
// It reports over EVERY endpoint that has been observed in the window, not
// only the ones selection would use. That is deliberate and worth stating,
// because a dashboard will assume otherwise: an endpoint on the wrong chain or
// a node stalled for weeks is already excluded from serving traffic, and the
// point of this view is that somebody can see it is there. Spread is therefore
// a worst-pair figure and one bad reporter dominates it — DisagreementSeconds
// is the one to read for whether the pool agrees.
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

	// adjusted projects an observation to `now` at the chain's own rate, so the
	// time between observations does not read as disagreement.
	adjusted := func(obs blockObs) float64 {
		if !view.BlockRateKnown {
			return float64(obs.Height)
		}
		return float64(obs.Height) + view.BlockRate*now.Sub(obs.Timestamp).Seconds()
	}
	var adjHigh, adjLow float64
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
			adjHigh, adjLow = adjusted(obs), adjusted(obs)
			first = false
		default:
			if obs.Height > view.Highest {
				view.Highest = obs.Height
			}
			if obs.Height < view.Lowest {
				view.Lowest = obs.Height
			}
			if a := adjusted(obs); a > adjHigh {
				adjHigh = a
			} else if a < adjLow {
				adjLow = a
			}
		}
		if obs.Timestamp.After(view.Newest) {
			view.Newest = obs.Timestamp
		}
	}
	view.Endpoints = len(seen)
	if adjHigh > adjLow {
		view.adjustedSpread = adjHigh - adjLow
	}
	return view
}

// LastHeightObservation returns when any of these endpoints last reported a
// height inside the consensus window, and whether any did.
//
// One pass over the window rather than a lookup per endpoint: the window is
// bounded (maxObservations) and the sibling set is small, and this is called
// once per backend per cycle, so the scan is cheaper than the map it would
// otherwise need maintaining on the hot observation path.
func (bc *BlockConsensus) LastHeightObservation(endpoints domain.EndpointAddrList) (time.Time, bool) {
	if len(endpoints) == 0 {
		return time.Time{}, false
	}
	wanted := make(map[domain.EndpointAddr]struct{}, len(endpoints))
	for _, ep := range endpoints {
		wanted[ep] = struct{}{}
	}

	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var newest time.Time
	for _, obs := range bc.observations {
		if _, ok := wanted[obs.Endpoint]; !ok {
			continue
		}
		if obs.Timestamp.After(newest) {
			newest = obs.Timestamp
		}
	}
	return newest, !newest.IsZero()
}
