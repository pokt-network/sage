package qos

import (
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
)

const (
	defaultWindowDuration  = 2 * time.Minute
	defaultMaxObservations = 1000
	defaultGracePeriod     = 30 * time.Second
)

// BlockConsensus computes the perceived block height from endpoint observations
// using a median-anchored consensus algorithm with optional external floor.
type BlockConsensus struct {
	logger          *slog.Logger
	mu              sync.RWMutex
	observations    []blockObs
	windowDuration  time.Duration
	maxObservations int
	syncAllowance   uint64

	perceived atomic.Uint64 // lock-free read on hot path

	// rateSamples is a short history of (perceived height, when) used to derive
	// how fast this chain produces blocks. Under mu, appended only when the
	// perceived height moves. See BlockRate.
	rateSamples []rateSample

	externalFloor atomic.Uint64 // from external block sources
	graceStart    time.Time
	gracePeriod   time.Duration
}

// storeHook, when set, is called with "add" or "reset" immediately before the
// perceived height is published, while mu is held. It exists for one test —
// blockconsensus_ordering_test.go — which cannot otherwise wedge itself
// between the computation and the store to prove the two happen together: the
// interleaving that used to corrupt a reset is real but too narrow to
// reproduce reliably by racing goroutines. Nothing outside a test ever sets
// it; the hot path pays one atomic load, next to the atomic store it guards.
var storeHook atomic.Pointer[func(string)]

func beforeStoreHook(op string) {
	if h := storeHook.Load(); h != nil {
		(*h)(op)
	}
}

type blockObs struct {
	Endpoint  domain.EndpointAddr
	Height    uint64
	Timestamp time.Time
}

// NewBlockConsensus creates a BlockConsensus with the given sync allowance.
func NewBlockConsensus(logger *slog.Logger, syncAllowance uint64) *BlockConsensus {
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockConsensus{
		logger:          logger,
		observations:    make([]blockObs, 0, 64),
		windowDuration:  defaultWindowDuration,
		maxObservations: defaultMaxObservations,
		syncAllowance:   syncAllowance,
		graceStart:      time.Now(),
		gracePeriod:     defaultGracePeriod,
	}
}

// AddObservation records a block height observation from an endpoint and recomputes perceived.
//
// Implausible heights are refused at the door. Every plugin funnels its
// observations through here, so this one guard keeps the whole package's
// arithmetic inside a range where it cannot wrap — see MaxPlausibleBlockHeight.
func (bc *BlockConsensus) AddObservation(endpoint domain.EndpointAddr, height uint64) {
	if !IsPlausibleBlockHeight(height) {
		// Zero is routine — it just means "not observed yet" — so only a height
		// that is positively absurd is worth waking anyone for. It means a
		// supplier is lying or a parser is broken, and either is worth knowing.
		if height != 0 {
			bc.logger.Warn("block consensus: refusing implausible height",
				"endpoint", endpoint,
				"height", height,
				"max_plausible", uint64(MaxPlausibleBlockHeight),
			)
		}
		return
	}

	now := time.Now()

	// An observation at less than half the perceived head is not a lagging
	// node, it is a wrong one — a fresh sync, a different chain, a parser
	// reading the wrong field — and it stretches the chain-view spread to the
	// whole chain height. Said here, at ingest, with the endpoint named:
	// on 2026-09-04 one sui endpoint did exactly this for two cycles and left
	// no line to attribute it by.
	if perceived := bc.perceived.Load(); perceived > 0 && height < perceived/2 {
		bc.logger.Warn("block consensus: endpoint reports a height far below the perceived head",
			"endpoint", endpoint,
			"height", height,
			"perceived", perceived,
		)
	}

	bc.mu.Lock()
	// Prune stale observations.
	bc.pruneOlderThan(now.Add(-bc.windowDuration))

	// Cap observations.
	if len(bc.observations) >= bc.maxObservations {
		// Drop oldest quarter.
		drop := bc.maxObservations / 4
		bc.observations = bc.observations[drop:]
	}

	bc.observations = append(bc.observations, blockObs{
		Endpoint:  endpoint,
		Height:    height,
		Timestamp: now,
	})

	perceived := bc.computePerceived(now)
	// Stored under mu, not after it. The atomic exists so PerceivedBlock() can
	// read without a lock; it does not make the write orderable against Reset.
	// With the store outside, a Reset could take mu, clear everything and
	// publish 0 in the window between this unlock and this store — and then
	// this store would put the poisoned height straight back, moments after
	// the operator was told the reset had happened. Writing here costs the hot
	// path nothing: mu is already held.
	beforeStoreHook("add")
	bc.perceived.Store(perceived)
	bc.recordRateSampleLocked(perceived, now)
	bc.mu.Unlock()
}

// EndpointHeights returns the latest observation per endpoint inside the
// window, newest first.
func (bc *BlockConsensus) EndpointHeights() []EndpointHeight {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	latest := make(map[domain.EndpointAddr]blockObs, len(bc.observations))
	for _, obs := range bc.observations {
		if cur, ok := latest[obs.Endpoint]; !ok || obs.Timestamp.After(cur.Timestamp) {
			latest[obs.Endpoint] = obs
		}
	}
	out := make([]EndpointHeight, 0, len(latest))
	for _, obs := range latest {
		out = append(out, EndpointHeight{Endpoint: obs.Endpoint, Height: obs.Height, ObservedAt: obs.Timestamp})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObservedAt.Equal(out[j].ObservedAt) {
			return out[i].Endpoint < out[j].Endpoint
		}
		return out[i].ObservedAt.After(out[j].ObservedAt)
	})
	return out
}

// PerceivedBlock returns the current perceived block height (atomic, zero-contention).
func (bc *BlockConsensus) PerceivedBlock() uint64 {
	return bc.perceived.Load()
}

// SetExternalFloor sets the external block height floor (e.g., from external
// block sources).
//
// Under mu like every other store, so that a Reset and a floor update cannot
// interleave: outside the lock, a floor fetched before the operator's reset
// could land after it and outlive the state the reset was meant to discard.
func (bc *BlockConsensus) SetExternalFloor(height uint64) {
	bc.mu.Lock()
	bc.externalFloor.Store(height)
	bc.mu.Unlock()
}

// Reset discards every observation, zeroes the perceived height and the
// external floor, and restarts the grace window.
//
// It exists for an operator to throw away a poisoned perceived height (a
// supplier that briefly lied, or an external floor set from a since-corrected
// source) without a restart. Restarting the grace window matters as much as
// zeroing the floor: without it, a floor set again immediately after Reset
// would apply on the very next observation instead of waiting out a fresh
// cold-start window like it would for a plugin that had never seen traffic.
// Both stores happen under mu for the same reason AddObservation's does: the
// atomics are there for lock-free reads, and outside the lock a reset and an
// in-flight observation can interleave so that the height the operator just
// threw away is the one left standing.
func (bc *BlockConsensus) Reset() {
	bc.mu.Lock()
	bc.observations = bc.observations[:0]
	// The rate history is part of what a reset discards: kept, it would let a
	// poisoned chain's cadence outlive the heights it was derived from.
	bc.rateSamples = bc.rateSamples[:0]
	bc.graceStart = time.Now()
	beforeStoreHook("reset")
	bc.perceived.Store(0)
	bc.externalFloor.Store(0)
	bc.mu.Unlock()
}

// SetSyncAllowance updates the sync allowance used for outlier filtering.
func (bc *BlockConsensus) SetSyncAllowance(allowance uint64) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.syncAllowance = allowance
}

// pruneOlderThan removes observations before cutoff. Must be called with mu held.
func (bc *BlockConsensus) pruneOlderThan(cutoff time.Time) {
	n := 0
	for _, obs := range bc.observations {
		if !obs.Timestamp.Before(cutoff) {
			bc.observations[n] = obs
			n++
		}
	}
	bc.observations = bc.observations[:n]
}

// computePerceived calculates the perceived block height. Must be called with mu held.
func (bc *BlockConsensus) computePerceived(now time.Time) uint64 {
	if len(bc.observations) == 0 {
		return bc.applyExternalFloor(0, now)
	}

	// Collect heights.
	heights := make([]uint64, len(bc.observations))
	for i, obs := range bc.observations {
		heights[i] = obs.Height
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	// Median.
	median := heights[len(heights)/2]

	// Outlier threshold: median + (syncAllowance * 3).
	//
	// Saturating, because this used to wrap. Note heights[len/2] takes the UPPER
	// median on even counts, so a liar needs only half the observations — two
	// endpoints, one hostile — to drag the median to its own value. When that
	// value was huge the cap wrapped to a tiny number, every honest height then
	// exceeded it, and perceived fell to 0 — which every plugin reads as cold
	// start and responds to by disabling block-height filtering entirely. The
	// ceiling in AddObservation now stops a height that large from ever being
	// recorded; this keeps the arithmetic honest regardless of syncAllowance,
	// which is operator-set and unbounded.
	outlierCap := saturatingAdd(median, saturatingMul(bc.syncAllowance, 3))

	// Perceived = max of non-outlier heights.
	var perceived uint64
	for _, h := range heights {
		if h <= outlierCap && h > perceived {
			perceived = h
		}
	}

	return bc.applyExternalFloor(perceived, now)
}

// applyExternalFloor applies the external floor if past the grace period.
func (bc *BlockConsensus) applyExternalFloor(perceived uint64, now time.Time) uint64 {
	floor := bc.externalFloor.Load()
	if floor == 0 {
		return perceived
	}
	// Don't apply floor during grace period (cold start).
	if now.Before(bc.graceStart.Add(bc.gracePeriod)) {
		return perceived
	}
	if floor > perceived {
		return floor
	}
	return perceived
}

// maxRateSamples bounds the block-rate history. Sixteen samples of a chain
// that moves is minutes of history on a fast chain and an hour on a slow one,
// which is the range the rate needs to be stable over; the cap exists because
// entries are never removed individually.
const maxRateSamples = 16

// rateSample is a perceived height and when it was published.
type rateSample struct {
	height uint64
	at     time.Time
}

// recordRateSampleLocked appends a sample when the perceived height moves.
// Called under mu.
//
// Only on movement, deliberately. A chain observed every second and a chain
// observed every minute should yield the same blocks-per-second, and sampling
// on every observation would fill the history with repeats of one height on
// the busy service and derive a rate of zero from them.
func (bc *BlockConsensus) recordRateSampleLocked(perceived uint64, now time.Time) {
	if perceived == 0 {
		return
	}
	if n := len(bc.rateSamples); n > 0 && bc.rateSamples[n-1].height == perceived {
		return
	}
	if len(bc.rateSamples) >= maxRateSamples {
		bc.rateSamples = append(bc.rateSamples[:0], bc.rateSamples[1:]...)
	}
	bc.rateSamples = append(bc.rateSamples, rateSample{height: perceived, at: now})
}

// BlockRate reports how many blocks this chain produces per second, derived
// from how far the perceived height has moved over how long, and whether that
// is known at all.
//
// It is derived rather than configured because a per-chain block-time table is
// a set of values that drift and duplicate what the consensus is already
// watching. Two samples of a moving chain are enough, and the answer
// self-corrects when a chain changes its cadence.
//
// Not known, and reported as such rather than guessed: fewer than two samples,
// no elapsed time between them, or a height that has not advanced. A stalled
// chain has no rate, and inventing one would turn a stalled chain into a
// confident wrong number in every metric derived from it.
func (bc *BlockConsensus) BlockRate() (float64, bool) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return blockRate(bc.rateSamples)
}

func blockRate(samples []rateSample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	oldest, newest := samples[0], samples[len(samples)-1]
	elapsed := newest.at.Sub(oldest.at).Seconds()
	if elapsed <= 0 || newest.height <= oldest.height {
		return 0, false
	}
	return float64(newest.height-oldest.height) / elapsed, true
}
