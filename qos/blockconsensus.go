package qos

import (
	"log/slog"
	"slices"
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
	// syncAllowance is the service's sync allowance, moved at runtime with
	// the plugin's selection bound (HeightTracking.SetSyncAllowance).
	syncAllowance atomic.Uint64

	perceived atomic.Uint64 // lock-free read on hot path

	// rateSamples is a short history of (perceived height, when) used to derive
	// how fast this chain produces blocks. Under mu, appended only when the
	// perceived height moves. See BlockRate.
	rateSamples []rateSample

	externalFloor atomic.Uint64 // from external block sources
	graceStart    time.Time
	gracePeriod   time.Duration
}

// storeHook, when set, is called with "add", "reset" or "floor" immediately before the
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
	bc := &BlockConsensus{
		logger:          logger,
		observations:    make([]blockObs, 0, 64),
		windowDuration:  defaultWindowDuration,
		maxObservations: defaultMaxObservations,
		graceStart:      time.Now(),
		gracePeriod:     defaultGracePeriod,
	}
	bc.syncAllowance.Store(syncAllowance)
	return bc
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
//
// Perceived is recomputed here, not only on the next observation: a service
// whose endpoints report no height at all (mainnet sei until 2026-09-29) never
// makes one, and its floor sat in the chain view for a week with perceived at 0.
func (bc *BlockConsensus) SetExternalFloor(height uint64) {
	bc.mu.Lock()
	bc.externalFloor.Store(height)
	beforeStoreHook("floor")
	bc.perceived.Store(bc.computePerceived(time.Now()))
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

	tolerance := saturatingMul(bc.syncAllowance.Load(), 3)
	anchor := partyAnchor(bc.observations, tolerance)

	// Outlier threshold: anchor + (syncAllowance * 3).
	//
	// Saturating, because this used to wrap: a huge anchor wrapped the cap to
	// a tiny number, every honest height then exceeded it, and perceived fell
	// to 0 — which every plugin reads as cold start and responds to by
	// disabling block-height filtering entirely. The ceiling in AddObservation
	// now stops a height that large from ever being recorded; this keeps the
	// arithmetic honest regardless of syncAllowance, which is operator-set and
	// unbounded.
	outlierCap := saturatingAdd(anchor, tolerance)

	// Perceived = max of non-outlier heights.
	var perceived uint64
	for _, obs := range bc.observations {
		if h := obs.Height; h <= outlierCap && h > perceived {
			perceived = h
		}
	}

	return bc.applyExternalFloor(perceived, now)
}

// partyAnchor is the height the outlier cap is measured from. Each party casts
// one vote, the median of its own observations; the anchor is the highest vote
// another party corroborates (the two within tolerance of each other), and the
// lower median of the votes when no two parties agree. A pool of one operator
// gets exactly the median of every observation, as before.
//
// One vote per party, because the median of every observation weighs an
// operator by how many endpoints it staked. On mainnet metis (2026-09-28) one
// operator held 41 of 50 endpoints, all on a node 79,000 blocks behind: the
// median was that node's height, the in-sync operators were cut as outliers,
// and perceived was the stale height — held up only by the external floor,
// and not at all in the grace minute after each boot. A party is
// domain.EndpointAddr.Party: two brands of one owner cast one vote.
//
// The highest corroborated vote, not the median of the votes, because
// parties behind the head do not agree with each other. On mainnet solana
// (2026-09-29) the votes were one dead party, one stuck 14,363 slots behind
// and two at the head: the lower median was the stuck party, the two at the
// head sat outside the cap, and perceived fell to the stuck height on two of
// five pods. Parties behind can no longer pull the head down, together or
// apart. The price: two parties agreeing on a height above the rest lift the
// head to it, where the median asked a liar for a majority; the plausibility
// ceiling at ingest still bounds how far.
//
// The lower median when nothing is corroborated — two parties that disagree —
// resolves an even split to the side behind, leaving the external floor as
// the tiebreaker. Endpoints with no operator (bare test addresses) vote alone.
func partyAnchor(observations []blockObs, tolerance uint64) uint64 {
	byParty := make(map[string][]uint64)
	for _, obs := range observations {
		key := obs.Endpoint.Party()
		if key == "" {
			key = string(obs.Endpoint)
		}
		byParty[key] = append(byParty[key], obs.Height)
	}
	votes := make([]uint64, 0, len(byParty))
	for _, heights := range byParty {
		slices.Sort(heights)
		votes = append(votes, heights[len(heights)/2])
	}
	slices.Sort(votes)
	for i := len(votes) - 1; i > 0; i-- {
		if votes[i]-votes[i-1] <= tolerance {
			return votes[i]
		}
	}
	return votes[(len(votes)-1)/2]
}

// applyExternalFloor applies the external floor if past the grace period.
//
// The floor engages only when the pool is collectively behind the trusted
// node by more than the sync allowance, and then lifts perceived to
// floor minus allowance, not to the floor itself. A trusted node a few
// dozen blocks ahead of every supplier is propagation, not a pool that is
// behind: on the 2026-09-04 canary robinhood's source ran 74 blocks ahead of
// the highest supplier and arb-one's 31, and with the floor taken as the
// head every supplier looked behind at once, which is what the strict height
// filter rejects. Taken this way the floor says what it is for — "the pool
// may not be more than an allowance behind the truth" — and a source that is
// merely ahead changes nothing.
func (bc *BlockConsensus) applyExternalFloor(perceived uint64, now time.Time) uint64 {
	floor := bc.externalFloor.Load()
	if floor == 0 {
		return perceived
	}
	// Don't apply floor during grace period (cold start).
	if now.Before(bc.graceStart.Add(bc.gracePeriod)) {
		return perceived
	}
	effective := floor
	if allowance := bc.syncAllowance.Load(); floor > allowance {
		effective = floor - allowance
	}
	if effective > perceived {
		return effective
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

// HeightProjection advances a stored height reading to the moment the
// perceived head last moved, at the chain's own block rate. Take one per
// selection with Projection; the zero value projects nothing.
//
// It exists because the height filter compared readings of different ages. A
// pool is probed once per cycle, so a healthy endpoint read 100 s before the
// head was read is 100 s of blocks "behind" it — about 130 blocks on bsc,
// which is more than its allowance — and the strict filter rejected it for our
// own sampling rather than for its lag. The chain view already projects this
// way for its disagreement metric.
type HeightProjection struct {
	rate      float64
	headAt    time.Time
	perceived uint64
	window    time.Duration
}

// BlocksIn is how many blocks the chain makes in d at its measured rate, 0
// while the rate is unknown.
func (bc *BlockConsensus) BlocksIn(d time.Duration) uint64 {
	p := bc.Projection()
	if p.rate <= 0 {
		return 0
	}
	return uint64(p.rate * d.Seconds())
}

// Projection captures what Project needs under one read lock, so a selection
// does not take the lock once per endpoint.
func (bc *BlockConsensus) Projection() HeightProjection {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	p := HeightProjection{perceived: bc.perceived.Load(), window: bc.windowDuration}
	if rate, ok := blockRate(bc.rateSamples); ok {
		p.rate = rate
		p.headAt = bc.rateSamples[len(bc.rateSamples)-1].at
	}
	return p
}

// Project returns height, read at observedAt, as it would have read when the
// head was read, never above the perceived head. The reading is returned
// unchanged when the rate is unknown, when it is no older than the head's,
// or when it is more than two consensus windows older.
//
// Projecting assumes the node kept syncing since it was read, so a node that
// stalled just after its reading passes until the next reading shows the
// stall — one probe cycle, since that reading carries a fresh time. Two
// windows is the bound for when readings stop coming: one window is the 120 s
// mainnet probe cycle itself, and a reading taken just over a cycle ago is the
// ordinary case, not the stale one.
func (p HeightProjection) Project(height uint64, observedAt time.Time) uint64 {
	if p.rate <= 0 || observedAt.IsZero() || height >= p.perceived {
		return height
	}
	age := p.headAt.Sub(observedAt)
	if age <= 0 || age > 2*p.window {
		return height
	}
	return min(height+uint64(p.rate*age.Seconds()), p.perceived)
}

// staleAnswerSeconds is how far behind the head, in the chain's own time, an
// answer naming the head may be before it counts as stale: generous for
// propagation, far short of a response cache holding one answer for a minute.
const staleAnswerSeconds = 10

// staleAnswerMinBlocks is the least lag that counts as stale, for chains
// slow enough that ten seconds is under two blocks.
const staleAnswerMinBlocks = 2

// AnswerLag measures an answer that names the chain head against the head
// this consensus expects at now: perceived, advanced at the chain's block rate
// since it last moved (at most one window).
// Perceived moves only when an observation arrives, so comparing against it
// unadvanced would forgive a stale answer by up to one probe cycle.
//
// stale is lag above max(staleAnswerMinBlocks, rate x staleAnswerSeconds),
// and never while the block rate is unknown (a cold start) or the head has not
// moved for a window: the lag is still reported, but there is then no telling
// propagation or a halt from a cache.
// ok is false while there is no head to compare with. An answer ahead of the
// head is lag 0.
func (bc *BlockConsensus) AnswerLag(height uint64, now time.Time) (lag uint64, stale, ok bool) {
	p := bc.Projection()
	if p.perceived == 0 {
		return 0, false, false
	}
	head := p.perceived
	age := now.Sub(p.headAt)
	if p.rate > 0 && age > 0 {
		head += uint64(p.rate * min(age, p.window).Seconds())
	}
	if height < head {
		lag = head - height
	}
	// No verdict without a rate, or once the head has not moved for a
	// window: a halted chain, or readings that stopped coming, look the same
	// from here, and projecting on would call every party's honest answer
	// stale at once — a major penalty and a retry for all of them.
	if p.rate <= 0 || age > p.window {
		return lag, false, true
	}
	tolerance := max(uint64(staleAnswerMinBlocks), uint64(p.rate*staleAnswerSeconds))
	return lag, lag > tolerance, true
}

// staleStateSlack is what a state answer's block timestamp may trail the
// clock by beyond two blocks before it counts as stale: propagation, clock
// skew and the time the probe itself took.
const staleStateSlack = 10 * time.Second

// staleStateImplausible is how far from the clock a block time may be before
// it is not a reading at all.
const staleStateImplausible = 24 * time.Hour

// StateLag grades an answer about state at "latest" by the timestamp of the
// block it was computed at, against at: stale when it trails by more than two
// block times plus staleStateSlack. lag is that trail in blocks.
//
// The clock, not perceived, because what such an answer carries is a block's
// own time. block.number inside eth_call is not the chain's own on every
// chain (on Arbitrum it is Ethereum's), and block.timestamp is. The rate is
// still needed for "two block times", so there is no verdict without one;
// and none once the head has not moved for a window, since a halted chain
// would read every party's honest answer as old (see AnswerLag).
func (bc *BlockConsensus) StateLag(blockTime, at time.Time) (lag uint64, stale, ok bool) {
	p := bc.Projection()
	if p.perceived == 0 || p.rate <= 0 {
		return 0, false, false
	}
	trail := at.Sub(blockTime)
	// A time this far off is not an old answer but a wrong one: a different
	// contract at the canary's address, a gateway's junk. Not graded.
	if trail > staleStateImplausible || trail < -staleStateImplausible {
		return 0, false, false
	}
	if trail > 0 {
		lag = uint64(trail.Seconds() * p.rate)
	}
	if at.Sub(p.headAt) > p.window {
		return lag, false, true
	}
	blockTimes := time.Duration(2 / p.rate * float64(time.Second))
	return lag, trail > blockTimes+staleStateSlack, true
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
