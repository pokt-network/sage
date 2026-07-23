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

	externalFloor atomic.Uint64 // from external block sources
	graceStart    time.Time
	gracePeriod   time.Duration
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
	bc.mu.Unlock()

	bc.perceived.Store(perceived)
}

// PerceivedBlock returns the current perceived block height (atomic, zero-contention).
func (bc *BlockConsensus) PerceivedBlock() uint64 {
	return bc.perceived.Load()
}

// SetExternalFloor sets the external block height floor (e.g., from external block sources).
func (bc *BlockConsensus) SetExternalFloor(height uint64) {
	bc.externalFloor.Store(height)
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
