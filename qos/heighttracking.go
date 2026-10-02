package qos

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
)

// HeightTracking is the block consensus and runtime sync allowance a
// height-aware plugin keeps, with the extension methods that only delegate to
// them: EndpointHeightLister, ExternalFloorSetter, ChainViewer,
// HeightObserver, SyncAllowanceTuner and the read half of BlockHeightTracker.
// A plugin embeds it, sets Consensus in its constructor and calls
// SetSyncAllowance once with the configured value.
//
// The sync allowance here is the plugin's selection bound, the value the
// tuning knob moves. It is deliberately not BlockConsensus's own allowance,
// which is fixed at construction; embedding *BlockConsensus directly would
// promote methods with that other meaning.
type HeightTracking struct {
	// Consensus owns the height observations and the perceived head.
	Consensus     *BlockConsensus
	syncAllowance atomic.Uint64
}

// EndpointHeights reports the latest height each endpoint supplied
// (EndpointHeightLister).
func (h *HeightTracking) EndpointHeights() []EndpointHeight { return h.Consensus.EndpointHeights() }

// SetExternalFloor takes a trusted outside height as the floor the perceived
// head may not fall below (ExternalFloorSetter).
func (h *HeightTracking) SetExternalFloor(height uint64) { h.Consensus.SetExternalFloor(height) }

// PerceivedBlockHeight returns the consensus head.
func (h *HeightTracking) PerceivedBlockHeight() uint64 { return h.Consensus.PerceivedBlock() }

// StartSync is a no-op: health checks and client traffic drive height
// updates. A plugin with background work defines its own.
func (h *HeightTracking) StartSync(_ context.Context) {}

// ChainView reports what this service currently believes about its chain, for
// the metrics exporter (ChainViewer).
func (h *HeightTracking) ChainView() ChainView { return h.Consensus.ChainView() }

// LastHeightObservation reports when any of these endpoints last supplied a
// block height, so the executor can tell whether its height probe would learn
// anything the plugin does not already know (HeightObserver).
func (h *HeightTracking) LastHeightObservation(endpoints domain.EndpointAddrList) (time.Time, bool) {
	return h.Consensus.LastHeightObservation(endpoints)
}

// SyncAllowance implements SyncAllowanceTuner.
func (h *HeightTracking) SyncAllowance() uint64 { return h.syncAllowance.Load() }

// SetSyncAllowance implements SyncAllowanceTuner: the tuning knob
// qos.sync_allowance, per service, without a restart.
func (h *HeightTracking) SetSyncAllowance(blocks uint64) { h.syncAllowance.Store(blocks) }

// AllStaleBy reports whether every endpoint in eps is known, by getHeight, to
// sit below the relaxed height bound: twice the sync allowance behind the
// perceived head. A plugin's StaleChecker passes its own store's heights.
func (h *HeightTracking) AllStaleBy(eps domain.EndpointAddrList, getHeight func(domain.EndpointAddr) (uint64, bool)) bool {
	return AllStale(eps, getHeight, MinAllowedHeight(h.Consensus.PerceivedBlock(), h.syncAllowance.Load()*2))
}
