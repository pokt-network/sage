package qos

import (
	"testing"
	"time"
)

// The tuning knob moves the consensus's allowance too: a service retuned at
// runtime lifts its perceived head to floor minus the new allowance, as one
// configured with that value from boot would.
func TestHeightTracking_SetSyncAllowanceMovesConsensus(t *testing.T) {
	h := &HeightTracking{Consensus: NewBlockConsensus(nil, 5)}
	h.SetSyncAllowance(5)
	h.Consensus.gracePeriod = 0
	h.Consensus.graceStart = time.Now().Add(-time.Hour)
	h.SetExternalFloor(500)

	h.SetSyncAllowance(50)
	h.Consensus.AddObservation("ep1", 100)

	if got := h.PerceivedBlockHeight(); got != 450 {
		t.Fatalf("perceived = %d, want 450 (floor 500 minus the retuned allowance 50)", got)
	}
}
