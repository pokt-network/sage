package qos

import (
	"math"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestBlockConsensus_SingleObservation(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("ep1", 100)
	if got := bc.PerceivedBlock(); got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
}

func TestBlockConsensus_MedianAndMax(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	// Heights: 95, 98, 100, 102, 105
	// Median: 100, outlier cap: 100 + 15 = 115
	// All pass, perceived = max = 105
	for _, h := range []uint64{95, 98, 100, 102, 105} {
		bc.AddObservation(domain.EndpointAddr("ep"), h)
	}
	if got := bc.PerceivedBlock(); got != 105 {
		t.Fatalf("expected 105, got %d", got)
	}
}

func TestBlockConsensus_OutlierFiltering(t *testing.T) {
	bc := NewBlockConsensus(nil, 5) // outlier cap = median + 15
	// Heights: 100, 101, 102, 200 (outlier)
	// Median of [100,101,102,200] = 102 (index 2)
	// Outlier cap: 102 + 15 = 117, so 200 is excluded
	// Perceived = 102
	for _, h := range []uint64{100, 101, 102, 200} {
		bc.AddObservation(domain.EndpointAddr("ep"), h)
	}
	if got := bc.PerceivedBlock(); got != 102 {
		t.Fatalf("expected 102, got %d", got)
	}
}

func TestBlockConsensus_ZeroHeightIgnored(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("ep1", 0) // Should be ignored.
	if got := bc.PerceivedBlock(); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
	bc.AddObservation("ep1", 50)
	if got := bc.PerceivedBlock(); got != 50 {
		t.Fatalf("expected 50, got %d", got)
	}
}

func TestBlockConsensus_ExternalFloor_DuringGrace(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.gracePeriod = 1 * time.Hour // Extend grace period so it's definitely active.
	bc.graceStart = time.Now()

	bc.SetExternalFloor(500)
	bc.AddObservation("ep1", 100)

	// During grace period, external floor should NOT be applied.
	if got := bc.PerceivedBlock(); got != 100 {
		t.Fatalf("expected 100 during grace, got %d", got)
	}
}

func TestBlockConsensus_ExternalFloor_AfterGrace(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.gracePeriod = 0 // No grace period.
	bc.graceStart = time.Now().Add(-time.Hour)

	bc.SetExternalFloor(500)
	bc.AddObservation("ep1", 100)

	// External floor > perceived, should use floor.
	if got := bc.PerceivedBlock(); got != 500 {
		t.Fatalf("expected 500, got %d", got)
	}
}

func TestBlockConsensus_ExternalFloor_LowerThanPerceived(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.gracePeriod = 0
	bc.graceStart = time.Now().Add(-time.Hour)

	bc.SetExternalFloor(50)
	bc.AddObservation("ep1", 100)

	// Perceived > floor, should keep perceived.
	if got := bc.PerceivedBlock(); got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
}

func TestBlockConsensus_SetSyncAllowance(t *testing.T) {
	bc := NewBlockConsensus(nil, 0) // Very tight: cap = median + 0
	for _, h := range []uint64{100, 101, 102, 110} {
		bc.AddObservation(domain.EndpointAddr("ep"), h)
	}
	// Sorted: [100,101,102,110], median (index 2) = 102, cap = 102+0 = 102.
	// Heights <= 102: 100, 101, 102. Perceived = 102.
	if got := bc.PerceivedBlock(); got != 102 {
		t.Fatalf("expected 102, got %d", got)
	}

	// Relax sync allowance.
	bc.SetSyncAllowance(5) // cap = 101 + 15 = 116. All pass.
	bc.AddObservation("ep", 103)
	// After adding 103: heights = [100,101,102,110,103], sorted [100,101,102,103,110]
	// Median=102, cap=102+15=117. All pass. Max=110.
	if got := bc.PerceivedBlock(); got != 110 {
		t.Fatalf("expected 110, got %d", got)
	}
}

func TestBlockConsensus_AtomicRead(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	// PerceivedBlock should be safe to call concurrently.
	bc.AddObservation("ep", 42)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			_ = bc.PerceivedBlock()
		}
		close(done)
	}()
	for i := 0; i < 1000; i++ {
		bc.AddObservation("ep", uint64(42+i))
	}
	<-done
}

func TestBlockConsensus_WindowPruning(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.windowDuration = 10 * time.Millisecond

	bc.AddObservation("ep", 100)
	time.Sleep(20 * time.Millisecond)
	// Old observation should be pruned on next add.
	bc.AddObservation("ep", 200)
	if got := bc.PerceivedBlock(); got != 200 {
		t.Fatalf("expected 200, got %d", got)
	}
}

// --- implausible height / overflow ---

// THE attack. heights[len/2] takes the upper median on even counts, so one liar
// out of two observations drags the median to its own value. With that value at
// MaxUint64, `median + syncAllowance*3` wrapped to 299: every honest height then
// exceeded the outlier cap, nothing survived the filter, and perceived fell to
// 0. Zero is what every plugin reads as cold start — so they respond by turning
// block-height filtering OFF, and the liar has disabled the very check meant to
// catch it. Fail-open, from a two-endpoint session.
func TestBlockConsensus_ImplausibleHeightCannotCollapsePerceived(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	bc.AddObservation("honest", 20_000_000)
	bc.AddObservation("attacker", math.MaxUint64)

	if got := bc.PerceivedBlock(); got != 20_000_000 {
		t.Errorf("perceived = %d, want 20000000 — the liar must not move it", got)
	}
}

// The liar holding a majority must not help it either: the guard is at ingress,
// so an implausible height never reaches the median regardless of how many
// endpoints report it.
func TestBlockConsensus_ImplausibleHeightMajority(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	for _, h := range []uint64{20_000_000, 20_000_001, 20_000_002} {
		bc.AddObservation("honest", h)
	}
	for i := 0; i < 5; i++ {
		bc.AddObservation("attacker", math.MaxUint64)
	}

	if got := bc.PerceivedBlock(); got != 20_000_002 {
		t.Errorf("perceived = %d, want 20000002 even with the liars in the majority", got)
	}
}

// A height just over the ceiling is refused; one just under is ordinary data.
// The boundary matters because the ceiling is what keeps every downstream sum
// clear of MaxUint64.
func TestBlockConsensus_PlausibilityBoundary(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("ep1", MaxPlausibleBlockHeight)
	if got := bc.PerceivedBlock(); got != MaxPlausibleBlockHeight {
		t.Errorf("perceived = %d, want the ceiling itself to be accepted", got)
	}

	bc2 := NewBlockConsensus(nil, 5)
	bc2.AddObservation("ep1", MaxPlausibleBlockHeight+1)
	if got := bc2.PerceivedBlock(); got != 0 {
		t.Errorf("perceived = %d, want 0 — one over the ceiling is refused", got)
	}
}

// syncAllowance is operator-set and unbounded, so the cap arithmetic must hold
// even where the plausibility ceiling is not what protects it.
//
// The inputs are picked to discriminate, which took a second attempt: at
// MaxUint64/2 the product wraps to a still-enormous number and every height
// passes anyway, so the test succeeds whether or not the bug is present.
// MaxUint64/3+1 is the value that wraps to exactly 2, dragging the cap down to
// median+2 and hiding an honest tip above it.
func TestBlockConsensus_AbsurdSyncAllowanceDoesNotWrap(t *testing.T) {
	bc := NewBlockConsensus(nil, math.MaxUint64/3+1)
	bc.AddObservation("ep1", 100)
	bc.AddObservation("ep2", 200)
	bc.AddObservation("ep3", 300)

	// An allowance this large means "never filter anyone", so the cap saturates
	// and the tip wins. Wrapping would return 200 — the median — instead.
	if got := bc.PerceivedBlock(); got != 300 {
		t.Errorf("perceived = %d, want 300; a wrapped cap hides the tip at median+2", got)
	}
}

func TestBlockConsensus_ZeroHeightStillIgnored(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("ep1", 100)
	bc.AddObservation("ep2", 0)
	if got := bc.PerceivedBlock(); got != 100 {
		t.Errorf("perceived = %d, want 100 — zero is not an observation", got)
	}
}
