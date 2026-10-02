package qos

import (
	"bytes"
	"fmt"
	"log/slog"
	"math"
	"strings"
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

	// The pool is 400 behind a trusted node with an allowance of 5: the
	// floor engages, and lifts perceived to floor minus allowance, the
	// lowest height the pool would be allowed to sit at.
	if got := bc.PerceivedBlock(); got != 495 {
		t.Fatalf("expected 495 (floor 500 minus allowance 5), got %d", got)
	}
}

// A trusted node merely ahead of the pool — by less than the allowance — is
// propagation, not a pool that is behind, and must not move perceived. On the
// canary this was robinhood's source 74 blocks ahead of every supplier with
// an allowance of 3000, and the old rule made every supplier look behind.
func TestBlockConsensus_ExternalFloor_WithinAllowanceDoesNotEngage(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	bc.gracePeriod = 0
	bc.graceStart = time.Now().Add(-time.Hour)

	// The floor is applied when perceived is recomputed, on observation.
	bc.SetExternalFloor(1074)
	bc.AddObservation("ep1", 1000)
	bc.AddObservation("ep2", 1000)
	if got := bc.PerceivedBlock(); got != 1000 {
		t.Fatalf("a source 74 ahead within an allowance of 100 moved perceived to %d; want 1000", got)
	}
	bc.SetExternalFloor(1150)
	bc.AddObservation("ep2", 1000)
	if got := bc.PerceivedBlock(); got != 1050 {
		t.Fatalf("a source 150 ahead should lift perceived to 1050 (floor minus allowance), got %d", got)
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

// TestBlockConsensus_Reset pins what an operator-triggered reset discards: the
// observation window, the derived perceived height, and the external floor —
// and that the grace period restarts, so a floor set again right after reset
// does not apply until a fresh grace window elapses.
func TestBlockConsensus_Reset(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("ep1", 100)
	bc.SetExternalFloor(50)
	if got := bc.PerceivedBlock(); got != 100 {
		t.Fatalf("perceived = %d before Reset, want 100", got)
	}

	bc.Reset()

	if got := bc.PerceivedBlock(); got != 0 {
		t.Fatalf("PerceivedBlock() = %d after Reset, want 0", got)
	}
	if got := bc.externalFloor.Load(); got != 0 {
		t.Fatalf("externalFloor = %d after Reset, want 0", got)
	}
	bc.mu.RLock()
	n := len(bc.observations)
	bc.mu.RUnlock()
	if n != 0 {
		t.Fatalf("observations = %d after Reset, want 0", n)
	}
	if bc.graceStart.Before(time.Now().Add(-time.Second)) {
		t.Fatalf("graceStart = %v, want restarted to roughly now", bc.graceStart)
	}
}

func TestEndpointHeights_LatestPerEndpoint(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("a", 100)
	bc.AddObservation("b", 101)
	bc.AddObservation("a", 102)
	got := bc.EndpointHeights()
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (one per endpoint): %+v", len(got), got)
	}
	byEP := map[domain.EndpointAddr]uint64{}
	for _, h := range got {
		byEP[h.Endpoint] = h.Height
	}
	if byEP["a"] != 102 || byEP["b"] != 101 {
		t.Errorf("heights = %v, want a=102 (latest) b=101", byEP)
	}
}

func TestAddObservation_WarnsOnAHeightFarBelowTheHead(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	bc := NewBlockConsensus(logger, 5)
	for _, ep := range []domain.EndpointAddr{"a", "b", "c"} {
		bc.AddObservation(ep, 1_000_000)
	}
	bc.AddObservation("d", 999_990) // lagging, not wrong
	if strings.Contains(buf.String(), "far below") {
		t.Fatalf("a lagging endpoint must not warn: %s", buf.String())
	}
	bc.AddObservation("e", 3) // the sui case: near zero
	if !strings.Contains(buf.String(), "far below") || !strings.Contains(buf.String(), `endpoint=e`) {
		t.Fatalf("a near-zero height entered the window without naming the endpoint: %s", buf.String())
	}
}

func TestChainView_ReportsTheExternalFloor(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation("a", 100)
	if got := bc.ChainView().ExternalFloor; got != 0 {
		t.Fatalf("no floor set, got %d", got)
	}
	bc.SetExternalFloor(150)
	if got := bc.ChainView().ExternalFloor; got != 150 {
		t.Fatalf("floor = %d, want 150 reported whether or not it has engaged", got)
	}
}

// An operator that stakes most of a pool on one lagging node must not become
// the median and cut the in-sync operators as outliers (mainnet metis,
// 2026-09-28: 41 of 50 endpoints on one operator 79,000 blocks behind).
func TestBlockConsensus_OneVotePerOperator(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)
	for i := range 41 {
		bc.AddObservation(domain.EndpointAddr(fmt.Sprintf("s%d-https://n%d.behind.net", i, i%7)), 1000)
	}
	for i := range 7 {
		bc.AddObservation(domain.EndpointAddr(fmt.Sprintf("k%d-https://rm0%d.fresh.tech", i, i%2)), 80_000)
	}
	bc.AddObservation("g1-https://s1.other.xyz", 80_001)
	bc.AddObservation("g2-https://s2.other.xyz", 80_000)
	if got := bc.PerceivedBlock(); got != 80_001 {
		t.Fatalf("perceived = %d, want 80001: two in-sync operators outvote one lagging operator however many endpoints it stakes", got)
	}
}

// One vote each must not hand the head to a liar: with two operators the
// lower median is the honest one, so the liar stays an outlier.
func TestBlockConsensus_TwoOperatorsLiarStaysOutlier(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)
	bc.AddObservation("h1-https://a.honest.net", 1000)
	bc.AddObservation("h2-https://b.honest.net", 1001)
	bc.AddObservation("x1-https://a.liar.io", 50_000)
	if got := bc.PerceivedBlock(); got != 1001 {
		t.Fatalf("perceived = %d, want 1001", got)
	}
}

// Two brands of one owner are one party: they cast one vote, so they cannot
// outvote a single independent operator.
func TestBlockConsensus_BrandsOfOneOwnerVoteOnce(t *testing.T) {
	for tag, brand := range map[string]string{"ba": "brand-a.io", "bb": "brand-b.io"} {
		for i := range 5 {
			domain.RecordOwner(fmt.Sprintf("pokt1%s%d", tag, i), "pokt1ownerx", brand)
		}
	}
	bc := NewBlockConsensus(nil, 10)
	bc.AddObservation("pokt1ba0-https://r1.brand-a.io", 50_000)
	bc.AddObservation("pokt1bb0-https://s1.brand-b.io", 50_000)
	bc.AddObservation("h1-https://a.honest.net", 1000)
	if got := bc.PerceivedBlock(); got != 1000 {
		t.Fatalf("perceived = %d, want 1000: one owner behind two brands is one vote", got)
	}
}

// An even split between parties in sync and parties behind resolves to the
// ones behind; the external floor is the tiebreaker.
func TestBlockConsensus_EvenSplitFavoursTheLaggingSide(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)
	bc.AddObservation("a1-https://n1.behind.net", 1000)
	bc.AddObservation("b1-https://rm01.fresh.tech", 80_000)
	if got := bc.PerceivedBlock(); got != 1000 {
		t.Fatalf("perceived = %d, want 1000", got)
	}
}

// A floor lifts perceived when it is set, not only on the next observation:
// a service whose endpoints report no height never makes one.
func TestBlockConsensus_FloorAppliesWithoutObservations(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.gracePeriod = 0
	bc.graceStart = time.Now().Add(-time.Hour)
	bc.SetExternalFloor(1000)
	if got := bc.PerceivedBlock(); got != 995 {
		t.Fatalf("perceived = %d, want 995 (floor minus allowance)", got)
	}
}

// Solana, 2026-09-29: one dead party, one stuck party and two at the head.
// Parties behind the head disagree with each other; they must not become the
// anchor and cut the head as an outlier.
func TestBlockConsensus_StaleMinorityPartiesAtDifferentHeights(t *testing.T) {
	bc := NewBlockConsensus(nil, 1500)
	for i := range 9 {
		bc.AddObservation(domain.EndpointAddr(fmt.Sprintf("k%d-https://n%d.head-a.network", i, i)), 429_671_368)
	}
	for i := range 27 {
		bc.AddObservation(domain.EndpointAddr(fmt.Sprintf("n%d-https://n%d.head-b.net", i, i)), 429_671_360)
	}
	for i := range 3 {
		bc.AddObservation(domain.EndpointAddr(fmt.Sprintf("d%d-https://n%d.dead.net", i, i)), 428_197_468)
	}
	for i := range 6 {
		bc.AddObservation(domain.EndpointAddr(fmt.Sprintf("s%d-https://r%d.stuck.xyz", i, i)), 429_657_005)
	}
	if got := bc.PerceivedBlock(); got != 429_671_368 {
		t.Fatalf("perceived = %d, want 429671368: two parties at the head outvote two stale ones", got)
	}
}

// Three parties, one behind: the two in sync anchor the head.
func TestBlockConsensus_ThreePartiesOneStale(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)
	bc.AddObservation("a1-https://x.fresh-a.net", 5000)
	bc.AddObservation("b1-https://x.fresh-b.net", 5002)
	bc.AddObservation("c1-https://x.behind.net", 3000)
	if got := bc.PerceivedBlock(); got != 5002 {
		t.Fatalf("perceived = %d, want 5002", got)
	}
}

// The price of corroboration, pinned: two parties agreeing on a height above
// the rest lift the head to it.
func TestBlockConsensus_TwoAgreeingPartiesAnchor(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)
	bc.AddObservation("a1-https://x.one.net", 1000)
	bc.AddObservation("b1-https://x.two.net", 1000)
	bc.AddObservation("c1-https://x.three.net", 1000)
	bc.AddObservation("l1-https://x.high-a.io", 2000)
	bc.AddObservation("l2-https://x.high-b.io", 2001)
	if got := bc.PerceivedBlock(); got != 2001 {
		t.Fatalf("perceived = %d, want 2001", got)
	}
}

// AnswerLag measures against the head advanced at the block rate since it
// last moved, and calls a lag stale only past max(2 blocks, 10s of blocks).
func TestBlockConsensus_AnswerLag(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	if _, _, ok := bc.AnswerLag(10, time.Now()); ok {
		t.Fatal("no head yet: ok must be false")
	}
	bc.AddObservation("a1-https://x.a.net", 1000)
	now := time.Now()
	bc.mu.Lock()
	bc.rateSamples = []rateSample{{height: 900, at: now.Add(-44 * time.Second)}, {height: 1000, at: now.Add(-4 * time.Second)}}
	bc.mu.Unlock()
	// rate 2.5/s, head moved 4s ago: expected head 1010, tolerance 25.
	for _, tc := range []struct {
		answer    uint64
		lag       uint64
		wantStale bool
	}{
		{1010, 0, false},
		{1020, 0, false},
		{990, 20, false},
		{900, 110, true},
	} {
		lag, stale, ok := bc.AnswerLag(tc.answer, now)
		if !ok || lag != tc.lag || stale != tc.wantStale {
			t.Errorf("answer %d: lag %d stale %v ok %v, want lag %d stale %v", tc.answer, lag, stale, ok, tc.lag, tc.wantStale)
		}
	}
}

// A head that has not moved for a window is a halted chain or a stalled
// measurement, not everyone's cache: lag is reported, nothing is stale.
func TestBlockConsensus_AnswerLagNoVerdictOnAStalledHead(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	bc.AddObservation("a1-https://x.a.net", 1000)
	now := time.Now()
	bc.mu.Lock()
	moved := now.Add(-bc.windowDuration - time.Second)
	bc.rateSamples = []rateSample{{height: 900, at: moved.Add(-40 * time.Second)}, {height: 1000, at: moved}}
	bc.mu.Unlock()
	lag, stale, ok := bc.AnswerLag(1000, now)
	if !ok || stale || lag == 0 {
		t.Fatalf("lag %d stale %v ok %v, want a lag and no verdict", lag, stale, ok)
	}
}

// StateLag grades a block timestamp against the clock: two block times plus
// the slack, no verdict without a rate or once the head has stalled.
func TestBlockConsensus_StateLag(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	now := time.Now()
	if _, _, ok := bc.StateLag(now, now); ok {
		t.Fatal("no head: ok must be false")
	}
	bc.AddObservation("a1-https://x.a.net", 1000)
	bc.mu.Lock()
	bc.rateSamples = []rateSample{{height: 990, at: now.Add(-24 * time.Second)}, {height: 1000, at: now.Add(-4 * time.Second)}}
	bc.mu.Unlock()
	// rate 0.5/s: two blocks are 4s, stale past 14s.
	if lag, stale, ok := bc.StateLag(now.Add(-10*time.Second), now); !ok || stale || lag != 5 {
		t.Errorf("10s old: lag %d stale %v ok %v, want 5 false true", lag, stale, ok)
	}
	if _, stale, _ := bc.StateLag(now.Add(-20*time.Second), now); !stale {
		t.Error("20s old must be stale")
	}
	if _, stale, ok := bc.StateLag(now.Add(-time.Hour), now.Add(bc.windowDuration+time.Minute)); !ok || stale {
		t.Error("a stalled head gives no verdict")
	}
}

// A block time a day or more from the clock is junk, not an old answer.
func TestBlockConsensus_StateLagIgnoresAnImplausibleTime(t *testing.T) {
	bc := NewBlockConsensus(nil, 100)
	bc.AddObservation("a1-https://x.a.net", 1000)
	now := time.Now()
	bc.mu.Lock()
	bc.rateSamples = []rateSample{{height: 990, at: now.Add(-24 * time.Second)}, {height: 1000, at: now.Add(-4 * time.Second)}}
	bc.mu.Unlock()
	if _, _, ok := bc.StateLag(time.Unix(1, 0), now); ok {
		t.Fatal("a 1970 timestamp must not be a reading")
	}
}
