package reputation

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

func fixedScoreFn(scores map[domain.EndpointAddr]float64) ScoreFn {
	return func(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType) (float64, bool) {
		v, ok := scores[ep]
		return v, ok
	}
}

func TestTieredSelector_Tier1(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"ep1": 95,
		"ep2": 90,
		"ep3": 60,
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0 // Disable probation for determinism.
	cfg.Tier2Pct = 0     // And tier 2's traffic share: with it, ep3 is a legitimate pick 5% of the time.
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	endpoints := domain.EndpointAddrList{"ep1", "ep2", "ep3"}
	result := sel.Select(context.Background(), "eth", endpoints, domain.RPCTypeJSONRPC)

	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	// Should select from tier 1 (ep1 or ep2).
	if result[0] != "ep1" && result[0] != "ep2" {
		t.Errorf("expected tier 1 endpoint, got %s", result[0])
	}
}

func TestTieredSelector_CascadeToTier2(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"ep1": 60,
		"ep2": 55,
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1", "ep2"}, domain.RPCTypeJSONRPC)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0] != "ep1" && result[0] != "ep2" {
		t.Errorf("expected tier 2 endpoint, got %s", result[0])
	}
}

func TestTieredSelector_CascadeToTier3(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"ep1": 35,
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1"}, domain.RPCTypeJSONRPC)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0] != "ep1" {
		t.Errorf("expected ep1, got %s", result[0])
	}
}

// Sub-threshold endpoints are filtered out whenever anything better exists.
func TestTieredSelector_FilteredOut(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"bad":     5, // Below minThreshold (10).
		"healthy": 90,
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	for i := 0; i < 50; i++ {
		result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"bad", "healthy"}, domain.RPCTypeJSONRPC)
		if len(result) != 1 || result[0] != "healthy" {
			t.Fatalf("expected [healthy], got %v", result)
		}
	}
}

// Pool collapse: every endpoint below the floor must still yield the least-bad
// one. Returning nothing would be a total outage caused by reputation alone.
func TestTieredSelector_PoolCollapseServesLeastBad(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"worst":    1,
		"bad":      5,
		"leastBad": 9, // Still below minThreshold (10).
	}
	cfg := DefaultSelectorConfig()
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	var collapsed int
	sel.SetCollapseHook(func(domain.ServiceID) { collapsed++ })

	eps := domain.EndpointAddrList{"worst", "bad", "leastBad"}
	for i := 0; i < 50; i++ {
		result := sel.Select(context.Background(), "eth", eps, domain.RPCTypeJSONRPC)
		if len(result) != 1 || result[0] != "leastBad" {
			t.Fatalf("expected [leastBad], got %v", result)
		}
	}
	if collapsed != 50 {
		t.Errorf("expected collapse hook to fire on every selection, got %d", collapsed)
	}
}

// Ties at the least-bad score are spread over, not pinned to the first one.
func TestTieredSelector_PoolCollapseSpreadsTies(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{"a": 5, "b": 5, "c": 5}
	sel := NewTieredSelector(DefaultSelectorConfig(), fixedScoreFn(scores))

	seen := map[domain.EndpointAddr]bool{}
	for i := 0; i < 200; i++ {
		result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"a", "b", "c"}, domain.RPCTypeJSONRPC)
		if len(result) != 1 {
			t.Fatalf("expected 1 result, got %v", result)
		}
		seen[result[0]] = true
	}
	if len(seen) != 3 {
		t.Errorf("expected all 3 tied endpoints to be selected over 200 draws, got %v", seen)
	}
}

func TestTieredSelector_TopTierCandidates_PoolCollapse(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{"a": 5, "b": 5, "c": 1}
	sel := NewTieredSelector(DefaultSelectorConfig(), fixedScoreFn(scores))

	got := sel.TopTierCandidates(context.Background(), "eth", domain.EndpointAddrList{"a", "b", "c"}, domain.RPCTypeJSONRPC)
	if len(got) != 2 || !got.Contains("a") || !got.Contains("b") {
		t.Errorf("expected the two least-bad endpoints [a b], got %v", got)
	}
}

func TestTieredSelector_ProbationPrepend(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"healthy": 90,
		"probe":   20, // Between minThreshold(10) and probationThreshold(30).
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 100 // Always include probation for test determinism.
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"healthy", "probe"}, domain.RPCTypeJSONRPC)

	if len(result) != 2 {
		t.Fatalf("expected 2 results (probation + healthy), got %d: %v", len(result), result)
	}
	if result[0] != "probe" {
		t.Errorf("expected probation endpoint first, got %s", result[0])
	}
	if result[1] != "healthy" {
		t.Errorf("expected healthy endpoint second, got %s", result[1])
	}
}

func TestTieredSelector_ProbationDoesNotReplace(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"healthy": 90,
		"probe":   20,
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 100
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"healthy", "probe"}, domain.RPCTypeJSONRPC)

	// The healthy endpoint must always be present (probation prepends, not replaces).
	found := false
	for _, ep := range result {
		if ep == "healthy" {
			found = true
		}
	}
	if !found {
		t.Errorf("healthy endpoint missing from result: %v", result)
	}
}

func TestTieredSelector_AllProbation(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"ep1": 15,
		"ep2": 20,
	}
	cfg := DefaultSelectorConfig()
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1", "ep2"}, domain.RPCTypeJSONRPC)
	if len(result) != 1 {
		t.Fatalf("expected 1 result when all in probation, got %d", len(result))
	}
}

func TestTieredSelector_Empty(t *testing.T) {
	cfg := DefaultSelectorConfig()
	sel := NewTieredSelector(cfg, fixedScoreFn(nil))

	result := sel.Select(context.Background(), "eth", nil, domain.RPCTypeJSONRPC)
	if result != nil {
		t.Errorf("expected nil for empty endpoints, got %v", result)
	}
}

func TestTieredSelector_UnknownEndpointGoesToTier3(t *testing.T) {
	// ep_unknown not in score map -> tier 3.
	scores := map[domain.EndpointAddr]float64{}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep_unknown"}, domain.RPCTypeJSONRPC)
	if len(result) != 1 || result[0] != "ep_unknown" {
		t.Errorf("expected unknown endpoint in tier 3, got %v", result)
	}
}

func TestTieredSelector_Tier2TricklePrependsWhenTier1Wins(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"top":    90,
		"parked": 79, // Tier 2: one probe under the tier-1 line.
	}
	cfg := DefaultSelectorConfig()
	cfg.Tier2Pct = 100 // Always trickle for determinism.
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"top", "parked"}, domain.RPCTypeJSONRPC)

	if len(result) != 2 || result[0] != "parked" || result[1] != "top" {
		t.Fatalf("expected [parked top] (tier-2 first, tier-1 pick behind it), got %v", result)
	}
}

func TestTieredSelector_Tier2TrickleShare(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{"top": 90, "parked": 79}
	sel := NewTieredSelector(DefaultSelectorConfig(), fixedScoreFn(scores)) // Tier2Pct 5.

	const n = 20000
	trickled := 0
	for range n {
		r := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"top", "parked"}, domain.RPCTypeJSONRPC)
		if r[0] == "parked" {
			trickled++
		}
	}
	// 5% of 20000 = 1000, sd ≈ 31.
	if trickled < 800 || trickled > 1200 {
		t.Fatalf("tier-2 first on %d/%d selections, want about 5%%", trickled, n)
	}
}

func TestTieredSelector_Tier2TrickleOffAndInapplicable(t *testing.T) {
	ctx := context.Background()

	// Off: a tier-2 endpoint is never tried first.
	cfg := DefaultSelectorConfig()
	cfg.Tier2Pct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(map[domain.EndpointAddr]float64{"top": 90, "parked": 79}))
	for range 500 {
		if r := sel.Select(ctx, "eth", domain.EndpointAddrList{"top", "parked"}, domain.RPCTypeJSONRPC); len(r) != 1 || r[0] != "top" {
			t.Fatalf("trickle off, expected [top], got %v", r)
		}
	}

	// Tier 1 empty: tier 2 is the winning tier and carries everything already,
	// so the trickle must not double it up.
	cfg.Tier2Pct = 100
	sel = NewTieredSelector(cfg, fixedScoreFn(map[domain.EndpointAddr]float64{"a": 70, "b": 60}))
	if r := sel.Select(ctx, "eth", domain.EndpointAddrList{"a", "b"}, domain.RPCTypeJSONRPC); len(r) != 1 {
		t.Fatalf("tier 2 winning, expected one pick, got %v", r)
	}

	// Tier 2 empty: nothing to trickle to, tier-1 pick alone.
	sel = NewTieredSelector(cfg, fixedScoreFn(map[domain.EndpointAddr]float64{"top": 90, "bad": 40}))
	if r := sel.Select(ctx, "eth", domain.EndpointAddrList{"top", "bad"}, domain.RPCTypeJSONRPC); len(r) != 1 || r[0] != "top" {
		t.Fatalf("no tier-2 endpoint, expected [top], got %v", r)
	}
}
