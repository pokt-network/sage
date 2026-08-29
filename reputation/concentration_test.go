package reputation

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// pool builds a candidate list holding counts[i] endpoints for operator i.
func pool(counts ...int) domain.EndpointAddrList {
	var out domain.EndpointAddrList
	for op, n := range counts {
		for i := 0; i < n; i++ {
			out = append(out, domain.EndpointAddr(
				fmt.Sprintf("pokt1s%d-%d-https://rpc-%d.op%d.net", op, i, i, op),
			))
		}
	}
	return out
}

// shares runs the capped pick many times and returns each operator's observed
// share of selections.
func shares(t *testing.T, cfg OperatorCapConfig, candidates domain.EndpointAddrList, draws int) map[string]float64 {
	t.Helper()
	hits := map[string]int{}
	for i := 0; i < draws; i++ {
		_, ep, ok := cappedPick(cfg, candidates, nil)
		if !ok {
			t.Fatalf("cappedPick reported it could not apply on a %d-endpoint pool", len(candidates))
		}
		hits[ep.Operator()]++
	}
	out := make(map[string]float64, len(hits))
	for op, n := range hits {
		out[op] = float64(n) / float64(draws)
	}
	return out
}

func approx(got, want, tol float64) bool { return math.Abs(got-want) <= tol }

// Uncapped, an operator holding 40 of 50 registrations takes 80% of the
// service. That is the blast radius the cap exists to bound.
func TestCappedPick_BoundsTheDominantOperator(t *testing.T) {
	// 40 / 5 / 5 across three operators.
	candidates := pool(40, 5, 5)
	got := shares(t, OperatorCapConfig{}, candidates, 20000)

	if got["op0.net"] > DefaultMaxOperatorShare+0.02 {
		t.Errorf("dominant operator share = %.3f, want <= %.2f", got["op0.net"], DefaultMaxOperatorShare)
	}
	// The excess has to land somewhere: the two small operators are each
	// entitled to 10% and can absorb up to 3x that.
	for _, op := range []string{"op1.net", "op2.net"} {
		if got[op] <= 0.10 {
			t.Errorf("%s share = %.3f, want more than its uncapped 0.10", op, got[op])
		}
	}
}

// A pool already within the cap must not be reshaped at all — the cap is a
// ceiling, not a redistribution policy.
func TestCappedPick_LeavesAnEvenPoolAlone(t *testing.T) {
	candidates := pool(10, 10, 10)
	got := shares(t, OperatorCapConfig{}, candidates, 20000)

	for _, op := range []string{"op0.net", "op1.net", "op2.net"} {
		if !approx(got[op], 1.0/3.0, 0.02) {
			t.Errorf("%s share = %.3f, want ~0.333", op, got[op])
		}
	}
}

// Registration-weighting is the default: below the cap, share follows the
// registrations held, not the number of distinct hostnames.
func TestCappedPick_FollowsRegistrationsBelowTheCap(t *testing.T) {
	// 4 / 3 / 3 = 0.40 / 0.30 / 0.30, all under the 0.50 cap.
	candidates := pool(4, 3, 3)
	got := shares(t, OperatorCapConfig{}, candidates, 20000)

	if !approx(got["op0.net"], 0.40, 0.02) {
		t.Errorf("op0 share = %.3f, want ~0.40", got["op0.net"])
	}
}

// A provider handed far more than its registrations entitle it to cannot serve
// it — each registration carries its own per-session allowance. The ceiling
// stops the cap from converting concentration into 429s.
func TestCappedPick_DisplacementCeilingHoldsExcessInPlace(t *testing.T) {
	// 49 / 1: the single-registration operator is entitled to 2%. Without a
	// ceiling the cap would hand it ~35% — 17x what it can serve.
	candidates := pool(49, 1)
	got := shares(t, OperatorCapConfig{}, candidates, 20000)

	small := got["op1.net"]
	entitlement := 1.0 / 50.0
	if small > DefaultDisplacementCeiling*entitlement+0.02 {
		t.Errorf("small operator share = %.3f, want <= %.3f (%.0fx its %.3f entitlement)",
			small, DefaultDisplacementCeiling*entitlement, DefaultDisplacementCeiling, entitlement)
	}
	// The excess nobody can absorb stays with the big operator rather than
	// vanishing: shares must still sum to 1.
	if !approx(got["op0.net"]+small, 1.0, 0.001) {
		t.Errorf("shares sum to %.4f, want 1", got["op0.net"]+small)
	}
	if got["op0.net"] <= DefaultTwoOperatorMaxShare {
		t.Errorf("unabsorbable excess should stay with op0, got share %.3f", got["op0.net"])
	}
}

// 0.50 x 2 = 1.0 is exactly the infeasibility boundary, so two-operator pools
// use the looser value rather than being forced to an even split.
func TestCappedPick_TwoOperatorPoolsUseTheLooserCap(t *testing.T) {
	// 9 / 1 with a generous ceiling so the ceiling is not what binds.
	candidates := pool(9, 1)
	cfg := OperatorCapConfig{DisplacementCeiling: -1}
	got := shares(t, cfg, candidates, 20000)

	if !approx(got["op0.net"], DefaultTwoOperatorMaxShare, 0.02) {
		t.Errorf("op0 share = %.3f, want ~%.2f", got["op0.net"], DefaultTwoOperatorMaxShare)
	}
}

// One operator holding the whole pool has nobody to redistribute to. Capping
// there would mean serving a fraction of requests from nowhere.
func TestCappedPick_SingleOperatorPoolIsNotCapped(t *testing.T) {
	candidates := pool(5)
	if _, _, ok := cappedPick(OperatorCapConfig{}, candidates, nil); ok {
		t.Error("cap should not apply to a single-operator pool")
	}
}

func TestCappedPick_DisabledAndDegenerateInputs(t *testing.T) {
	candidates := pool(5, 5)

	if _, _, ok := cappedPick(OperatorCapConfig{MaxShare: -1}, candidates, nil); ok {
		t.Error("a negative MaxShare must disable the cap")
	}
	if _, _, ok := cappedPick(OperatorCapConfig{}, nil, nil); ok {
		t.Error("an empty pool cannot be capped")
	}
	if _, _, ok := cappedPick(OperatorCapConfig{}, candidates[:1], nil); ok {
		t.Error("a one-endpoint pool cannot be capped")
	}
}

// The keep predicate is how the relay path caps within the winning reputation
// tier rather than across the whole session.
func TestCappedPick_KeepPredicateRestrictsThePool(t *testing.T) {
	candidates := pool(5, 5)
	keepIdx := map[int]bool{0: true, 5: true}
	only := map[domain.EndpointAddr]bool{candidates[0]: true, candidates[5]: true}

	for i := 0; i < 200; i++ {
		_, ep, ok := cappedPick(OperatorCapConfig{}, candidates, func(i int) bool {
			return keepIdx[i]
		})
		if !ok {
			t.Fatal("cap should apply across the two kept endpoints")
		}
		if !only[ep] {
			t.Fatalf("picked %q, which the predicate excluded", ep)
		}
	}
}

// --- integration through the selector --- //

func TestSelect_ConcentrationCapAppliesWithinTheWinningTier(t *testing.T) {
	candidates := pool(40, 5, 5)
	scores := map[domain.EndpointAddr]float64{}
	for _, ep := range candidates {
		scores[ep] = 95 // all tier 1
	}

	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))
	sel.SetOperatorCap(OperatorCapConfig{}, func(context.Context, domain.ServiceID) bool { return true })

	hits := map[string]int{}
	const draws = 20000
	for i := 0; i < draws; i++ {
		got := sel.Select(context.Background(), "eth", candidates, domain.RPCTypeJSONRPC)
		if len(got) != 1 {
			t.Fatalf("expected 1 endpoint, got %v", got)
		}
		hits[got[0].Operator()]++
	}

	share := float64(hits["op0.net"]) / draws
	if share > DefaultMaxOperatorShare+0.02 {
		t.Errorf("dominant operator share = %.3f, want <= %.2f", share, DefaultMaxOperatorShare)
	}
}

// A lower tier must not be promoted by the cap: the cap reshapes *within* the
// tier reputation already chose, it does not reach across tiers.
func TestSelect_ConcentrationCapNeverLeavesTheWinningTier(t *testing.T) {
	candidates := pool(3, 3)
	scores := map[domain.EndpointAddr]float64{}
	for i, ep := range candidates {
		if i < 3 {
			scores[ep] = 95 // tier 1: all of op0
		} else {
			scores[ep] = 55 // tier 2: all of op1
		}
	}

	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	cfg.Tier2Pct = 0 // The trickle also prepends tier 2, on purpose; this test is about the cap.
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))
	sel.SetOperatorCap(OperatorCapConfig{}, func(context.Context, domain.ServiceID) bool { return true })

	for i := 0; i < 500; i++ {
		got := sel.Select(context.Background(), "eth", candidates, domain.RPCTypeJSONRPC)
		if len(got) != 1 {
			t.Fatalf("expected 1 endpoint, got %v", got)
		}
		if got[0].Operator() != "op0.net" {
			t.Fatalf("cap promoted a tier-2 endpoint %q over tier 1", got[0])
		}
	}
}

// With no gate the cap is inert, so selection stays uniform over the tier and
// the dominant operator keeps its full 80%.
func TestSelect_ConcentrationCapIsOffByDefault(t *testing.T) {
	candidates := pool(40, 5, 5)
	scores := map[domain.EndpointAddr]float64{}
	for _, ep := range candidates {
		scores[ep] = 95
	}

	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	hits := map[string]int{}
	const draws = 10000
	for i := 0; i < draws; i++ {
		got := sel.Select(context.Background(), "eth", candidates, domain.RPCTypeJSONRPC)
		hits[got[0].Operator()]++
	}
	if share := float64(hits["op0.net"]) / draws; !approx(share, 0.80, 0.02) {
		t.Errorf("uncapped share = %.3f, want ~0.80", share)
	}
}

// Selection runs once per relay, and the cap ships on by default, so its cost
// on the hot path is part of whether it is shippable at all.
func BenchmarkSelect(b *testing.B) {
	candidates := pool(20, 15, 10, 5) // 50 endpoints across 4 operators
	scores := map[domain.EndpointAddr]float64{}
	for _, ep := range candidates {
		scores[ep] = 95
	}
	cfg := DefaultSelectorConfig()
	cfg.ProbationPct = 0
	ctx := context.Background()

	b.Run("uncapped", func(b *testing.B) {
		sel := NewTieredSelector(cfg, fixedScoreFn(scores))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sel.Select(ctx, "eth", candidates, domain.RPCTypeJSONRPC)
		}
	})

	b.Run("capped", func(b *testing.B) {
		sel := NewTieredSelector(cfg, fixedScoreFn(scores))
		sel.SetOperatorCap(OperatorCapConfig{}, func(context.Context, domain.ServiceID) bool { return true })
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sel.Select(ctx, "eth", candidates, domain.RPCTypeJSONRPC)
		}
	})
}
