package qos

import (
	"fmt"
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestSelect_NormalFiltering(t *testing.T) {
	eps := domain.EndpointAddrList{"a", "b", "c"}

	// Filter out "b".
	filters := []FilterFunc{
		func(ep domain.EndpointAddr) error {
			if ep == "b" {
				return fmt.Errorf("excluded")
			}
			return nil
		},
	}

	result := Select(eps, filters, nil, nil, nil)
	if result.Degraded {
		t.Fatal("should not be degraded")
	}
	if result.Tier != 1 {
		t.Fatalf("expected tier 1, got %d", result.Tier)
	}
	if len(result.Endpoints) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(result.Endpoints))
	}
}

func TestSelect_TierCascade(t *testing.T) {
	eps := domain.EndpointAddrList{"a", "b", "c"}

	// Tier 1: filter out everything.
	strictFilters := []FilterFunc{
		func(_ domain.EndpointAddr) error { return fmt.Errorf("nope") },
	}

	// Tier 2 (relaxed): allow "a" only.
	relaxedFilters := []FilterFunc{
		func(ep domain.EndpointAddr) error {
			if ep == "a" {
				return nil
			}
			return fmt.Errorf("nope")
		},
	}

	result := Select(eps, strictFilters, relaxedFilters, nil, nil)
	if !result.Degraded {
		t.Fatal("expected degraded")
	}
	if result.Tier != 2 {
		t.Fatalf("expected tier 2, got %d", result.Tier)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0] != "a" {
		t.Fatalf("expected [a], got %v", result.Endpoints)
	}
}

func TestSelect_FallbackToTier3(t *testing.T) {
	eps := domain.EndpointAddrList{"a", "b"}

	rejectAll := []FilterFunc{
		func(_ domain.EndpointAddr) error { return fmt.Errorf("nope") },
	}
	allowA := []FilterFunc{
		func(ep domain.EndpointAddr) error {
			if ep == "a" {
				return nil
			}
			return fmt.Errorf("nope")
		},
	}

	result := Select(eps, rejectAll, rejectAll, allowA, nil)
	if !result.Degraded {
		t.Fatal("expected degraded")
	}
	if result.Tier != 3 {
		t.Fatalf("expected tier 3, got %d", result.Tier)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0] != "a" {
		t.Fatalf("expected [a], got %v", result.Endpoints)
	}
}

func TestSelect_FallbackToOriginal(t *testing.T) {
	eps := domain.EndpointAddrList{"a", "b", "c"}

	rejectAll := []FilterFunc{
		func(_ domain.EndpointAddr) error { return fmt.Errorf("nope") },
	}

	result := Select(eps, rejectAll, rejectAll, rejectAll, nil)
	if !result.Degraded {
		t.Fatal("expected degraded")
	}
	if result.Tier != 3 {
		t.Fatalf("expected tier 3, got %d", result.Tier)
	}
	if len(result.Endpoints) != 3 {
		t.Fatalf("expected original 3 endpoints, got %d", len(result.Endpoints))
	}
}

func TestSelect_EmptyInput(t *testing.T) {
	result := Select(nil, nil, nil, nil, nil)
	if result.Endpoints != nil {
		t.Fatal("expected nil for empty input")
	}
	if result.Degraded {
		t.Fatal("should not be degraded for empty input")
	}
}

func TestSelect_NoFilters(t *testing.T) {
	eps := domain.EndpointAddrList{"a", "b"}
	result := Select(eps, nil, nil, nil, nil)
	if len(result.Endpoints) != 2 {
		t.Fatalf("expected 2, got %d", len(result.Endpoints))
	}
	if result.Tier != 1 {
		t.Fatalf("expected tier 1, got %d", result.Tier)
	}
}

func TestLeastStaleFallback(t *testing.T) {
	heights := map[domain.EndpointAddr]uint64{
		"fresh":   100, // lag 0
		"fresh2":  100, // lag 0 (ties with fresh)
		"behind":  95,  // lag 5
		"stale":   50,  // lag 50
		"unknown": 0,   // no data
	}
	getHeight := func(ep domain.EndpointAddr) (uint64, bool) {
		h, ok := heights[ep]
		if !ok || h == 0 {
			return 0, false
		}
		return h, true
	}

	t.Run("narrows to least-stale band, keeping ties", func(t *testing.T) {
		ranker := LeastStaleFallback(getHeight, 100)
		got := ranker(domain.EndpointAddrList{"fresh", "fresh2", "behind", "stale", "unknown"})
		if len(got) != 2 {
			t.Fatalf("expected 2 least-stale endpoints, got %v", got)
		}
		set := map[domain.EndpointAddr]bool{got[0]: true, got[1]: true}
		if !set["fresh"] || !set["fresh2"] {
			t.Fatalf("expected freshest tied endpoints, got %v", got)
		}
	})

	t.Run("known heights beat unknowns", func(t *testing.T) {
		ranker := LeastStaleFallback(getHeight, 100)
		got := ranker(domain.EndpointAddrList{"behind", "unknown"})
		if len(got) != 1 || got[0] != "behind" {
			t.Fatalf("expected [behind] (known beats unknown), got %v", got)
		}
	})

	t.Run("cold start returns input unchanged", func(t *testing.T) {
		ranker := LeastStaleFallback(getHeight, 0)
		in := domain.EndpointAddrList{"fresh", "stale"}
		got := ranker(in)
		if len(got) != 2 {
			t.Fatalf("cold start should not narrow, got %v", got)
		}
	})

	t.Run("no known heights returns input unchanged", func(t *testing.T) {
		ranker := LeastStaleFallback(getHeight, 100)
		got := ranker(domain.EndpointAddrList{"unknown"})
		if len(got) != 1 {
			t.Fatalf("expected unchanged, got %v", got)
		}
	})
}

func TestSelect_FallbackRankerNarrowsDegradedSet(t *testing.T) {
	eps := domain.EndpointAddrList{"fresh", "stale"}
	heights := map[domain.EndpointAddr]uint64{"fresh": 100, "stale": 1}
	getHeight := func(ep domain.EndpointAddr) (uint64, bool) {
		h, ok := heights[ep]
		return h, ok
	}
	rejectAll := []FilterFunc{
		func(_ domain.EndpointAddr) error { return fmt.Errorf("nope") },
	}

	// All tiers reject → final fallback. Ranker must narrow to the freshest.
	result := Select(eps, rejectAll, rejectAll, rejectAll, LeastStaleFallback(getHeight, 100))
	if !result.Degraded || result.Tier != 3 {
		t.Fatalf("expected degraded tier 3, got degraded=%v tier=%d", result.Degraded, result.Tier)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0] != "fresh" {
		t.Fatalf("expected fallback narrowed to [fresh], got %v", result.Endpoints)
	}
}

func TestBlockHeightFilter(t *testing.T) {
	heights := map[domain.EndpointAddr]uint64{
		"a": 100,
		"b": 90,
		"c": 80,
	}

	getHeight := func(ep domain.EndpointAddr) (uint64, bool) {
		h, ok := heights[ep]
		return h, ok
	}

	// Min height = 90: "c" excluded.
	f := BlockHeightFilter(getHeight, 90)

	if err := f("a"); err != nil {
		t.Fatalf("a should pass: %v", err)
	}
	if err := f("b"); err != nil {
		t.Fatalf("b should pass: %v", err)
	}
	if err := f("c"); err == nil {
		t.Fatal("c should be filtered")
	}

	// Unknown endpoint should pass.
	if err := f("unknown"); err != nil {
		t.Fatalf("unknown should pass: %v", err)
	}
}

// Zero allowance means "do not filter on height", never "require the tip".
// Getting this backwards collapses the pool onto whoever reported last — see
// MinAllowedHeight's comment for why that starves everyone else.
func TestMinAllowedHeight(t *testing.T) {
	cases := []struct {
		name      string
		perceived uint64
		allowance uint64
		want      uint64
	}{
		{"zero allowance does not filter", 1_000_000, 0, 0},
		{"cold start does not filter", 0, 100, 0},
		{"ordinary case", 1_000_000, 100, 999_900},
		{"allowance wider than the chain floors at zero", 50, 100, 0},
		{"allowance exactly the height floors at zero", 100, 100, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MinAllowedHeight(tc.perceived, tc.allowance); got != tc.want {
				t.Errorf("MinAllowedHeight(%d, %d) = %d, want %d", tc.perceived, tc.allowance, got, tc.want)
			}
		})
	}
}
