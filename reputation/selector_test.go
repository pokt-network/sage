package reputation

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

func fixedScoreFn(scores map[domain.EndpointAddr]float64) ScoreFn {
	return func(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr) (float64, bool) {
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
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	endpoints := domain.EndpointAddrList{"ep1", "ep2", "ep3"}
	result := sel.Select(context.Background(), "eth", endpoints)

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

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1", "ep2"})
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

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0] != "ep1" {
		t.Errorf("expected ep1, got %s", result[0])
	}
}

func TestTieredSelector_FilteredOut(t *testing.T) {
	scores := map[domain.EndpointAddr]float64{
		"ep1": 5, // Below minThreshold (10).
	}
	cfg := DefaultSelectorConfig()
	sel := NewTieredSelector(cfg, fixedScoreFn(scores))

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1"})
	if result != nil {
		t.Errorf("expected nil for filtered-out endpoints, got %v", result)
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

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"healthy", "probe"})

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

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"healthy", "probe"})

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

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep1", "ep2"})
	if len(result) != 1 {
		t.Fatalf("expected 1 result when all in probation, got %d", len(result))
	}
}

func TestTieredSelector_Empty(t *testing.T) {
	cfg := DefaultSelectorConfig()
	sel := NewTieredSelector(cfg, fixedScoreFn(nil))

	result := sel.Select(context.Background(), "eth", nil)
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

	result := sel.Select(context.Background(), "eth", domain.EndpointAddrList{"ep_unknown"})
	if len(result) != 1 || result[0] != "ep_unknown" {
		t.Errorf("expected unknown endpoint in tier 3, got %v", result)
	}
}
