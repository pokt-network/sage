package reputation

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestPickWeightedByInverseLoad_LoadBias(t *testing.T) {
	eps := domain.EndpointAddrList{"A", "B"}
	load := map[domain.EndpointAddr]int{"A": 10, "B": 0}

	const trials = 2000
	countsB := 0
	for i := 0; i < trials; i++ {
		if pickWeightedByInverseLoad(eps, load) == "B" {
			countsB++
		}
	}
	// Weights: A=1/11≈0.09, B=1/1=1.0 → B probability ≈ 1.0/1.09 ≈ 0.917.
	// Assert B selected at least 85% of the time.
	if countsB < int(0.85*float64(trials)) {
		t.Errorf("B selected %d/%d (want ≥85%% with load={A:10,B:0})", countsB, trials)
	}
}

func TestPickWeightedByInverseLoad_EmptyLoad_Uniform(t *testing.T) {
	eps := domain.EndpointAddrList{"A", "B", "C", "D"}
	const trials = 4000
	counts := map[domain.EndpointAddr]int{}
	for i := 0; i < trials; i++ {
		counts[pickWeightedByInverseLoad(eps, nil)]++
	}
	// Expected ~1000 each; allow ≥700.
	for _, ep := range eps {
		if counts[ep] < 700 {
			t.Errorf("endpoint %s got %d picks; expected ~1000 with uniform distribution", ep, counts[ep])
		}
	}
}

func TestPickWeightedByInverseLoad_Single(t *testing.T) {
	got := pickWeightedByInverseLoad(domain.EndpointAddrList{"only"}, nil)
	if got != "only" {
		t.Errorf("expected single candidate returned, got %q", got)
	}
}

func TestPickWeightedByInverseLoad_Empty(t *testing.T) {
	if got := pickWeightedByInverseLoad(nil, nil); got != "" {
		t.Errorf("expected empty result for nil candidates, got %q", got)
	}
}

func TestSelectSpread_CascadesToLowerTier(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")

	// Two endpoints; push both below T1 (score < 80) so only T2 qualifies.
	// One critical error (-25) → score 75 → T2.
	eps := domain.EndpointAddrList{"ep1", "ep2"}
	for _, ep := range eps {
		_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("x", 0))
	}

	pick := svc.SelectSpread(ctx, svcID, eps, domain.RPCTypeJSONRPC, nil)
	if pick == "" {
		t.Fatal("expected non-empty pick from T2 cascade")
	}
}

func TestSelectSpread_EmptyEndpoints(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()
	if got := svc.SelectSpread(context.Background(), "eth", nil, domain.RPCTypeJSONRPC, nil); got != "" {
		t.Errorf("expected empty for nil endpoints, got %q", got)
	}
}
