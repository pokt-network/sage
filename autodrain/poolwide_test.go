package autodrain

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/reputation"
)

// failing records n first attempts by ep, ok of them answered.
func (h *harness) failing(ep domain.EndpointAddr, ok, n int) {
	for i := 0; i < n; i++ {
		st := reputation.SignalMajorError
		if i < ok {
			st = reputation.SignalSuccess
		}
		h.e.OnAttempt(sei, jsonrpc, ep, attr(st), "first", method)
	}
}

// A slowdown every operator of the pool shares drains nobody: each meets the
// operator-rate trigger, callers are failing, and each fails no worse than
// its peers, so a drain would only move its load onto them.
func TestEngine_PoolWideSlowdownDrainsNobody(t *testing.T) {
	h := newHarnessWith(t, fakeVouch{opa: true, opb: true}, true, fakeRates{"opa.example": 0.05, "opb.example": 0.045})
	h.failing(opa, 60, 100)
	h.failing(opb, 65, 100)
	h.clients(200, 12)

	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("active = %+v, want no drain on a pool-wide slowdown", active)
	}
	evs, _ := h.log.Recent(context.Background(), "", 10)
	if len(evs) != 2 {
		t.Fatalf("events = %+v, want both operators recorded", evs)
	}
	for _, ev := range evs {
		if ev.Outcome != OutcomePoolWide || !ev.PoolWide || ev.Peers != 1 || ev.PeerOperatorRate == 0 {
			t.Errorf("event = %+v, want pool_wide with the peer's rate", ev)
		}
	}
}

// One operator failing well past healthy peers still drains.
func TestEngine_OutlierAgainstHealthyPeersDrains(t *testing.T) {
	h := newHarnessWith(t, fakeVouch{opb: true}, true, fakeRates{"opa.example": 0.09, "opb.example": 0.01})
	h.failing(opa, 60, 100)
	h.failing(opb, 99, 100)
	h.clients(200, 12)

	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 || active[0].Operator != "opa.example" {
		t.Fatalf("active = %+v, want the outlier drained", active)
	}
}

// The collapse trigger is held to the same comparison: an operator answering
// nothing beside peers answering nothing either is the pool, not the
// operator.
func TestEngine_CollapseOnADeadPoolIsPoolWide(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opa})
	}
	h.failing(opa, 0, 100)
	h.failing(opb, 1, 100)
	h.clients(200, 40)

	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("active = %+v, want no drain while every operator is down", active)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomePoolWide {
		t.Fatalf("outcomes = %v, want opa pool_wide", o)
	}
}
