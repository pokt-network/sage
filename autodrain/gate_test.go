package autodrain

import (
	"context"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/reputation"
)

// fakeRates is the per-operator chronic rate, keyed by operator.
type fakeRates map[string]float64

func (f fakeRates) OperatorRate(_ domain.ServiceID, _ domain.RPCType, operator string) (reputation.OperatorRateView, bool) {
	r, ok := f[operator]
	if !ok {
		return reputation.OperatorRateView{}, false
	}
	return reputation.OperatorRateView{Rate: r, Attempts: 50_000}, true
}

// The fallback feeding an operator that answers nothing is not an incident
// while the callers of that service are served: retry is already covering it,
// and a drain buys nothing. Every false proposal of the 2026-09-15/16 shadow
// run had this shape.
func TestEngine_ClientGateStopsADrainNobodyNeeds(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feedTraffic(0, 60)
	h.clients(200, 1) // 0.5%, under the bar

	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("drained a service whose callers are fine: %+v", active)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomeBelowClient {
		t.Fatalf("events = %v, want the client gate", o)
	}
}

// Ten answers, all failed, is not enough to judge a service either way — and
// it says so in its own outcome, rather than reading as "callers are fine".
func TestEngine_TooFewClientAnswersIsItsOwnOutcome(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feedTraffic(0, 60)
	h.clients(10, 10)

	h.e.Evaluate(context.Background())

	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomeNoClientEvidence {
		t.Fatalf("events = %v, want no_client_evidence", o)
	}
	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("drained on 10 answers: %+v", active)
	}
}

// dilutedFeed is the sei shape the collapse trigger cannot see: the operator
// takes a minority of the picks and answers 90% of what it is sent, so only its
// chronic rate across every key it holds gives it away.
func dilutedFeed(h *harness) {
	for i := 0; i < 10; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opa})
	}
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opb})
	}
	for i := 0; i < 100; i++ {
		st := reputation.SignalSuccess
		if i%10 == 0 {
			st = reputation.SignalMajorError
		}
		h.e.OnSignal(sei, jsonrpc, opa, st, false)
	}
	h.clients(200, 40)
}

func TestEngine_OperatorRateTriggerSeesTheDilutedOperator(t *testing.T) {
	h := newHarnessWith(t, fakeVouch{opb: true}, true, fakeRates{"opa.example": 0.09})
	dilutedFeed(h)

	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 || active[0].Operator != "opa.example" {
		t.Fatalf("active = %+v, want the diluted operator drained", active)
	}
	evs, _ := h.log.Recent(context.Background(), "", 10)
	if len(evs) != 1 || evs[0].Trigger != TriggerOperatorRate {
		t.Fatalf("events = %+v, want the operator_rate trigger", evs)
	}
	if evs[0].OperatorRate != 0.09 || evs[0].ClientFailure != 0.2 {
		t.Fatalf("event evidence = %+v, want the rate and client share recorded", evs[0])
	}
}

// sei's pool is two operators and the rate trigger flags both within minutes
// of each other (mainnet, 2026-09-16: rpcgate at 0.096, nodefleet at 0.050).
// Draining both would empty the pool, so at most one auto drain is ever live
// per pool — and the guard has to survive a restart or the leader moving,
// which means reading the shared drain store rather than one instance's memory.
func TestEngine_TwoOperatorPoolNeverDrainsBoth(t *testing.T) {
	rates := fakeRates{"opa.example": 0.09, "opb.example": 0.05}
	vouch := fakeVouch{opa: true, opa2: true, opb: true}
	bothFailing := func(h *harness) {
		for i := 0; i < 100; i++ {
			h.e.OnSignal(sei, jsonrpc, opa, reputation.SignalMajorError, false)
			h.e.OnSignal(sei, jsonrpc, opb, reputation.SignalMajorError, false)
		}
		h.clients(200, 40)
	}

	h := newHarnessWith(t, vouch, true, rates)
	bothFailing(h)
	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 {
		t.Fatalf("active = %+v, want exactly one drain for a two-operator pool", active)
	}

	// A fresh instance on the same store — a pod restart, or the leader moving
	// mid-incident — must not drain the operator the first one left alone.
	next := newHarnessOn(t, h.drains, vouch, true, rates)
	bothFailing(next)
	next.e.Evaluate(context.Background())

	if after := next.drains.Active(context.Background(), sei); len(after) != 1 {
		t.Fatalf("active = %+v, want the pool to keep one operator", after)
	}
	for _, o := range next.outcomes(t) {
		if strings.HasSuffix(o, ":"+OutcomeDrained) {
			t.Fatalf("a second instance drained the rest of the pool: %v", next.outcomes(t))
		}
	}
}

// The same shape with a healthy operator rate is nothing at all: a
// mostly-working operator the fallback is not feeding stays put.
func TestEngine_LowOperatorRateIsNotACandidate(t *testing.T) {
	h := newHarnessWith(t, fakeVouch{opb: true}, true, fakeRates{"opa.example": 0.005})
	dilutedFeed(h)

	h.e.Evaluate(context.Background())

	if o := h.outcomes(t); len(o) != 0 {
		t.Fatalf("events = %v, want none", o)
	}
}
