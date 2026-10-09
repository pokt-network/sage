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

// A node's own HTTP 500 — CometBFT's answer to a GET for an unknown tx — is
// the chain answering. Callers who got it were served, and counting it as a
// failure opened the gate on services whose answers were all the chain's.
func TestEngine_ChainAnswersDoNotOpenTheClientGate(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feedTraffic(0, 60)
	for i := 0; i < 200; i++ {
		switch {
		case i < 60:
			h.e.OnClientResult(sei, 500, domain.OriginChain) // 30%
		case i < 61:
			h.e.OnClientResult(sei, 504, domain.OriginSupplier) // 0.5%
		default:
			h.e.OnClientResult(sei, 200, domain.OriginChain)
		}
	}

	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("drained a service whose only failures were the chain's answers: %+v", active)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomeBelowClient {
		t.Fatalf("events = %v, want the client gate", o)
	}
}

// Retry and hedge keep a failing operator off the client error rate, not off
// the client. An operator answering half of hundreds of attempts is drained
// even while clients see under 5% failures (mainnet base, 2026-09-26: 51%
// success, 4.8% client failure, ~6 client 504s a second).
func TestEngine_SevereOperatorPassesTheClientGate(t *testing.T) {
	h := newHarnessWith(t, fakeVouch{opb: true}, true, fakeRates{"opa.example": 0.25})
	for i := 0; i < 10; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opa})
	}
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opb})
	}
	for i := 0; i < 300; i++ {
		h.e.OnAttempt(sei, jsonrpc, opa, attr(okIf(i%2 == 1)), "first", method)
	}
	h.clients(200, 8) // 4%: under the bar

	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 || active[0].Operator != "opa.example" {
		t.Fatalf("active = %+v, want the severe operator drained past the client gate", active)
	}
	evs, _ := h.log.Recent(context.Background(), "", 10)
	if len(evs) != 1 || !evs[0].Severe || evs[0].Outcome != OutcomeDrained {
		t.Fatalf("events = %+v, want one drained event marked severe", evs)
	}
}

// Severity is read on first attempts. An operator answering 70% of its first
// attempts and 10% of its retries reads 50% across both, and is not severe:
// the retries carried what others had failed (mainnet bsc, 2026-10-04: 68%
// first, 13% retries, proposed ten times while no caller failed). One that
// answers nothing on any attempts is severe however it is reached.
func TestEngine_SeverityReadsFirstAttempts(t *testing.T) {
	for name, tc := range map[string]struct {
		firstOK, firsts, retryOK, retries int
		kind                              string
		want                              string
	}{
		// 210 of 300 first attempts, 90 of 300 retries: 50% across both.
		"retries drag it down": {210, 300, 90, 300, "retry", OutcomeBelowClient},
		// None of 600 hedge arms, and no first attempts at all.
		"answers nothing": {0, 0, 0, 600, "hedge", OutcomeDrained},
	} {
		h := newHarnessWith(t, fakeVouch{opb: true}, true, fakeRates{"opa.example": 0.25})
		for i := 0; i < tc.firsts; i++ {
			h.e.OnAttempt(sei, jsonrpc, opa, attr(okIf(i < tc.firstOK)), "first", method)
		}
		for i := 0; i < tc.retries; i++ {
			h.e.OnAttempt(sei, jsonrpc, opa, attr(okIf(i < tc.retryOK)), tc.kind, method)
		}
		h.clients(200, 0)
		h.e.Evaluate(context.Background())
		if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+tc.want {
			t.Errorf("%s: events = %v, want %s", name, o, tc.want)
		}
	}
}

// okIf is a success signal when ok, a major error otherwise.
func okIf(ok bool) reputation.SignalType {
	if ok {
		return reputation.SignalSuccess
	}
	return reputation.SignalMajorError
}

// Reputation collapses a batch's items to one signal per endpoint, the worst,
// so an operator failing one item per batch reads as answering nothing. HTTP
// attempts are counted per item from the attempt hook, and the signal hook is
// read for WebSocket pools only (mainnet poly, 2026-10-04: 0% over 563 signals
// while about half of the operator's attempts were answered).
func TestEngine_HTTPCountsItemsNotSignals(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opa})
	}
	for i := 0; i < 60; i++ {
		h.e.OnSignal(sei, jsonrpc, opa, reputation.SignalMajorError, false) // one worst signal per batch
		h.e.OnAttempt(sei, jsonrpc, opa, attr(okIf(i%2 == 0)), "first", method)
	}
	h.clients(200, 40)
	h.e.Evaluate(context.Background())
	if o := h.outcomes(t); len(o) != 0 {
		t.Fatalf("events = %v: an operator answering half its items was read as answering none", o)
	}

	// The same shape on WebSocket, where the signal is the only feed.
	ws := domain.RPCTypeWebSocket
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, ws, domain.EndpointAddrList{opa})
	}
	for i := 0; i < 60; i++ {
		h.e.OnSignal(sei, ws, opa, reputation.SignalMajorError, false)
	}
	h.e.Evaluate(context.Background())
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomeDrained {
		t.Fatalf("events = %v, want the WebSocket operator drained on its signals", o)
	}
}

// The gate still holds for everything short of severe: 90% success on 100
// attempts, with clients fine, is a routing inefficiency.
func TestEngine_MildOperatorStillMeetsTheClientGate(t *testing.T) {
	h := newHarnessWith(t, fakeVouch{opb: true}, true, fakeRates{"opa.example": 0.09})
	for i := 0; i < 100; i++ {
		st := reputation.SignalSuccess
		if i%10 == 0 {
			st = reputation.SignalMajorError
		}
		h.e.OnAttempt(sei, jsonrpc, opa, attr(st), "first", method)
	}
	h.clients(200, 1)

	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("drained a mild operator while clients were fine: %+v", active)
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
		h.e.OnAttempt(sei, jsonrpc, opa, attr(st), "first", method)
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
			h.e.OnAttempt(sei, jsonrpc, opa, attr(reputation.SignalMajorError), "first", method)
			h.e.OnAttempt(sei, jsonrpc, opb, attr(reputation.SignalMajorError), "first", method)
		}
		h.clients(200, 40)
	}

	// Both fail alike, which the pool-wide guard holds back; it is off here
	// so the cap is what is tested.
	noGuard := func(domain.ServiceID) float64 { return 0 }
	h := newHarnessWith(t, vouch, true, rates)
	h.e.SetPeerRatio(noGuard)
	bothFailing(h)
	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 {
		t.Fatalf("active = %+v, want exactly one drain for a two-operator pool", active)
	}

	// A fresh instance on the same store — a pod restart, or the leader moving
	// mid-incident — must not drain the operator the first one left alone.
	next := newHarnessOn(t, h.drains, vouch, true, rates)
	next.e.SetPeerRatio(noGuard)
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
