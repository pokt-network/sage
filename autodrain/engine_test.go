package autodrain

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/reputation"
)

const (
	opa     = domain.EndpointAddr("pokt1a-https://r001.opa.example")
	opa2    = domain.EndpointAddr("pokt1c-https://s029.opa.example")
	opb     = domain.EndpointAddr("pokt1b-https://node1.opb.example")
	sei     = domain.ServiceID("sei")
	method  = "eth_call"
	jsonrpc = domain.RPCTypeJSONRPC
)

type fakeEndpoints map[domain.ServiceID]domain.EndpointAddrList

func (f fakeEndpoints) AvailableEndpoints(_ context.Context, svc domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	return f[svc], nil
}

type fakeVouch map[domain.EndpointAddr]bool

func (f fakeVouch) Vouched(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType) bool {
	return f[ep]
}

type harness struct {
	e      *Engine
	drains *drain.MemoryStore
	flags  *featureflag.MemoryStore
	log    *MemoryLog
	now    time.Time
	leader bool
}

func newHarness(t *testing.T, vouch fakeVouch, act bool) *harness {
	return newHarnessWith(t, vouch, act, nil)
}

func newHarnessWith(t *testing.T, vouch fakeVouch, act bool, rates OperatorRates) *harness {
	return newHarnessOn(t, drain.NewMemoryStore(), vouch, act, rates)
}

// newHarnessOn builds an engine over an existing drain store, so a test can
// stand a second instance on the drains the first one set.
func newHarnessOn(t *testing.T, drains *drain.MemoryStore, vouch fakeVouch, act bool, rates OperatorRates) *harness {
	t.Helper()
	h := &harness{
		drains: drains,
		flags:  featureflag.NewMemoryStore(featureflag.DefaultFlags),
		log:    &MemoryLog{},
		now:    time.Now(),
		leader: true,
	}
	if act {
		_ = h.flags.Set(context.Background(), featureflag.FlagAutoDrain, true)
		_ = h.flags.Set(context.Background(), featureflag.FlagAutoDrainShadow, false)
	}
	h.e = New(Deps{
		Drains:    h.drains,
		Endpoints: fakeEndpoints{sei: {opa, opa2, opb}},
		Vouch:     vouch,
		Flags:     h.flags,
		Events:    h.log,
		Rates:     rates,
		IsLeader:  func() bool { return h.leader },
		Now:       func() time.Time { return h.now },
	})
	return h
}

// clients records what callers of the service saw over the window. The gate
// reads this, so every traffic shape below has to say whether anyone was hurt.
func (h *harness) clients(total, failed int) {
	for i := 0; i < total; i++ {
		status := 200
		if i < failed {
			status = 504
		}
		h.e.OnClientResult(sei, status)
	}
}

// feed reproduces the sei shape: the collapse guard sends opa 40 of 50
// picks, opa answers none of its 60 attempts, opb answers 70 of 100.
// feed is the incident shape: the traffic below, and callers of the service
// failing 20% of the time while it lasts.
func (h *harness) feed(opaSuccess, opaAttempts int) {
	h.feedTraffic(opaSuccess, opaAttempts)
	h.clients(200, 40)
}

func (h *harness) feedTraffic(opaSuccess, opaAttempts int) {
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opa})
	}
	for i := 0; i < 10; i++ {
		h.e.OnCollapse(sei, jsonrpc, domain.EndpointAddrList{opb})
	}
	for i := 0; i < opaAttempts; i++ {
		st := reputation.SignalMajorError
		if i < opaSuccess {
			st = reputation.SignalSuccess
		}
		h.e.OnAttempt(sei, jsonrpc, opa, attr(st), "first", method)
	}
	for i := 0; i < 100; i++ {
		st := reputation.SignalSuccess
		if i%10 >= 7 {
			st = reputation.SignalMajorError
		}
		h.e.OnAttempt(sei, jsonrpc, opb, attr(st), "first", method)
	}
}

// attr is the attribution the metrics attempt hook carries for an attempt
// reputation would grade st.
func attr(st reputation.SignalType) string {
	if st == reputation.SignalSuccess {
		return "none"
	}
	return "supplier"
}

func (h *harness) outcomes(t *testing.T) []string {
	t.Helper()
	evs, _ := h.log.Recent(context.Background(), "", 100)
	var out []string
	for _, ev := range evs {
		out = append(out, ev.Operator+":"+ev.Outcome)
	}
	return out
}

func TestEngine_SeiShapeDrainsTheOperatorTheFallbackFeeds(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 {
		t.Fatalf("active drains = %+v, want one", active)
	}
	got := active[0]
	if got.Operator != "opa.example" || got.RPCType != jsonrpc || !strings.HasPrefix(got.Reason, ReasonPrefix) {
		t.Fatalf("drain = %+v, want opa.example json_rpc with the auto: reason", got)
	}
	if d := got.Until.Sub(h.now); d < drainFor-time.Second || d > drainFor+time.Second {
		t.Fatalf("drain length = %s, want %s", d, drainFor)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:drained" {
		t.Fatalf("events = %v", o)
	}
}

func TestEngine_ShadowByDefaultNeverSetsADrain(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, false)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())
	h.e.Evaluate(context.Background()) // an unchanged decision is not recorded twice

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("shadow set a drain: %+v", active)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:shadow" {
		t.Fatalf("events = %v, want one shadow decision", o)
	}
}

// moonbeam: opa is the only operator anyone vouches for — or nobody is.
// Draining it would empty the pool.
func TestEngine_NoVouchedAlternativeNoDrain(t *testing.T) {
	h := newHarness(t, fakeVouch{}, true)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("drained with no alternative: %+v", active)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomeNoVouched {
		t.Fatalf("events = %v", o)
	}
}

// A slow operator that answers most requests, and one below the attempt
// floor, are not candidates at all.
func TestEngine_NotACandidate(t *testing.T) {
	for name, feed := range map[string][2]int{
		"answers 60%":         {36, 60},
		"below attempt floor": {0, 40},
	} {
		h := newHarness(t, fakeVouch{opb: true}, true)
		h.feed(feed[0], feed[1])
		h.e.Evaluate(context.Background())
		if o := h.outcomes(t); len(o) != 0 {
			t.Errorf("%s: events = %v, want none", name, o)
		}
	}
}

// A person's drain on the key is theirs: the engine neither replaces nor
// extends it.
func TestEngine_LeavesManualDrainsAlone(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	manual := drain.Entry{Key: drain.Key{ServiceID: sei, Operator: "opa.example", RPCType: jsonrpc}, Until: h.now.Add(6 * time.Hour), Reason: "ops: sei relief"}
	_ = h.drains.Set(context.Background(), manual)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 || active[0].Reason != manual.Reason || !active[0].Until.Equal(manual.Until) {
		t.Fatalf("manual drain changed: %+v", active)
	}
	if o := h.outcomes(t); len(o) != 1 || o[0] != "opa.example:"+OutcomeManual {
		t.Fatalf("events = %v", o)
	}
}

// A person released an auto drain before it expired: that is a veto, and it
// sticks for suppressFor.
func TestEngine_ManualReleaseSuppresses(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())
	_ = h.drains.Release(context.Background(), drain.Key{ServiceID: sei, Operator: "opa.example", RPCType: jsonrpc})

	h.now = h.now.Add(time.Minute)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())

	if active := h.drains.Active(context.Background(), sei); len(active) != 0 {
		t.Fatalf("re-drained after a person released it: %+v", active)
	}
	if o := h.outcomes(t); len(o) != 2 || o[0] != "opa.example:"+OutcomeSuppressed {
		t.Fatalf("events = %v, want suppressed after drained", o)
	}
}

// One new auto drain per service per serviceGap, whatever the RPC type.
func TestEngine_RateLimitedPerService(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())

	// The same shape on the REST face a few minutes later.
	h.now = h.now.Add(5 * time.Minute)
	for i := 0; i < 40; i++ {
		h.e.OnCollapse(sei, domain.RPCTypeREST, domain.EndpointAddrList{opa})
	}
	for i := 0; i < 60; i++ {
		h.e.OnAttempt(sei, domain.RPCTypeREST, opa, attr(reputation.SignalMajorError), "first", method)
	}
	h.e.Evaluate(context.Background())

	if o := h.outcomes(t); len(o) < 2 || o[0] != "opa.example:"+OutcomeRateLimited {
		t.Fatalf("events = %v, want the REST decision rate limited", o)
	}
}

func TestEngine_NotLeaderNeitherCountsNorActs(t *testing.T) {
	h := newHarness(t, fakeVouch{opb: true}, true)
	h.leader = false
	h.e.Evaluate(context.Background()) // learns it is not leader
	h.feed(0, 60)
	h.leader = true
	h.e.Evaluate(context.Background())

	if o := h.outcomes(t); len(o) != 0 {
		t.Fatalf("events = %v: a non-leader's traffic was counted", o)
	}
}

// An owner staked with two providers that host many owners does not make them
// one provider: the drained provider's alternative may be the other one even
// though the same owner stakes on both (mainnet, 2026-09-26: 12 owners staked
// across two independent providers). Only a domain dedicated to one owner
// links through that owner (domain.Affiliates).
func TestEngine_SharedOwnerAcrossMultiTenantProvidersStillLeavesAnAlternative(t *testing.T) {
	domain.RecordOwner("pokt1a", "pokt1sharedowner", "opa.example")
	domain.RecordOwner("pokt1b", "pokt1sharedowner", "opb.example")
	for i := 0; i < 6; i++ {
		domain.RecordOwner(fmt.Sprintf("pokt1tenantA%d", i), fmt.Sprintf("pokt1ownerA%d", i), "opa.example")
		domain.RecordOwner(fmt.Sprintf("pokt1tenantB%d", i), fmt.Sprintf("pokt1ownerB%d", i), "opb.example")
	}

	h := newHarness(t, fakeVouch{opb: true}, true)
	h.feed(0, 60)
	h.e.Evaluate(context.Background())

	active := h.drains.Active(context.Background(), sei)
	if len(active) != 1 || active[0].Operator != "opa.example" {
		t.Fatalf("active drains = %+v, want opa.example drained with opb.example as the alternative", active)
	}
}

// firsts feeds n attempts of one kind for ep, the first chain of them
// answered with a chain error and the rest clean.
func (h *harness) firsts(ep domain.EndpointAddr, kind string, n, chain int) {
	for i := 0; i < n; i++ {
		attribution := "none"
		if i < chain {
			attribution = "blockchain"
		}
		h.e.OnAttempt(sei, jsonrpc, ep, attribution, kind, method)
	}
}

// An operator answering every first attempt with a chain error while its
// peers answer 4% is a candidate scoring cannot see: each of those answers is
// a success to reputation, and every caller got a 200. The errors it delivered
// past its peers' share are the harm the gate reads.
func TestEngine_ChainAnswerOutlier(t *testing.T) {
	for _, act := range []bool{false, true} {
		h := newHarness(t, fakeVouch{opb: true}, act)
		h.firsts(opa, "first", 60, 60)
		h.firsts(opb, "first", 100, 4)
		h.clients(200, 0)
		h.e.Evaluate(context.Background())

		evs, _ := h.log.Recent(context.Background(), "", 10)
		want := OutcomeShadow
		if act {
			want = OutcomeDrained
		}
		if len(evs) != 1 || evs[0].Operator != "opa.example" || evs[0].Outcome != want || evs[0].Trigger != TriggerChainAnswers {
			t.Fatalf("act=%v: events = %+v, want one %s chain_answers decision on opa", act, evs, want)
		}
		if ev := evs[0]; ev.ChainShare != 1 || ev.PeerChainShare != 0.04 || ev.AnswerHarm < 0.28 || ev.AnswerHarm > 0.29 {
			t.Fatalf("evidence = %+v, want share 1, peers 0.04, harm (60-2.4)/200", ev)
		}
		if act {
			if active := h.drains.Active(context.Background(), sei); len(active) != 1 || !strings.Contains(active[0].Reason, "chain error") {
				t.Fatalf("drains = %+v, want opa drained naming the chain errors", active)
			}
		}
	}
}

// Not candidates: a pool whose operators all answer chain errors alike, an
// outlier measured only on retries and hedge arms (they carry what others
// failed), and client-attributed answers, which are nobody's. An outlier too
// small to reach the client bar is recorded and left alone.
func TestEngine_ChainAnswerNotAnOutlier(t *testing.T) {
	for name, tc := range map[string]struct {
		feed    func(h *harness)
		clients int
		want    []string
	}{
		"pool alike": {func(h *harness) { h.firsts(opa, "first", 60, 36); h.firsts(opb, "first", 100, 60) }, 200, nil},
		"retries only": {func(h *harness) {
			h.firsts(opa, "retry", 60, 60)
			h.firsts(opa, "hedge", 60, 60)
			h.firsts(opb, "first", 100, 4)
		}, 200, nil},
		"client-attributed": {func(h *harness) {
			for i := 0; i < 60; i++ {
				h.e.OnAttempt(sei, jsonrpc, opa, "client", "first", method)
			}
			h.firsts(opb, "first", 100, 4)
		}, 200, nil},
		"too little harm": {func(h *harness) { h.firsts(opa, "first", 60, 60); h.firsts(opb, "first", 100, 4) }, 5000, []string{"opa.example:" + OutcomeBelowClient}},
	} {
		h := newHarness(t, fakeVouch{opb: true}, true)
		tc.feed(h)
		h.clients(tc.clients, 0)
		h.e.Evaluate(context.Background())
		if o := h.outcomes(t); fmt.Sprint(o) != fmt.Sprint(tc.want) {
			t.Errorf("%s: events = %v, want %v", name, o, tc.want)
		}
	}
}

// answers feeds n first attempts on method m for ep, the first chain of them
// chain errors and the rest clean.
func (h *harness) answers(ep domain.EndpointAddr, m string, n, chain int) {
	for i := 0; i < n; i++ {
		attribution := "none"
		if i < chain {
			attribution = "blockchain"
		}
		h.e.OnAttempt(sei, jsonrpc, ep, attribution, "first", m)
	}
}

// Shares are compared method by method, over methods the peers answer.
// Selection steers a method away from hosts that refused it, and then only one
// operator answers it: its errors to that method are no evidence (mainnet
// osmosis, 2026-10-05: every peer host method-blocked on CometBFT block, one
// operator's ordinary 500s to bad-height queries read as 27-39% against 0%).
// A peer's 408 is not an answer either. An operator erring on methods the
// peers answer cleanly is still flagged.
func TestEngine_ChainAnswersComparedByMethod(t *testing.T) {
	for name, tc := range map[string]struct {
		feed func(h *harness)
		want []string
	}{
		"only it answers the method": {func(h *harness) {
			h.answers(opa, "block", 60, 60)
			h.answers(opa, "status", 60, 0)
			h.answers(opb, "status", 100, 0)
		}, nil},
		"peers fail the method instead of answering": {func(h *harness) {
			h.answers(opa, "block", 60, 60)
			for i := 0; i < 100; i++ {
				h.e.OnAttempt(sei, jsonrpc, opb, "supplier", "first", "block")
			}
		}, nil},
		"peers too thin on the method": {func(h *harness) {
			h.answers(opa, "getSlot", 60, 60)
			h.answers(opb, "getSlot", 10, 0)
		}, nil},
		"errs on every method the peers answer": {func(h *harness) {
			for _, m := range []string{"getSlot", "getAccountInfo", "getBlock"} {
				h.answers(opa, m, 30, 30)
				h.answers(opb, m, 40, 2)
			}
		}, []string{"opa.example:" + OutcomeShadow}},
	} {
		h := newHarness(t, fakeVouch{opb: true}, false)
		tc.feed(h)
		h.clients(200, 0)
		h.e.Evaluate(context.Background())
		if o := h.outcomes(t); fmt.Sprint(o) != fmt.Sprint(tc.want) {
			t.Errorf("%s: events = %v, want %v", name, o, tc.want)
		}
	}
}

// An operator's methods are bounded per minute; the rest share one bucket,
// which is never compared.
func TestOpCount_MethodsFoldPastTheCap(t *testing.T) {
	var c opCount
	for i := 0; i < maxMethods+10; i++ {
		c.method(fmt.Sprintf("m%d", i)).answered++
	}
	if len(c.methods) != maxMethods+1 || c.methods[overflowMethod].answered != 10 {
		t.Fatalf("methods = %d, overflow = %+v; want %d and 10 folded", len(c.methods), c.methods[overflowMethod], maxMethods+1)
	}
}
