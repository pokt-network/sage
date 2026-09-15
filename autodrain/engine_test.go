package autodrain

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/reputation"
)

const (
	opa   = domain.EndpointAddr("pokt1a-https://r001.opa.example")
	opa2  = domain.EndpointAddr("pokt1c-https://s029.opa.example")
	opb = domain.EndpointAddr("pokt1b-https://node1.opb.example")
	sei       = domain.ServiceID("sei")
	jsonrpc   = domain.RPCTypeJSONRPC
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
	t.Helper()
	h := &harness{
		drains: drain.NewMemoryStore(),
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
		IsLeader:  func() bool { return h.leader },
		Now:       func() time.Time { return h.now },
	})
	return h
}

// feed reproduces the sei shape: the collapse guard sends opa 40 of 50
// picks, opa answers none of its 60 attempts, opb answers 70 of 100.
func (h *harness) feed(opaSuccess, opaAttempts int) {
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
		h.e.OnSignal(sei, jsonrpc, opa, st, false)
	}
	for i := 0; i < 100; i++ {
		st := reputation.SignalSuccess
		if i%10 >= 7 {
			st = reputation.SignalMajorError
		}
		h.e.OnSignal(sei, jsonrpc, opb, st, false)
	}
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
		h.e.OnSignal(sei, domain.RPCTypeREST, opa, reputation.SignalMajorError, false)
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
