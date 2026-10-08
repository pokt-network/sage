package reputation

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party that fails one method class and answers the others is charged only
// when selecting for that class, on every non-WebSocket key it has in the
// service, and not at all with the flag off.
func TestClassShare_ChargesOnlyTheFailingClass(t *testing.T) {
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	on := true
	s.SetClassShare(func(domain.ServiceID) bool { return on }, nil)
	ctx := context.Background()
	// The failures land on one key; the score is read on another key of the
	// same party, which has no failures of its own.
	failing := domain.EndpointAddr("pokt1a-https://r001.flaky.example")
	other := domain.EndpointAddr("pokt1a-https://r002.flaky.example")
	solid := domain.EndpointAddr("pokt1b-https://r001.solid.example")
	sig := func(typ SignalType, class string) Signal {
		return Signal{Type: typ, Reason: "x", Class: class, Timestamp: time.Now()}
	}
	for i := 0; i < 400; i++ {
		_ = s.RecordSignal(ctx, "sol", failing, domain.RPCTypeJSONRPC, sig(SignalSuccess, ClassLight))
		_ = s.RecordSignal(ctx, "sol", failing, domain.RPCTypeJSONRPC, sig(SignalMajorError, ClassHeavy))
		_ = s.RecordSignal(ctx, "sol", solid, domain.RPCTypeJSONRPC, sig(SignalSuccess, ClassLight))
		_ = s.RecordSignal(ctx, "sol", solid, domain.RPCTypeJSONRPC, sig(SignalSuccess, ClassHeavy))
	}
	s.refreshBaselines()

	score := func(ep domain.EndpointAddr, rpc domain.RPCType, class string) float64 {
		c := ctx
		if class != "" {
			c = WithMethodClass(ctx, class)
		}
		v, _ := s.scoreForSelector(c, "sol", ep, rpc)
		return v
	}
	if got := score(other, domain.RPCTypeJSONRPC, ""); got != 100 {
		t.Errorf("no class: %.1f, want 100", got)
	}
	if got := score(other, domain.RPCTypeJSONRPC, ClassLight); got != 100 {
		t.Errorf("light, which the party answers: %.1f, want 100", got)
	}
	if got := score(other, domain.RPCTypeJSONRPC, ClassHeavy); got != 60 {
		t.Errorf("heavy, which the party fails: %.1f, want 60", got)
	}
	if got := score(other, domain.RPCTypeWebSocket, ClassHeavy); got != 100 {
		t.Errorf("its WebSocket key: %.1f, want 100", got)
	}
	if got := score(solid, domain.RPCTypeJSONRPC, ClassHeavy); got != 100 {
		t.Errorf("the party answering heavy calls: %.1f, want 100", got)
	}
	// Tier 2 is still tried on a small share of picks; the rest of the heavy
	// picks go to the clean party, where without a class they split.
	picks := func(class string) (n int) {
		c := ctx
		if class != "" {
			c = WithMethodClass(ctx, class)
		}
		for i := 0; i < 400; i++ {
			if s.SelectBest(c, "sol", domain.EndpointAddrList{other, solid}, domain.RPCTypeJSONRPC) == solid {
				n++
			}
		}
		return n
	}
	if n := picks(ClassHeavy); n < 340 {
		t.Errorf("heavy: %d of 400 picks to the clean party, want at least 340", n)
	}
	if n := picks(""); n > 300 {
		t.Errorf("no class: %d of 400 picks to one party, want them split", n)
	}

	shares := map[string]float64{}
	for _, p := range s.PartyClassShares()[ClassHeavy] {
		shares[p.Party] = p.Share
	}
	if math.Abs(shares[failing.Party()]-0.5) > 0.001 || shares[solid.Party()] != 0 {
		t.Errorf("heavy shares = %v, want 0.5 (major weighs half) and 0", shares)
	}

	on = false
	s.refreshBaselines()
	if got := score(other, domain.RPCTypeJSONRPC, ClassHeavy); got != 100 {
		t.Errorf("flag off: %.1f, want 100", got)
	}
}

// The class share counts first client attempts with a class, failures weighed
// as the chronic rate weighs them: a throttle is minor and not a failure, and
// a retry or hedge arm, a probe, a WebSocket attempt or an unclassed one does
// not count.
func TestClassShare_CountsFirstClassedAttempts(t *testing.T) {
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	ep := domain.EndpointAddr("pokt1a-https://r001.flaky.example")
	id := opID{svc: "sol", op: ep.Party()}
	now := time.Unix(time.Now().Unix(), 0)
	for _, c := range []struct {
		name           string
		rpc            domain.RPCType
		sig            Signal
		attempt, fails float64
	}{
		{"timeout", domain.RPCTypeJSONRPC, Signal{Type: SignalMajorError, Class: ClassHeavy}, 1, 0.5},
		{"critical", domain.RPCTypeJSONRPC, Signal{Type: SignalCriticalError, Class: ClassHeavy}, 1, 1},
		{"throttle", domain.RPCTypeJSONRPC, Signal{Type: SignalMinorError, Class: ClassHeavy}, 1, 0},
		{"success", domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess, Class: ClassHeavy}, 1, 0},
		{"retry or hedge arm", domain.RPCTypeJSONRPC, Signal{Type: SignalMajorError, Class: ClassHeavy, Leftover: true}, 0, 0},
		{"probe", domain.RPCTypeJSONRPC, Signal{Type: SignalMajorError, Class: ClassHeavy, Probe: true}, 0, 0},
		{"websocket", domain.RPCTypeWebSocket, Signal{Type: SignalMajorError, Class: ClassHeavy}, 0, 0},
		{"unclassed", domain.RPCTypeJSONRPC, Signal{Type: SignalMajorError}, 0, 0},
	} {
		before, _ := s.classes[ClassHeavy].get(id, now)
		s.recordClass("sol", ep, c.rpc, c.sig, now)
		after, _ := s.classes[ClassHeavy].get(id, now)
		if d := after.Attempts - before.Attempts; math.Abs(d-c.attempt) > 1e-9 {
			t.Errorf("%s: counted %.0f attempts, want %.0f", c.name, d, c.attempt)
		}
		if d := after.Failures - before.Failures; math.Abs(d-c.fails) > 1e-9 {
			t.Errorf("%s: counted %.1f failures, want %.1f", c.name, d, c.fails)
		}
	}
}
