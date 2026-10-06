package reputation

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party whose backend throttles a steady share of its first attempts is
// charged on its HTTP keys, linear from the floor to -40 at the full excess
// over the service's cleanest party; its WebSocket keys are not, and nothing
// is charged with the flag off.
func TestThrottleShare_ChargesTheExcessOnHTTPKeys(t *testing.T) {
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	on := true
	s.SetThrottleShare(func(domain.ServiceID) bool { return on }, nil)
	ctx := context.Background()
	busy := domain.EndpointAddr("pokt1a-https://r001.busy.example")
	ample := domain.EndpointAddr("pokt1b-https://r001.ample.example")
	sig := func(reason string) Signal {
		typ := SignalSuccess
		if reason != "relay_ok" {
			typ = SignalMinorError
		}
		return Signal{Type: typ, Reason: reason, Timestamp: time.Now()}
	}
	for i := 0; i < 1000; i++ {
		reason := "relay_ok"
		if i%10 == 0 {
			reason = reasonHTTP429 // 10%
		}
		_ = s.RecordSignal(ctx, "eth", busy, domain.RPCTypeJSONRPC, sig(reason))
		_ = s.RecordSignal(ctx, "eth", ample, domain.RPCTypeJSONRPC, sig("relay_ok"))
	}
	_ = s.RecordSignal(ctx, "eth", busy, domain.RPCTypeWebSocket, sig("relay_ok"))
	s.refreshBaselines()

	score := func(ep domain.EndpointAddr, rpc domain.RPCType) float64 {
		v, _ := s.scoreForSelector(ctx, "eth", ep, rpc)
		return v
	}
	// 10% excess: -40 * (0.10-0.02)/(0.15-0.02) ≈ -24.6.
	if got := score(busy, domain.RPCTypeJSONRPC); math.Abs(got-75.4) > 0.5 {
		t.Errorf("throttling party's HTTP key: %.1f, want about 75.4", got)
	}
	if got := score(busy, domain.RPCTypeWebSocket); got != 100 {
		t.Errorf("its WebSocket key: %.1f, want 100", got)
	}
	if got := score(ample, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("the party with capacity: %.1f, want 100", got)
	}
	shares := map[string]float64{}
	for _, p := range s.PartyThrottleShares() {
		shares[p.Party] = p.Share
	}
	if math.Abs(shares[busy.Party()]-0.10) > 0.001 || shares[ample.Party()] != 0 {
		t.Errorf("shares = %v, want 10%% and 0", shares)
	}

	on = false
	s.refreshBaselines()
	if got := score(busy, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("flag off: %.1f, want 100", got)
	}
}

// Throttling counts on a fair sample: an HTTP 429 from the backend or the
// relay miner and a node's rate-limit answer do, while a retry or hedge arm, a
// probe and a WebSocket attempt do not.
func TestThrottleShare_CountsBackendThrottlingOnFirstAttempts(t *testing.T) {
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	ep := domain.EndpointAddr("pokt1a-https://r001.busy.example")
	id := opID{svc: "eth", op: ep.Party()}
	// A whole second: the tracker stamps whole seconds, so a fractional now
	// would decay the counts a little between reads.
	now := time.Unix(time.Now().Unix(), 0)
	for _, c := range []struct {
		name            string
		rpc             domain.RPCType
		sig             Signal
		attempt, thrott float64
	}{
		{"backend 429", domain.RPCTypeJSONRPC, Signal{Type: SignalMinorError, Reason: reasonHTTP429}, 1, 1},
		{"node rate limit", domain.RPCTypeJSONRPC, Signal{Type: SignalMinorError, Reason: reasonRateLimited}, 1, 1},
		{"relay miner 429", domain.RPCTypeJSONRPC, Signal{Type: SignalMinorError, Reason: reasonUpstream429}, 1, 1},
		{"success", domain.RPCTypeREST, Signal{Type: SignalSuccess, Reason: "relay_ok"}, 1, 0},
		{"retry or hedge arm", domain.RPCTypeJSONRPC, Signal{Type: SignalMinorError, Reason: reasonHTTP429, Leftover: true}, 0, 0},
		{"probe", domain.RPCTypeJSONRPC, Signal{Type: SignalMinorError, Reason: reasonHTTP429, Probe: true}, 0, 0},
		{"websocket", domain.RPCTypeWebSocket, Signal{Type: SignalMinorError, Reason: reasonHTTP429}, 0, 0},
		{"no signal", domain.RPCTypeJSONRPC, Signal{}, 0, 0},
	} {
		before, _ := s.throttles.get(id, now)
		s.recordThrottle("eth", ep, c.rpc, c.sig, now)
		after, _ := s.throttles.get(id, now)
		if d := after.Attempts - before.Attempts; math.Abs(d-c.attempt) > 1e-9 {
			t.Errorf("%s: counted %.0f attempts, want %.0f", c.name, d, c.attempt)
		}
		if d := after.Failures - before.Failures; math.Abs(d-c.thrott) > 1e-9 {
			t.Errorf("%s: counted %.0f throttled, want %.0f", c.name, d, c.thrott)
		}
	}
}
