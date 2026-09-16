package reputation

import (
	"math"
	"testing"

	"github.com/pokt-network/sage/domain"
)

const rateSvc = domain.ServiceID("sei")

// seedKey writes the state a key would hold after `attempts` attempts at a
// steady failure rate, starting from a cold EWMA — the shape the warm-up
// correction exists to undo.
func seedKey(s *serviceImpl, key string, trueRate float64, attempts uint64) {
	stored := trueRate * (1 - math.Exp(-s.lambda*float64(attempts)))
	sh := s.shard(key)
	sh.mu.Lock()
	if sh.cache[rateSvc] == nil {
		sh.cache[rateSvc] = map[string]State{}
	}
	sh.cache[rateSvc][key] = State{Score: 100, Rate: stored, Attempts: attempts, TrafficAttempts: attempts}
	sh.mu.Unlock()
}

func rateService(t *testing.T, operatorTerm bool) *serviceImpl {
	t.Helper()
	s := NewService(nil, nil, ServiceConfig{})
	s.SetRelativeChronic(func(domain.ServiceID) bool { return true })
	s.SetOperatorChronic(func(domain.ServiceID) bool { return operatorTerm })
	return s
}

func storedRate(s *serviceImpl, key string) float64 {
	sh := s.shard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.cache[rateSvc][key].Rate
}

// A young key and an old one behaving identically must be charged identically:
// the raw EWMA says otherwise only because it has not warmed up.
func TestOperatorRateIsIndependentOfKeyAge(t *testing.T) {
	s := rateService(t, true)
	seedKey(s, "https://young.opa.example|json_rpc", 0.08, 300)
	seedKey(s, "https://old.opa.example|json_rpc", 0.08, 100_000)
	s.refreshBaselines()

	r, ok := s.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example")
	if !ok {
		t.Fatal("no operator rate for opa.example")
	}
	if math.Abs(r.Rate-0.08) > 0.01 {
		t.Fatalf("operator rate %.4f, want ~0.08", r.Rate)
	}
	// The raw per-key rates the correction starts from are 30x apart.
	young, old := storedRate(s, "https://young.opa.example|json_rpc"), storedRate(s, "https://old.opa.example|json_rpc")
	if young > old/10 {
		t.Fatalf("expected the young key's raw rate to be far below the old one's: %.5f vs %.5f", young, old)
	}
}

// The mainnet sei shape: one operator spreads a service over ~90 keys that each
// see a few hundred attempts, another concentrates the same traffic on 7. The
// first is failing twice as often; per-key rates say the opposite.
func TestOperatorRateSeesThroughKeyDilution(t *testing.T) {
	const (
		spreadKey = "https://s001.opa.example|json_rpc"
		heavyKey  = "https://node1.opb.example|json_rpc"
	)
	seed := func(s *serviceImpl) {
		for i := 0; i < 90; i++ {
			seedKey(s, "https://s"+string(rune('a'+i%26))+string(rune('a'+i/26))+".opa.example|json_rpc", 0.08, 300)
		}
		seedKey(s, spreadKey, 0.08, 300)
		for i := 0; i < 7; i++ {
			seedKey(s, "https://node"+string(rune('1'+i))+".opb.example|json_rpc", 0.04, 60_000)
		}
		s.refreshBaselines()
	}

	on := rateService(t, true)
	seed(on)
	spread, ok := on.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example")
	if !ok {
		t.Fatal("no operator rate for the spread operator")
	}
	heavy, ok := on.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opb.example")
	if !ok {
		t.Fatal("no operator rate for the concentrated operator")
	}
	if spread.Rate <= heavy.Rate {
		t.Fatalf("spread operator rate %.4f should exceed concentrated %.4f", spread.Rate, heavy.Rate)
	}

	// With the operator term on, the worse operator is charged more.
	spreadPenalty := on.penaltyFor(rateSvc, spreadKey, storedRate(on, spreadKey))
	heavyPenalty := on.penaltyFor(rateSvc, heavyKey, storedRate(on, heavyKey))
	if spreadPenalty >= heavyPenalty {
		t.Fatalf("spread penalty %.2f should be harsher than concentrated %.2f", spreadPenalty, heavyPenalty)
	}

	// Off, the per-key rate gets it backwards: the spread operator's keys are
	// too young to cross the onset at all, so they pay nothing.
	off := rateService(t, false)
	seed(off)
	if p := off.penaltyFor(rateSvc, spreadKey, storedRate(off, spreadKey)); p != 0 {
		t.Fatalf("per-key basis charged the spread operator %.2f, expected 0 — the regression this replaces", p)
	}
}

func TestOperatorOfKey(t *testing.T) {
	cases := map[string]string{
		"https://s001.rpc.example.com/path|json_rpc": "example.com",
		"https://node1.opb.example|rest":             "opb.example",
		"pokt1abc|json_rpc":                          "pokt1abc",
		"nokey":                                      "",
	}
	for key, want := range cases {
		if got := operatorOfKey(key); got != want {
			t.Errorf("operatorOfKey(%q) = %q, want %q", key, got, want)
		}
	}
}
