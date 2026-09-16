package reputation

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

const rateSvc = domain.ServiceID("sei")

func opService(t *testing.T, storage Storage) *serviceImpl {
	t.Helper()
	if storage == nil {
		storage = NewMemoryStorage()
	}
	s := NewService(storage, nil, ServiceConfig{})
	s.SetRelativeChronic(func(domain.ServiceID) bool { return true })
	s.SetOperatorChronic(func(domain.ServiceID) bool { return true })
	return s
}

// feedOperator records n attempts against one endpoint, failures of them major
// errors (half a failure each, per FailureWeight).
func feedOperator(s *serviceImpl, endpoint domain.EndpointAddr, n, failures int) {
	for i := 0; i < n; i++ {
		sig := Signal{Type: SignalSuccess, Timestamp: time.Now()}
		if i < failures {
			sig.Type = SignalMajorError
		}
		_ = s.RecordSignal(context.Background(), rateSvc, endpoint, domain.RPCTypeJSONRPC, sig)
	}
}

// The measurement has to survive the session draw: an operator's endpoints are
// replaced every session, so evidence recorded against one set of hosts must
// still count when the next set arrives. This is the failure the per-key rate
// could not represent.
func TestOperatorRateSurvivesKeyChurn(t *testing.T) {
	s := opService(t, nil)

	// Three sessions, a different host each time, same operator, same
	// behaviour: 20% of attempts are major errors, so 10% failure weight.
	for _, host := range []string{"r001", "r002", "r003"} {
		feedOperator(s, domain.EndpointAddr("pokt1a-https://"+host+".opa.example"), 200, 40)
	}

	got, ok := s.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example")
	if !ok {
		t.Fatal("no operator rate after three sessions")
	}
	if math.Abs(got.Rate-0.10) > 0.01 {
		t.Fatalf("operator rate %.4f, want ~0.10", got.Rate)
	}
	if got.Attempts < 500 {
		t.Fatalf("attempts %v, want the evidence of all three sessions", got.Attempts)
	}

	// No single key holds enough attempts to have said this: each host saw 200
	// attempts and 20 failures' worth of weight, and its own EWMA has barely
	// moved off zero at the default half-life.
	sh := s.shard(s.keyOf("pokt1a-https://r001.opa.example", domain.RPCTypeJSONRPC))
	sh.mu.RLock()
	st := sh.cache[rateSvc][s.keyOf("pokt1a-https://r001.opa.example", domain.RPCTypeJSONRPC)]
	sh.mu.RUnlock()
	if st.Rate > 0.01 {
		t.Fatalf("per-key rate %.4f is high enough to have carried this on its own", st.Rate)
	}
}

// Evidence fades with time rather than with attempts, because attempts are not
// a clock when the endpoint set is redrawn every session.
func TestOperatorStatDecaysByTimeAndKeepsItsRatio(t *testing.T) {
	now := time.Now()
	st := OperatorStat{Attempts: 1000, Failures: 100, UpdatedAt: now.Unix()}

	aged := st.decayTo(now.Add(DefaultOperatorHalfLife), DefaultOperatorHalfLife)
	if math.Abs(aged.Attempts-500) > 5 || math.Abs(aged.Failures-50) > 1 {
		t.Fatalf("after one half-life: attempts %.1f failures %.1f, want ~500/~50", aged.Attempts, aged.Failures)
	}
	if math.Abs(aged.Rate()-0.10) > 0.001 {
		t.Fatalf("decay changed the rate: %.4f, want 0.10", aged.Rate())
	}

	// Faded far enough and it stops claiming to know anything.
	cold := st.decayTo(now.Add(20*DefaultOperatorHalfLife), DefaultOperatorHalfLife)
	if cold.Rate() != 0 {
		t.Fatalf("a long-quiet operator still reports %.4f", cold.Rate())
	}
}

// A pod roll must not lose what the fleet learned: the counters are written
// behind and adopted at startup.
func TestOperatorStatsPersistAcrossARestart(t *testing.T) {
	store := NewMemoryStorage()
	first := opService(t, store)
	feedOperator(first, "pokt1a-https://r001.opa.example", 400, 80)
	first.flushOperatorStats(store, time.Now())

	next := opService(t, store)
	if _, ok := next.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example"); ok {
		t.Fatal("a fresh service reported an operator rate before hydrating")
	}
	loaded, err := next.Hydrate(context.Background())
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if loaded.Operators != 1 {
		t.Fatalf("adopted %d operator counters, want 1", loaded.Operators)
	}
	got, ok := next.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example")
	if !ok || math.Abs(got.Rate-0.10) > 0.01 {
		t.Fatalf("after hydrate: rate %.4f ok=%v, want ~0.10", got.Rate, ok)
	}
}

// A drained operator records nothing, so its evidence must hold rather than
// drift while it is benched — the frozen-score problem, avoided by decaying on
// a clock instead of resetting with the key set.
func TestOperatorRateHoldsWhileAnOperatorIsIdle(t *testing.T) {
	s := opService(t, nil)
	feedOperator(s, "pokt1a-https://r001.opa.example", 400, 80)

	before, _ := s.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example")
	after, ok := s.ops.get(opID{rateSvc, "opa.example", string(domain.RPCTypeJSONRPC)}, time.Now().Add(30*time.Minute))
	if !ok {
		t.Fatal("operator evidence vanished while idle")
	}
	if math.Abs(after.Rate()-before.Rate) > 0.001 {
		t.Fatalf("idle 30m changed the rate: %.4f then %.4f", before.Rate, after.Rate())
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
