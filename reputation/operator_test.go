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
	if math.Abs(aged.rate()-0.10) > 0.001 {
		t.Fatalf("decay changed the rate: %.4f, want 0.10", aged.rate())
	}

	// Faded far enough and it stops claiming to know anything.
	cold := st.decayTo(now.Add(20*DefaultOperatorHalfLife), DefaultOperatorHalfLife)
	if cold.rate() != 0 {
		t.Fatalf("a long-quiet operator still reports %.4f", cold.rate())
	}
}

// An operator that recovered is forgiven once it has answered a clean run:
// its rate halves every operatorHealHalfLife after that, until it fails again.
func TestOperatorRateHealsAfterACleanRun(t *testing.T) {
	tr := newOpTracker(0)
	id := opID{"eth", "op.example", "json_rpc"}
	now := time.Now()
	for i := 0; i < 1000; i++ {
		tr.record(id, 0, now)
	}
	for i := 0; i < 100; i++ { // an outage
		tr.record(id, 1, now)
	}
	st, _ := tr.get(id, now)
	before := st.RateAt(now)
	if before < 0.08 {
		t.Fatalf("rate after the outage = %.4f, want ~0.09", before)
	}
	for i := 0; i < operatorHealAfter-1; i++ { // not yet a clean run
		tr.record(id, 0, now)
	}
	st, _ = tr.get(id, now.Add(operatorHealHalfLife))
	if r := st.RateAt(now.Add(operatorHealHalfLife)); r < before*0.9 {
		t.Fatalf("forgiven before a clean run: %.4f", r)
	}
	tr.record(id, 0, now) // the clean run is complete
	later := now.Add(operatorHealHalfLife)
	st, _ = tr.get(id, later)
	if r := st.RateAt(later); math.Abs(r-before/2) > before*0.1 {
		t.Fatalf("an hour after healing: %.4f, want about half of %.4f", r, before)
	}

	// A failure partway through healing stops the forgiveness but keeps what
	// was earned: the rate neither keeps halving nor snaps back to the
	// outage's.
	tr.record(id, 1, later)
	st, _ = tr.get(id, later.Add(operatorHealHalfLife))
	if r := st.RateAt(later.Add(operatorHealHalfLife)); math.Abs(r-before/2) > before*0.1 {
		t.Fatalf("after a failure mid-healing: %.4f, want to hold about half of %.4f", r, before)
	}
}

// An operator still failing one attempt in five never holds a clean run long
// enough to be forgiven.
func TestOperatorRateNotForgivenWhileFailing(t *testing.T) {
	tr := newOpTracker(0)
	id := opID{"eth", "bad.example", "json_rpc"}
	now := time.Now()
	for i := 0; i < 2000; i++ {
		failure := 0.0
		if i%5 == 0 {
			failure = 1
		}
		tr.record(id, failure, now.Add(time.Duration(i)*time.Second))
	}
	end := now.Add(2000 * time.Second)
	st, _ := tr.get(id, end)
	if st.HealedAt != 0 || math.Abs(st.RateAt(end)-0.2) > 0.02 {
		t.Fatalf("failing operator: healed_at %d rate %.4f, want never healed at ~0.20", st.HealedAt, st.RateAt(end))
	}
}

// An operator reset forgets the evidence in one service for every RPC type,
// and leaves other services and operators alone.
func TestOpTrackerResetForgetsOneOperatorInOneService(t *testing.T) {
	tr := newOpTracker(0)
	now := time.Now()
	for _, id := range []opID{
		{"eth", "op.example", "json_rpc"}, {"eth", "op.example", "websocket"},
		{"base", "op.example", "json_rpc"}, {"eth", "other.example", "json_rpc"},
	} {
		for i := 0; i < 300; i++ {
			tr.record(id, 1, now)
		}
	}
	if n := tr.reset("eth", "op.example", now); n != 2 {
		t.Fatalf("reset %d counters, want 2", n)
	}
	if st, _ := tr.get(opID{"eth", "op.example", "json_rpc"}, now); st.rate() != 0 || st.Attempts != 0 {
		t.Fatalf("reset counter = %+v, want empty", st)
	}
	if st, _ := tr.get(opID{"base", "op.example", "json_rpc"}, now); st.rate() == 0 {
		t.Fatal("another service's counter was reset")
	}
	if st, _ := tr.get(opID{"eth", "other.example", "json_rpc"}, now); st.rate() == 0 {
		t.Fatal("another operator's counter was reset")
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
	if math.Abs(after.rate()-before.Rate) > 0.001 {
		t.Fatalf("idle 30m changed the rate: %.4f then %.4f", before.Rate, after.rate())
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

// Retries and hedges score the key but stay out of the operator counters: a
// demoted operator's leftovers are the relays another host just failed, and
// counting them held its rate up by the demotion itself.
func TestOperatorRateIgnoresLeftoverAttempts(t *testing.T) {
	s := opService(t, nil)
	ep := domain.EndpointAddr("pokt1a-https://r001.opa.example")
	feedOperator(s, ep, 200, 0)
	for i := 0; i < 200; i++ {
		_ = s.RecordSignal(context.Background(), rateSvc, ep, domain.RPCTypeJSONRPC,
			Signal{Type: SignalCriticalError, Timestamp: time.Now(), Leftover: true})
	}
	got, ok := s.OperatorRate(rateSvc, domain.RPCTypeJSONRPC, "opa.example")
	if ok && got.Rate > 0 {
		t.Fatalf("operator rate %.4f after leftover failures only, want 0", got.Rate)
	}
	if score, _ := s.GetScore(context.Background(), rateSvc, ep, domain.RPCTypeJSONRPC); score >= 100 {
		t.Fatalf("key score %.1f: leftover failures must still score the key", score)
	}
}

// WebSocket stays out of the operator counters: connections and probes are too
// few for the probe failures every operator shares to read as anything but a
// high rate.
func TestOperatorRateIgnoresWebSocket(t *testing.T) {
	s := opService(t, nil)
	ep := domain.EndpointAddr("pokt1a-https://r001.opa.example")
	for i := 0; i < 400; i++ {
		_ = s.RecordSignal(context.Background(), rateSvc, ep, domain.RPCTypeWebSocket,
			Signal{Type: SignalMajorError, Timestamp: time.Now()})
	}
	if got, ok := s.OperatorRate(rateSvc, domain.RPCTypeWebSocket, "opa.example"); ok {
		t.Fatalf("websocket operator rate %.4f recorded, want none", got.Rate)
	}
}

// Stored WebSocket evidence from before the guard is not charged: the key falls
// back to its own rate.
func TestOperatorChronicSkipsStoredWebSocketStats(t *testing.T) {
	s := opService(t, nil)
	ep := domain.EndpointAddr("pokt1a-https://r001.opa.example")
	_ = s.RecordSignal(context.Background(), rateSvc, ep, domain.RPCTypeWebSocket, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	s.ops.merge(map[string]OperatorStat{
		OperatorField(rateSvc, "opa.example", domain.RPCTypeWebSocket): {Attempts: 1000, Failures: 100, UpdatedAt: time.Now().Unix()},
	}, time.Now())
	s.refreshBaselines()
	if _, ok := s.chronic.Load().byKey[keyID{rateSvc, s.keyOf(ep, domain.RPCTypeWebSocket)}]; ok {
		t.Fatal("websocket key charged a stored operator rate")
	}
}
