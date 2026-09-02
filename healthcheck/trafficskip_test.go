package healthcheck

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
)

// fakeCounter reports whatever the test puts in it, keyed the way the real
// per-url granularity keys: backend URL plus RPC type.
type fakeCounter struct {
	signals map[string]uint64
	known   map[string]bool
}

func (f *fakeCounter) TrafficSignals(_ domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (uint64, bool) {
	key := string(ep) + "|" + string(rpcType)
	if f.known != nil {
		if k, ok := f.known[key]; ok && !k {
			return 0, false
		}
	}
	v, ok := f.signals[key]
	return v, ok
}

// cycle runs one skip decision inside a begin/end pair, the way runOnce does.
func cycle(t *testing.T, s *trafficSkipper, ep domain.EndpointAddr) bool {
	t.Helper()
	s.beginCycle()
	got := s.skip("eth", ep, "https://node.example.com", domain.RPCTypeJSONRPC)
	s.endCycle()
	return got
}

// The point: a backend carrying enough traffic to replace the probe's own
// observation stops being probed, and only then.
func TestTrafficSkipper_SkipsOnlyAboveThreshold(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	c := &fakeCounter{signals: map[string]uint64{string(ep) + "|json_rpc": 100}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	// First sighting has no baseline to diff against, so it cannot skip
	// however much traffic the key has seen in its life.
	if cycle(t, s, ep) {
		t.Fatal("skipped on the first sighting; there was no window to measure")
	}

	// A window with traffic below the threshold still gets probed: one relay
	// does not stand in for one probe.
	c.signals[string(ep)+"|json_rpc"] = 115
	if cycle(t, s, ep) {
		t.Error("skipped on 15 signals with a threshold of 20")
	}

	// Enough traffic, and the probe is redundant.
	c.signals[string(ep)+"|json_rpc"] = 140
	if !cycle(t, s, ep) {
		t.Error("did not skip on 25 signals with a threshold of 20")
	}

	// Traffic stops: the next window has nothing new, so probing resumes.
	if cycle(t, s, ep) {
		t.Error("kept skipping after traffic stopped; a quiet backend must be probed")
	}
}

// A key the reputation service does not know yet must not be recorded as a
// baseline of zero.
//
// Without the guard, an unknown key stamps 0 into the baseline, and the first
// cycle that does know it diffs a whole life's cumulative traffic against that
// zero — so a key that has been quiet for an hour but busy yesterday skips its
// probe on the strength of yesterday. The count is cumulative; only the
// difference between two real readings is a window.
func TestTrafficSkipper_UnknownKeyRecordsNoBaseline(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	key := string(ep) + "|json_rpc"
	c := &fakeCounter{
		signals: map[string]uint64{key: 5000},
		known:   map[string]bool{key: false},
	}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	if cycle(t, s, ep) {
		t.Fatal("skipped a key the counter does not know")
	}
	if _, recorded := s.prev[trafficKey{service: "eth", backend: "https://node.example.com", rpcType: domain.RPCTypeJSONRPC}]; recorded {
		t.Fatal("recorded a baseline for an unknown key; the next cycle would diff a lifetime against zero")
	}

	// The key becomes known, carrying the cumulative total it always had.
	c.known[key] = true
	if cycle(t, s, ep) {
		t.Error("skipped on the first real reading: 5,000 lifetime signals are not 5,000 signals this window")
	}
	// And only now is there a baseline, so the cycle after can measure a real
	// window against it.
	c.signals[key] = 5100
	if !cycle(t, s, ep) {
		t.Error("did not skip on 100 signals measured against a real baseline")
	}
}

// A count that went backwards is an eviction, not negative traffic.
func TestTrafficSkipper_ResetDoesNotSkip(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	c := &fakeCounter{signals: map[string]uint64{string(ep) + "|json_rpc": 500}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	cycle(t, s, ep) // establish the baseline at 500
	c.signals[string(ep)+"|json_rpc"] = 3
	if cycle(t, s, ep) {
		t.Error("skipped after the key was evicted and re-created; that is a reset, not traffic")
	}
}

// One transport's traffic must not excuse another transport's probe: the RPC
// type is part of the reputation key for exactly this reason.
func TestTrafficSkipper_RPCTypeIsPartOfTheKey(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	c := &fakeCounter{signals: map[string]uint64{
		string(ep) + "|json_rpc":  0,
		string(ep) + "|websocket": 0,
	}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	probeBoth := func() (jsonRPC, ws bool) {
		s.beginCycle()
		jsonRPC = s.skip("eth", ep, "https://node.example.com", domain.RPCTypeJSONRPC)
		ws = s.skip("eth", ep, "https://node.example.com", domain.RPCTypeWebSocket)
		s.endCycle()
		return
	}
	probeBoth() // baselines

	// Busy JSON-RPC, silent WebSocket.
	c.signals[string(ep)+"|json_rpc"] = 100
	jsonRPC, ws := probeBoth()
	if !jsonRPC {
		t.Error("json_rpc: did not skip despite 100 signals")
	}
	if ws {
		t.Error("websocket: skipped on the json_rpc key's traffic")
	}
}

// The map must not become another one that only ever grows: a backend that
// leaves the session takes its entry with it, the way lastRun does.
func TestTrafficSkipper_ForgetsBackendsItNoLongerProbes(t *testing.T) {
	c := &fakeCounter{signals: map[string]uint64{}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	s.beginCycle()
	for _, host := range []string{"a", "b", "c"} {
		ep := domain.EndpointAddr("supA-https://" + host + ".example.com")
		c.signals[string(ep)+"|json_rpc"] = 1
		s.skip("eth", ep, "https://"+host+".example.com", domain.RPCTypeJSONRPC)
	}
	s.endCycle()
	if len(s.prev) != 3 {
		t.Fatalf("baseline holds %d entries, want 3", len(s.prev))
	}

	// Next cycle sees only one of them.
	s.beginCycle()
	ep := domain.EndpointAddr("supA-https://a.example.com")
	s.skip("eth", ep, "https://a.example.com", domain.RPCTypeJSONRPC)
	s.endCycle()
	if len(s.prev) != 1 {
		t.Errorf("baseline holds %d entries after the other backends left, want 1", len(s.prev))
	}
}

// The threshold has to follow the sample rate, because the sample rate is what
// decides how much traffic it takes to replace a probe's observation.
func TestMinTrafficSignalsFor(t *testing.T) {
	if got := MinTrafficSignalsFor(0.1); got != DefaultMinTrafficSignals {
		t.Errorf("at the default 10%% sample rate: got %d, want DefaultMinTrafficSignals (%d)", got, DefaultMinTrafficSignals)
	}
	if got, want := MinTrafficSignalsFor(0.01), MinTrafficSignalsFor(0.1)*10; got != want {
		t.Errorf("at 1%%: got %d, want %d — ten times the traffic to say the same thing", got, want)
	}
	for _, rate := range []float64{0, 1, 1.5, -1} {
		if got := MinTrafficSignalsFor(rate); got != signalsPerRelay {
			t.Errorf("unsampled (%v): got %d, want one relay's worth (%d)", rate, got, signalsPerRelay)
		}
	}
}

// newSkipExecutor builds the smallest Executor that can answer a skip
// question: the guards are all it touches.
func newSkipExecutor(t *testing.T, signals uint64, flagOn, warm bool) (*Executor, *recordingResultRecorder) {
	t.Helper()
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	c := &fakeCounter{signals: map[string]uint64{string(ep) + "|json_rpc": signals}}

	e := &Executor{}
	e.skipper = newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})
	e.flags = featureflag.NewMemoryStore(map[string]bool{
		featureflag.FlagTrafficInformedProbing: flagOn,
	})
	e.warm.Store(warm)
	rec := &recordingResultRecorder{}
	e.recorder = rec

	// Establish the baseline the diff needs, at zero traffic.
	c.signals[string(ep)+"|json_rpc"] = 0
	e.skipper.beginCycle()
	e.skipCoveredByTraffic(context.Background(), "eth", ep, "https://node.example.com", skipTestCheck())
	e.skipper.endCycle()
	c.signals[string(ep)+"|json_rpc"] = signals
	return e, rec
}

func skipTestCheck() qos.HealthCheck {
	return qos.HealthCheck{
		Name:    "block_number",
		Payload: domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}
}

func askSkip(e *Executor) bool {
	e.skipper.beginCycle()
	defer e.skipper.endCycle()
	return e.skipCoveredByTraffic(context.Background(), "eth",
		"supA-https://node.example.com", "https://node.example.com", skipTestCheck())
}

// The three guards, each on its own. Traffic alone is not enough to stop a
// probe: the flag has to be on and the pod has to be warm.
func TestSkipCoveredByTraffic_Guards(t *testing.T) {
	cases := []struct {
		name     string
		flagOn   bool
		warm     bool
		wantSkip bool
	}{
		{name: "flag on and warm", flagOn: true, warm: true, wantSkip: true},
		{name: "flag off", flagOn: false, warm: true},
		{name: "not warm", flagOn: true, warm: false},
		{name: "neither", flagOn: false, warm: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, rec := newSkipExecutor(t, 100, tc.flagOn, tc.warm)
			if got := askSkip(e); got != tc.wantSkip {
				t.Errorf("skip = %v, want %v (100 signals, threshold 20)", got, tc.wantSkip)
			}
			wantCounts := 0
			if tc.wantSkip {
				wantCounts = 1
			}
			if len(rec.skipped) != wantCounts {
				t.Errorf("recorded %d skips, want %d", len(rec.skipped), wantCounts)
			}
		})
	}
}

// A pod with no skipper wired behaves exactly as it did before this existed.
func TestSkipCoveredByTraffic_NoSkipperNeverSkips(t *testing.T) {
	e := &Executor{}
	e.warm.Store(true)
	if e.skipCoveredByTraffic(context.Background(), "eth",
		"supA-https://node.example.com", "https://node.example.com", skipTestCheck()) {
		t.Error("skipped with no traffic skipper wired")
	}
}
