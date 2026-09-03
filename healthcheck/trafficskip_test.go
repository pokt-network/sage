package healthcheck

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

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

// clock advances by hand so a window is a duration, not a cycle count.
type clock struct{ t time.Time }

func newClock() *clock { return &clock{t: time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC)} }

func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

// cycle runs one skip decision inside a begin/end pair, the way runOnce does,
// at the default 60s check interval.
func cycle(t *testing.T, s *trafficSkipper, c *clock, ep domain.EndpointAddr) bool {
	t.Helper()
	return cycleEvery(t, s, c, ep, time.Minute)
}

func cycleEvery(t *testing.T, s *trafficSkipper, c *clock, ep domain.EndpointAddr, interval time.Duration) bool {
	t.Helper()
	s.beginCycle()
	got := s.skip("eth", ep, "https://node.example.com", domain.RPCTypeJSONRPC, interval, c.t)
	s.endCycle(c.t)
	return got
}

// The point: a backend carrying enough traffic to replace the probe's own
// observation stops being probed, and only then.
func TestTrafficSkipper_SkipsOnlyAboveThreshold(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	c := &fakeCounter{signals: map[string]uint64{string(ep) + "|json_rpc": 100}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	clk := newClock()

	// First sighting has no baseline to diff against, so it cannot skip
	// however much traffic the key has seen in its life.
	if cycle(t, s, clk, ep) {
		t.Fatal("skipped on the first sighting; there was no window to measure")
	}

	// A window with traffic below the threshold still gets probed: one relay
	// does not stand in for one probe.
	clk.advance(time.Minute)
	c.signals[string(ep)+"|json_rpc"] = 115
	if cycle(t, s, clk, ep) {
		t.Error("skipped on 15 signals with a threshold of 20")
	}

	// Enough traffic, and the probe is redundant.
	clk.advance(time.Minute)
	c.signals[string(ep)+"|json_rpc"] = 140
	if !cycle(t, s, clk, ep) {
		t.Error("did not skip on 25 signals with a threshold of 20")
	}

	// Traffic stops: the next window has nothing new, so probing resumes.
	clk.advance(time.Minute)
	if cycle(t, s, clk, ep) {
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

	clk := newClock()
	if cycle(t, s, clk, ep) {
		t.Fatal("skipped a key the counter does not know")
	}
	if _, recorded := s.prev[trafficKey{service: "eth", backend: "https://node.example.com", rpcType: domain.RPCTypeJSONRPC}]; recorded {
		t.Fatal("recorded a baseline for an unknown key; the next cycle would diff a lifetime against zero")
	}

	// The key becomes known, carrying the cumulative total it always had.
	clk.advance(time.Minute)
	c.known[key] = true
	if cycle(t, s, clk, ep) {
		t.Error("skipped on the first real reading: 5,000 lifetime signals are not 5,000 signals this window")
	}
	// And only now is there a baseline, so the cycle after can measure a real
	// window against it.
	clk.advance(time.Minute)
	c.signals[key] = 5100
	if !cycle(t, s, clk, ep) {
		t.Error("did not skip on 100 signals measured against a real baseline")
	}
}

// A count that went backwards is an eviction, not negative traffic.
func TestTrafficSkipper_ResetDoesNotSkip(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	c := &fakeCounter{signals: map[string]uint64{string(ep) + "|json_rpc": 500}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	clk := newClock()
	cycle(t, s, clk, ep) // establish the baseline at 500
	clk.advance(time.Minute)
	c.signals[string(ep)+"|json_rpc"] = 3
	if cycle(t, s, clk, ep) {
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

	clk := newClock()
	probeBoth := func() (jsonRPC, ws bool) {
		s.beginCycle()
		jsonRPC = s.skip("eth", ep, "https://node.example.com", domain.RPCTypeJSONRPC, time.Minute, clk.t)
		ws = s.skip("eth", ep, "https://node.example.com", domain.RPCTypeWebSocket, time.Minute, clk.t)
		s.endCycle(clk.t)
		return
	}
	probeBoth() // baselines
	clk.advance(time.Minute)

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

	clk := newClock()
	s.beginCycle()
	for _, host := range []string{"a", "b", "c"} {
		ep := domain.EndpointAddr("supA-https://" + host + ".example.com")
		c.signals[string(ep)+"|json_rpc"] = 1
		s.skip("eth", ep, "https://"+host+".example.com", domain.RPCTypeJSONRPC, time.Minute, clk.t)
	}
	s.endCycle(clk.t)
	if len(s.prev) != 3 {
		t.Fatalf("baseline holds %d entries, want 3", len(s.prev))
	}

	// The next cycle sees only one of them. The other two are CARRIED, not
	// dropped: a check that is not due this cycle must keep its baseline, and
	// from in here that is indistinguishable from a backend that has left.
	clk.advance(time.Minute)
	s.beginCycle()
	ep := domain.EndpointAddr("supA-https://a.example.com")
	s.skip("eth", ep, "https://a.example.com", domain.RPCTypeJSONRPC, time.Minute, clk.t)
	s.endCycle(clk.t)
	if len(s.prev) != 3 {
		t.Errorf("baseline holds %d entries, want all 3 carried: an unvisited key may simply not be due", len(s.prev))
	}

	// Age is what evicts. Past maxBaselineAge the departed backends go.
	clk.advance(maxBaselineAge + time.Minute)
	s.beginCycle()
	s.skip("eth", ep, "https://a.example.com", domain.RPCTypeJSONRPC, time.Minute, clk.t)
	s.endCycle(clk.t)
	if len(s.prev) != 1 {
		t.Errorf("baseline holds %d entries after %v, want 1 — stale readings must not accumulate", len(s.prev), maxBaselineAge)
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

	e := &Executor{qosRegistry: qos.NewRegistry()}
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
	e.skipCoveredByTraffic(context.Background(), "eth", ep, domain.EndpointAddrList{ep},
		"https://node.example.com", skipTestCheck(), time.Minute, skipTestClock)
	e.skipper.endCycle(skipTestClock)
	c.signals[string(ep)+"|json_rpc"] = signals
	return e, rec
}

// skipTestClock is the moment the baseline above is taken; askSkip asks a full
// interval later so the window has elapsed.
var skipTestClock = time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC)

func skipTestCheck() qos.HealthCheck {
	return qos.HealthCheck{
		Name:    "block_number",
		Payload: domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}
}

func askSkip(e *Executor) bool {
	e.skipper.beginCycle()
	defer e.skipper.endCycle(skipTestClock.Add(time.Minute))
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	return e.skipCoveredByTraffic(context.Background(), "eth", ep, domain.EndpointAddrList{ep},
		"https://node.example.com", skipTestCheck(), time.Minute, skipTestClock.Add(time.Minute))
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
	e := &Executor{qosRegistry: qos.NewRegistry()}
	e.warm.Store(true)
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	if e.skipCoveredByTraffic(context.Background(), "eth", ep, domain.EndpointAddrList{ep},
		"https://node.example.com", skipTestCheck(), time.Minute, skipTestClock) {
		t.Error("skipped with no traffic skipper wired")
	}
}

// The canary bug, 2026-09-03: flag on, thousands of traffic signals per key
// per interval, zero skips forever.
//
// The first version diffed against "the previous cycle" and recorded a reading
// only when a check was due. The executor's tick is the SHORTEST interval
// across all services, so a 60s check on a fleet with one 20s check runs on
// one cycle in three — and on the two cycles in between, its key was absent
// from the map that became the next baseline. The baseline was therefore never
// present when the check was due, and the skip could never fire. One fast
// check anywhere silently disabled the feature everywhere.
func TestTrafficSkipper_SurvivesATickShorterThanTheCheckInterval(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	key := string(ep) + "|json_rpc"
	c := &fakeCounter{signals: map[string]uint64{key: 1000}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	const (
		tick     = 20 * time.Second // some other service probes this fast
		interval = time.Minute      // this check's own cadence
	)
	clk := newClock()

	// Six ticks: the check is due on ticks 3 and 6, and traffic accrues
	// steadily throughout.
	skipped := 0
	for i := 1; i <= 6; i++ {
		clk.advance(tick)
		c.signals[key] += 500
		if i%3 != 0 {
			// Not due: the executor does not even ask about this check.
			continue
		}
		if cycleEvery(t, s, clk, ep, interval) {
			skipped++
		}
	}

	if skipped == 0 {
		t.Fatal("never skipped across two full check intervals with 1,500 signals in each; " +
			"the baseline is being dropped on the cycles where the check is not due")
	}
}

// The window is a duration, so a check asked about more often than its own
// interval must not have its baseline reset each time and never accumulate.
func TestTrafficSkipper_CarriesTheBaselineUntilTheWindowElapses(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	key := string(ep) + "|json_rpc"
	c := &fakeCounter{signals: map[string]uint64{key: 0}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	clk := newClock()
	cycleEvery(t, s, clk, ep, time.Minute) // baseline at t=0

	// Six 10-second cycles at 5 signals each: never enough inside one cycle,
	// plenty across the minute.
	var skipped bool
	for range 6 {
		clk.advance(10 * time.Second)
		c.signals[key] += 5
		if cycleEvery(t, s, clk, ep, time.Minute) {
			skipped = true
		}
	}
	if !skipped {
		t.Error("never skipped: 30 signals accrued over a full minute against a threshold of 20, " +
			"so the baseline was being reset before the window could elapse")
	}
}

// And the window is respected in the other direction: a burst inside a window
// shorter than the interval does not skip, because the probe it replaces
// covers the whole interval.
func TestTrafficSkipper_WaitsForTheWindow(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	key := string(ep) + "|json_rpc"
	c := &fakeCounter{signals: map[string]uint64{key: 0}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 20})

	clk := newClock()
	cycleEvery(t, s, clk, ep, time.Minute)

	clk.advance(5 * time.Second)
	c.signals[key] = 10_000
	if cycleEvery(t, s, clk, ep, time.Minute) {
		t.Error("skipped after 5s of a 60s window; the probe covers the interval, not the instant")
	}
}

// The height check is skipped only when the plugin has actually received a
// height for this backend inside the probe's own interval.
//
// The blunt version of this rule — never skip an essential check — was right
// about the uncertainty and wrong about what to do with it. The traffic
// threshold guarantees how many observations arrive, not that any contains a
// height, because only one method per chain yields one. But the canary then
// showed sixteen busy services at 2-5 seconds of chain-view staleness against
// an 86-second cycle: client traffic was supplying heights continuously, and
// refusing to skip there protected nothing. The question is not whether
// traffic COULD carry a height, it is whether it DID.
func TestSkipCoveredByTraffic_EssentialCheckFollowsTheObservation(t *testing.T) {
	essential := skipTestCheck()
	essential.Essential = true
	const ep = domain.EndpointAddr("supA-https://node.example.com")

	ask := func(t *testing.T, plugin qos.Plugin) (bool, *recordingResultRecorder) {
		t.Helper()
		e, rec := newSkipExecutor(t, 100_000, true, true)
		reg := qos.NewRegistry()
		if err := reg.Register("eth", plugin); err != nil {
			t.Fatal(err)
		}
		e.qosRegistry = reg
		return e.skipCoveredByTraffic(context.Background(), "eth", ep, domain.EndpointAddrList{ep},
			"https://node.example.com", essential, time.Minute, skipTestClock.Add(time.Minute)), rec
	}
	observedAgo := func(d time.Duration) qos.Plugin {
		return &stubHeightObserver{seen: true, last: skipTestClock.Add(time.Minute).Add(-d)}
	}

	t.Run("a height inside the interval makes the probe redundant", func(t *testing.T) {
		got, rec := ask(t, observedAgo(5*time.Second))
		if !got {
			t.Error("did not skip: a height arrived 5s ago against a 60s interval, so the probe learns nothing")
		}
		if len(rec.skipped) != 1 {
			t.Errorf("recorded %d skips, want 1", len(rec.skipped))
		}
	})

	t.Run("a height older than the interval does not", func(t *testing.T) {
		if got, _ := ask(t, observedAgo(2*time.Minute)); got {
			t.Error("skipped on a height older than the interval — that is the staleness the probe exists to prevent")
		}
	})

	t.Run("no height at all does not", func(t *testing.T) {
		if got, _ := ask(t, &stubHeightObserver{}); got {
			t.Error("skipped with no height observation; unknown is not fresh")
		}
	})

	// The interface says the timestamp is meaningless when the bool is false,
	// so the caller has to read the bool. BlockConsensus happens to return a
	// zero time there, which the freshness arithmetic would reject on its own
	// — but that is its accident, not the contract, and a future implementation
	// returning a real time with false must not be read as fresh.
	t.Run("a recent timestamp reported as unseen does not", func(t *testing.T) {
		liar := &stubHeightObserver{seen: false, last: skipTestClock.Add(time.Minute)}
		if got, _ := ask(t, liar); got {
			t.Error("skipped on a timestamp the observer said was not real; the bool is the answer, not the time")
		}
	})

	t.Run("a plugin that tracks no height does not", func(t *testing.T) {
		if got, _ := ask(t, stubPluginNoHeight{}); got {
			t.Error("skipped for a plugin that cannot report an observation")
		}
	})

	// The non-essential check beside it is unaffected: it skips on traffic
	// volume alone, which is what it always did.
	t.Run("a non-essential check still skips on traffic", func(t *testing.T) {
		e, _ := newSkipExecutor(t, 100_000, true, true)
		if !e.skipCoveredByTraffic(context.Background(), "eth", ep, domain.EndpointAddrList{ep},
			"https://node.example.com", skipTestCheck(), time.Minute, skipTestClock.Add(time.Minute)) {
			t.Error("a non-essential check with the same traffic did not skip")
		}
	})
}

// stubHeightObserver is a plugin that reports a height observation.
type stubHeightObserver struct {
	stubPluginNoHeight
	seen bool
	last time.Time
}

func (s *stubHeightObserver) LastHeightObservation(domain.EndpointAddrList) (time.Time, bool) {
	return s.last, s.seen
}

// stubPluginNoHeight is the core Plugin interface and nothing else — the shape
// of a service whose plugin tracks no chain.
type stubPluginNoHeight struct{}

func (stubPluginNoHeight) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (stubPluginNoHeight) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}

// The cadence a health checker achieves is the longer of its configured
// interval and how long a cycle takes, and until 2026-09-03 nothing said so.
//
// The cycle runs on the ticker's own goroutine and dispatch blocks on a fixed
// worker pool, so a cycle that outlasts its tick delays the next one rather
// than overlapping it — time.Ticker drops the tick it missed. On the mainnet
// canary that showed up as a service flat for fourteen minutes on a
// sixty-second interval and then jumping thirty-four probes at once, and it
// cost a wrong prediction and a round trip to explain, because the only
// evidence was per-service probe rates that were really sampling artifacts.
func TestRecordCycle_ReportsAndWarnsOnOverrun(t *testing.T) {
	cases := []struct {
		name        string
		elapsed     time.Duration
		tick        time.Duration
		wantOverrun bool
	}{
		{name: "inside the tick", elapsed: 10 * time.Second, tick: time.Minute},
		{name: "exactly the tick is not an overrun", elapsed: time.Minute, tick: time.Minute},
		{name: "over the tick", elapsed: 14 * time.Minute, tick: time.Minute, wantOverrun: true},
		{name: "no tick configured", elapsed: time.Hour, tick: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf strings.Builder
			e := &Executor{
				logger:  slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
				workers: 4,
			}
			rec := &recordingResultRecorder{}
			e.recorder = rec

			e.recordCycle(tc.elapsed, tc.tick)

			// Every cycle is timed, overrun or not: the histogram is how an
			// operator sees the achieved cadence at all.
			if len(rec.cycles) != 1 {
				t.Fatalf("recorded %d cycles, want 1", len(rec.cycles))
			}
			if rec.cycles[0].elapsed != tc.elapsed || rec.cycles[0].tick != tc.tick {
				t.Errorf("recorded %+v, want elapsed %v tick %v", rec.cycles[0], tc.elapsed, tc.tick)
			}

			warned := strings.Contains(buf.String(), "overran its interval")
			if warned != tc.wantOverrun {
				t.Errorf("warned = %v, want %v (log: %q)", warned, tc.wantOverrun, buf.String())
			}
		})
	}
}

// A nil recorder must not panic: the executor runs without metrics in tests
// and in a minimal wiring.
func TestRecordCycle_NilRecorder(t *testing.T) {
	e := &Executor{logger: slog.New(slog.DiscardHandler), workers: 4}
	e.recordCycle(10*time.Minute, time.Minute) // must not panic
}

// A probe must be bounded on its own, not by the client relay timeout it would
// otherwise inherit.
//
// Without this a hung backend holds one of four workers for the whole relay
// timeout — 30s unconfigured — so a handful of dead backends per sweep spends
// most of the fleet's probe capacity waiting. On the mainnet canary on
// 2026-09-03 that produced a mean probe latency of 1.39s at four-way
// concurrency and a sweep of five to nineteen minutes against a sixty-second
// configured interval.
func TestProbeTimeout(t *testing.T) {
	t.Run("defaults when unset", func(t *testing.T) {
		e := &Executor{}
		if got := e.probeTimeout(); got != DefaultProbeTimeout {
			t.Errorf("probeTimeout = %v, want %v", got, DefaultProbeTimeout)
		}
	})

	t.Run("zero and negative restore the default", func(t *testing.T) {
		for _, d := range []time.Duration{0, -time.Second} {
			e := &Executor{}
			e.SetProbeTimeout(d)
			if got := e.probeTimeout(); got != DefaultProbeTimeout {
				t.Errorf("SetProbeTimeout(%v): got %v, want %v", d, got, DefaultProbeTimeout)
			}
		}
	})

	t.Run("an explicit value is honoured", func(t *testing.T) {
		e := &Executor{}
		e.SetProbeTimeout(2 * time.Second)
		if got := e.probeTimeout(); got != 2*time.Second {
			t.Errorf("probeTimeout = %v, want 2s", got)
		}
	})

	// The bound that matters: well below the 30s relay timeout a probe would
	// otherwise inherit, and well above a healthy response.
	t.Run("the default is bounded on both sides", func(t *testing.T) {
		if DefaultProbeTimeout >= 30*time.Second {
			t.Errorf("DefaultProbeTimeout = %v: at or above the relay timeout it exists to escape", DefaultProbeTimeout)
		}
		if DefaultProbeTimeout < time.Second {
			t.Errorf("DefaultProbeTimeout = %v: a probe cut off this early grades a loaded supplier as failing", DefaultProbeTimeout)
		}
	})
}

// The deadline has to reach the probe, not just sit on the executor.
func TestSendCheck_AppliesTheProbeDeadline(t *testing.T) {
	relayer := &deadlineRelayer{}
	e := &Executor{
		logger:          slog.New(slog.DiscardHandler),
		protocol:        relayer,
		qosRegistry:     qos.NewRegistry(),
		coveredServices: make(map[domain.ServiceID]struct{}),
	}
	// Warm short-circuits markCovered, which would otherwise need a session
	// manager this test has no use for.
	e.warm.Store(true)
	e.SetProbeTimeout(50 * time.Millisecond)

	e.sendCheck(context.Background(), "eth", "supA-https://a.example.com", nil, nil, skipTestCheck(), nil)

	if !relayer.called {
		t.Fatal("probe was never sent")
	}
	if !relayer.hadDeadline {
		t.Fatal("probe ran with no deadline; it would inherit the 30s relay timeout")
	}
	if relayer.remaining <= 0 || relayer.remaining > 50*time.Millisecond {
		t.Errorf("probe deadline %v away, want within the 50ms budget", relayer.remaining)
	}
}

// deadlineRelayer records what deadline the probe context carried.
type deadlineRelayer struct {
	called      bool
	hadDeadline bool
	remaining   time.Duration
}

func (d *deadlineRelayer) SendRelay(ctx context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.Payload) (*domain.Response, error) {
	d.called = true
	if dl, ok := ctx.Deadline(); ok {
		d.hadDeadline = true
		d.remaining = time.Until(dl)
	}
	return &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)}, nil
}

// The canary bug of 2026-09-03, second instance: 0% skip on a service with
// plenty of traffic, an hour after the same code skipped 40%.
//
// A reading is taken only when a check is DUE, and endCycle used to promote
// only what the cycle visited. So a check whose interval is longer than a
// cycle — the EVM chain-id check at five minutes against a cycle of about
// seventy seconds — lost its baseline on the three cycles in between, had none
// when it was finally due, and could never skip. It worked an hour earlier
// only because cycles were then SLOWER than every check interval, so every key
// was visited every cycle; the probe timeout made cycles short enough for the
// gap to open.
//
// The Essential carve-out makes this total rather than partial on an EVM
// service: eth_blockNumber never reaches the skipper, so the five-minute
// chain-id check is the ONLY skippable one, and it was the one being broken.
func TestTrafficSkipper_SlowCheckKeepsItsBaselineAcrossCycles(t *testing.T) {
	const ep = domain.EndpointAddr("supA-https://node.example.com")
	key := string(ep) + "|json_rpc"
	c := &fakeCounter{signals: map[string]uint64{key: 0}}
	s := newTrafficSkipper(c, TrafficSkipConfig{MinSignals: 4})

	const (
		cycle         = 74 * time.Second // what the probe timeout produced
		checkInterval = 5 * time.Minute  // eth_chainId
	)
	clk := newClock()

	// The check is due roughly every fourth cycle. Traffic accrues throughout
	// at a rate that clears the threshold many times over.
	skipped, dueCount := 0, 0
	nextDue := clk.t
	for range 20 {
		clk.advance(cycle)
		c.signals[key] += 10

		s.beginCycle()
		if !clk.t.Before(nextDue) {
			dueCount++
			if s.skip("eth", ep, "https://node.example.com", domain.RPCTypeJSONRPC, checkInterval, clk.t) {
				skipped++
			}
			nextDue = clk.t.Add(checkInterval)
		}
		s.endCycle(clk.t)
	}

	if dueCount < 4 {
		t.Fatalf("check was due only %d times in 20 cycles; test is not exercising the gap", dueCount)
	}
	if skipped == 0 {
		t.Fatalf("skipped 0 of %d due checks with 10 signals per cycle against a threshold of 4: "+
			"the baseline is being dropped on the cycles where the check is not due", dueCount)
	}
}
