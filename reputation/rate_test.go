package reputation

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
)

func TestRateConfig_Normalized_Defaults(t *testing.T) {
	c := RateConfig{}.Normalized()
	assert.Equal(t, DefaultHalfLifeAttempts, c.HalfLifeAttempts)
	assert.Equal(t, DefaultOnsetRate, c.OnsetRate)
	assert.Equal(t, DefaultFullRate, c.FullRate)
	assert.True(t, c.Enabled())
	assert.InDelta(t, math.Ln2/20000, c.Lambda(), 1e-12)
}

func TestRateConfig_NegativeHalfLifeDisables(t *testing.T) {
	c := RateConfig{HalfLifeAttempts: -1}.Normalized()
	assert.False(t, c.Enabled())
	assert.Equal(t, 0.0, c.Penalty(0.5), "a disabled term penalises nothing")
}

func TestRateConfig_PenaltyCurve(t *testing.T) {
	c := RateConfig{}.Normalized()
	assert.Equal(t, 0.0, c.Penalty(0))
	assert.Equal(t, 0.0, c.Penalty(0.0002), "onset itself is free")
	assert.InDelta(t, -40, c.Penalty(0.01), 1e-9, "full rate is -40")
	// Midpoint of the log span between onset (2e-4) and full (1e-2): sqrt(2e-6) ≈ 1.414e-3.
	assert.InDelta(t, -20, c.Penalty(math.Sqrt(0.0002*0.01)), 1e-6)
	assert.InDelta(t, -70, c.Penalty(0.1), 1e-9, "-70 at 10x full")
	assert.Equal(t, -70.0, c.Penalty(0.5), "capped at -70")
	// The numbers docs/scoring.md §7.3 quotes.
	assert.InDelta(t, -23.5, c.Penalty(0.002), 0.6, "a 0.2% violator lands tier 2")
	assert.InDelta(t, -12, c.Penalty(0.00065), 1.0, "a 0.065% violator keeps tier 1")
}

func TestFailureWeight(t *testing.T) {
	assert.Equal(t, 1.0, FailureWeight(SignalCriticalError))
	assert.Equal(t, 1.0, FailureWeight(SignalFatalError))
	assert.Equal(t, 0.5, FailureWeight(SignalMajorError))
	assert.Equal(t, 0.0, FailureWeight(SignalMinorError))
	assert.Equal(t, 0.0, FailureWeight(SignalSuccess))
}

// TestRateTerm_BurstVsChronic reproduces the simulation the constants were
// chosen from: a burst of 3 criticals on a clean key is free, 20 in a row
// cost about -13, and a steady 0.2% failure rate settles near -23.
func TestRateTerm_BurstVsChronic(t *testing.T) {
	c := RateConfig{}.Normalized()
	burst := func(n int) float64 {
		r := 0.0
		for i := 0; i < n; i++ {
			r += c.Lambda() * (1 - r)
		}
		return c.Penalty(r)
	}
	assert.Equal(t, 0.0, burst(3))
	assert.InDelta(t, -12.7, burst(20), 0.5)

	// Deterministic 0.2%: one critical every 500 attempts, 100k attempts.
	r := 0.0
	for i := 1; i <= 100_000; i++ {
		w := 0.0
		if i%500 == 0 {
			w = 1
		}
		r += c.Lambda() * (w - r)
	}
	require.InDelta(t, 0.002, r, 0.0003)
	assert.InDelta(t, -23.5, c.Penalty(r), 1.5)
}

// An inconsistent onset/full pair is treated as unset. Config callers are
// refused earlier by validateReputation; this is the guard for programmatic
// ones, where an inverted or out-of-range pair would make Penalty return NaN
// or reward failure instead of penalising it.
func TestRateConfig_NormalizedResetsAnInconsistentPair(t *testing.T) {
	def := RateConfig{}.Normalized()
	for name, in := range map[string]RateConfig{
		"inverted":       {OnsetRate: 0.05, FullRate: 0.01},
		"equal":          {OnsetRate: 0.01, FullRate: 0.01},
		"negative onset": {OnsetRate: -0.1, FullRate: 0.01},
		"negative full":  {OnsetRate: 0.0002, FullRate: -0.01},
		"full above 1":   {OnsetRate: 0.0002, FullRate: 1.5},
		"full at 1":      {OnsetRate: 0.0002, FullRate: 1},
	} {
		t.Run(name, func(t *testing.T) {
			got := in.Normalized()
			assert.Equal(t, def.OnsetRate, got.OnsetRate, "both rates reset, not just the bad one")
			assert.Equal(t, def.FullRate, got.FullRate)
			assert.False(t, math.IsNaN(got.Penalty(0.005)), "the curve is a number again")
			assert.Less(t, got.Penalty(0.005), 0.0, "and still penalises")

			// Defaulted must NOT repair: config validation reads the rates
			// through it so an operator's inconsistent pair is refused at load
			// time rather than quietly turned into the defaults.
			raw := in.Defaulted()
			assert.Equal(t, in.OnsetRate, raw.OnsetRate, "Defaulted repaired what validation must refuse")
			assert.Equal(t, in.FullRate, raw.FullRate, "Defaulted repaired what validation must refuse")
		})
	}

	// A consistent non-default pair is left exactly as given.
	ok := RateConfig{OnsetRate: 0.001, FullRate: 0.05}.Normalized()
	assert.Equal(t, 0.001, ok.OnsetRate)
	assert.Equal(t, 0.05, ok.FullRate)
}

// A WebSocket key that stops failing loses half its rate every
// wsRateHalfLife, whatever its traffic; an HTTP key's rate moves only with
// attempts, as before.
func TestWSRate_FadesWithTime(t *testing.T) {
	ctx := context.Background()
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{StateIdleTTL: -1})
	ep := domain.EndpointAddr("pokt1a-wss://ws.example")
	t0 := time.Now().Add(-24 * time.Hour)
	for _, rpc := range []domain.RPCType{domain.RPCTypeWebSocket, domain.RPCTypeJSONRPC} {
		for i := range 50 {
			sig := NewSignal(SignalCriticalError, "x", 0)
			sig.Timestamp = t0.Add(time.Duration(i) * time.Second)
			_ = s.RecordSignal(ctx, "poly", ep, rpc, sig)
		}
	}
	rateOf := func(rpc domain.RPCType) float64 {
		k := s.keyOf(ep, rpc)
		sh := s.shard(k)
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		return sh.cache["poly"][k].Rate
	}
	wsBefore, httpBefore := rateOf(domain.RPCTypeWebSocket), rateOf(domain.RPCTypeJSONRPC)
	require.Positive(t, wsBefore)
	require.Positive(t, httpBefore)

	// One success twelve hours later: two half-lives for WebSocket.
	for _, rpc := range []domain.RPCType{domain.RPCTypeWebSocket, domain.RPCTypeJSONRPC} {
		sig := NewSignal(SignalSuccess, "ok", 0)
		sig.Timestamp = t0.Add(12 * time.Hour)
		_ = s.RecordSignal(ctx, "poly", ep, rpc, sig)
	}
	if got, want := rateOf(domain.RPCTypeWebSocket), wsBefore/4; math.Abs(got-want) > want*0.01 {
		t.Errorf("websocket rate %.6f, want about %.6f (two half-lives)", got, want)
	}
	if got := rateOf(domain.RPCTypeJSONRPC); got < httpBefore*0.99 {
		t.Errorf("json_rpc rate %.6f faded with time from %.6f", got, httpBefore)
	}
}

// A WebSocket success that stands for many answered requests moves the
// failure rate as that many successes would: a key that failed often has its
// rate halved by one half-life's worth of answers, where one success barely
// moves it. The score moves by one success either way.
func TestWSRate_WeightedSuccessCountsAnsweredRequests(t *testing.T) {
	ep := domain.EndpointAddr("pokt1a-https://ws.example")
	run := func(weight int) (rate, score float64) {
		s := NewService(NewMemoryStorage(), nil, ServiceConfig{StateIdleTTL: -1})
		ctx := context.Background()
		now := time.Now()
		for range 5 { // 5 majors: score 50, not floored (a floored key's rate is frozen)
			_ = s.RecordSignal(ctx, "eth", ep, domain.RPCTypeWebSocket, Signal{Type: SignalMajorError, Timestamp: now})
		}
		_ = s.RecordSignal(ctx, "eth", ep, domain.RPCTypeWebSocket, Signal{Type: SignalSuccess, Timestamp: now, Weight: weight})
		views, _ := s.GetStates(ctx, "eth")
		for _, v := range views {
			return v.Rate, v.Score
		}
		t.Fatal("no state recorded")
		return 0, 0
	}
	oneRate, oneScore := run(1)
	halfLife := DefaultHalfLifeAttempts
	manyRate, manyScore := run(halfLife)
	if math.Abs(manyRate-oneRate/2) > oneRate*0.01 {
		t.Errorf("a success standing for %d answers left rate %.6f; want about half of %.6f", halfLife, manyRate, oneRate)
	}
	if manyScore != oneScore {
		t.Errorf("weight moved the score: %.1f vs %.1f", manyScore, oneScore)
	}
}
