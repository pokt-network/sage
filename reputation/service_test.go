package reputation

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
)

func newTestServiceStore() (*serviceImpl, *MemoryStorage) {
	store := NewMemoryStorage()
	tl := NewTimeline(100)
	svc := NewService(store, tl, DefaultServiceConfig())
	svc.Start()
	return svc, store
}

func TestService_RecordSignal_UpdatesCache(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("supplier1-https://example.com")

	// Initial score should be 100 (default).
	score, err := svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatal(err)
	}
	if score != 100 {
		t.Errorf("initial score = %f, want 100", score)
	}

	// Record a major error (-10).
	err = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewMajorErrorSignal("timeout", 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	score, _ = svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if score != 90 {
		t.Errorf("score after major error = %f, want 90", score)
	}
}

func TestService_ScoreClamping(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("ep1")

	// Drive score below 0 with fatal errors.
	for range 5 {
		_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewFatalErrorSignal("crash", 0))
	}

	score, _ := svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if score != 0 {
		t.Errorf("score should be clamped to 0, got %f", score)
	}

	// Reset and drive above max with successes.
	_ = svc.ResetScore(ctx, svcID, ep)
	for range 10 {
		_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", time.Millisecond))
	}

	score, _ = svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if score != 100 {
		t.Errorf("score should be clamped to 100, got %f", score)
	}
}

func TestService_AsyncWriteToStorage(t *testing.T) {
	svc, store := newTestServiceStore()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("ep1")

	_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewMinorErrorSignal("retry", 0))

	// Stop flushes pending writes.
	svc.Stop()

	key := scoreKey(svcID, svc.key(ep, domain.RPCTypeJSONRPC))
	st, err := store.GetState(ctx, key)
	if err != nil {
		t.Fatalf("expected state in storage after Stop, got error: %v", err)
	}
	if st.Score != 97 { // 100 - 3
		t.Errorf("stored score = %f, want 97", st.Score)
	}
}

func TestService_GetScores(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")

	_ = svc.RecordSignal(ctx, svcID, domain.EndpointAddr("ep1"), domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0))
	_ = svc.RecordSignal(ctx, svcID, domain.EndpointAddr("ep2"), domain.RPCTypeJSONRPC, NewMajorErrorSignal("fail", 0))

	scores, err := svc.GetScores(ctx, svcID)
	if err != nil {
		t.Fatal(err)
	}
	if len(scores) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(scores))
	}
}

func TestService_SelectBest(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")

	ep1 := domain.EndpointAddr("ep1")
	ep2 := domain.EndpointAddr("ep2")

	// Push ep1 below Tier 1 (100 → 75 via critical error, -25). ep2 stays
	// at 100 in Tier 1; the tier cascade picks ep2 deterministically.
	_ = svc.RecordSignal(ctx, svcID, ep1, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("fail", 0))

	best := svc.SelectBest(ctx, svcID, domain.EndpointAddrList{ep1, ep2}, domain.RPCTypeJSONRPC)
	if best != ep2 {
		t.Errorf("expected ep2 (higher tier), got %s", best)
	}
}

// TestService_SelectBest_WithinTierSpread asserts that when multiple endpoints
// share the top tier, SelectBest distributes picks across them rather than
// deterministically concentrating on one. This is the structural fix for
// PATH's supplier-concentration failure.
func TestService_SelectBest_WithinTierSpread(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")

	const n = 4
	eps := make(domain.EndpointAddrList, n)
	for i := 0; i < n; i++ {
		eps[i] = domain.EndpointAddr(string(rune('a' + i)))
	}
	// Leave all endpoints at the initial score (100 → all T1).

	const trials = 2000
	counts := make(map[domain.EndpointAddr]int, n)
	for i := 0; i < trials; i++ {
		pick := svc.SelectBest(ctx, svcID, eps, domain.RPCTypeJSONRPC)
		counts[pick]++
	}
	// Every endpoint must see meaningful traffic (expected ~500 each).
	// Allow a wide band (>= 15% of expected) to keep the test stable.
	floor := int(0.15 * float64(trials) / float64(n))
	for _, ep := range eps {
		if counts[ep] < floor {
			t.Errorf("endpoint %q got only %d picks over %d trials (floor=%d); selection is concentrating", ep, counts[ep], trials, floor)
		}
	}
}

func TestService_SelectBest_Empty(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	best := svc.SelectBest(context.Background(), "eth", nil, domain.RPCTypeJSONRPC)
	if best != "" {
		t.Errorf("expected empty for nil endpoints, got %s", best)
	}
}

func TestService_ResetScore(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("ep1")

	_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewFatalErrorSignal("crash", 0))
	score, _ := svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if score == 100 {
		t.Fatal("score should have decreased")
	}

	_ = svc.ResetScore(ctx, svcID, ep)
	score, _ = svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if score != 100 {
		t.Errorf("score after reset = %f, want 100", score)
	}
}

// TestService_Vouched exercises the beta-observed cold-start hole: right
// after boot, before any signal, an endpoint has no recorded score, and
// scoreForSelector would substitute InitialScore — enough to clear the
// probation threshold and look "fine" to a filter. Vouched must say no to
// exactly that endpoint, because a method block diverting traffic must not
// treat an unmeasured host as vouched for.
func TestService_Vouched(t *testing.T) {
	svc, _ := newTestServiceStore()
	defer svc.Stop()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("supplier1-https://example.com")

	// Fresh endpoint, no signal recorded yet: not vouched for, even though
	// GetScore/scoreForSelector would answer InitialScore (100) for it.
	if svc.Vouched(ctx, svcID, ep, domain.RPCTypeJSONRPC) {
		t.Fatal("an endpoint with no recorded score must not be vouched for")
	}

	// One success signal writes a real score at/above the probation
	// threshold into the cache: now vouched for.
	if err := svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !svc.Vouched(ctx, svcID, ep, domain.RPCTypeJSONRPC) {
		t.Fatal("a recorded score above the probation threshold must be vouched for")
	}

	// Enough failures to drop the recorded score below the probation
	// threshold: not vouched for any more.
	for range 2 {
		_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewFatalErrorSignal("crash", 0))
	}
	score, _ := svc.GetScore(ctx, svcID, ep, domain.RPCTypeJSONRPC)
	if score >= DefaultSelectorConfig().ProbationThreshold {
		t.Fatalf("setup failed: score %v did not drop below probation", score)
	}
	if svc.Vouched(ctx, svcID, ep, domain.RPCTypeJSONRPC) {
		t.Fatal("a score below the probation threshold must not be vouched for")
	}
}

// Under per-endpoint granularity the reputation key carries the supplier
// address, which rotates every session, so this map grows with the network
// rather than with SAGE's traffic. It is written on the relay path and nothing
// else shrinks it.
func TestRecordSignal_ScoreCacheIsBounded(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{KeyGranularity: KeyPerEndpoint})
	ctx := context.Background()

	// Every one of these is a distinct supplier on the same backend, which is
	// exactly what a few days of session rollovers produces.
	for i := range 200_000 {
		ep := domain.EndpointAddr(fmt.Sprintf("pokt1supplier%06d-https://node.example.com", i))
		if err := svc.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess}); err != nil {
			t.Fatalf("RecordSignal: %v", err)
		}
	}

	total := 0
	for i := range svc.shards {
		svc.shards[i].mu.RLock()
		total += len(svc.shards[i].cache["eth"])
		svc.shards[i].mu.RUnlock()
	}

	if total >= 200_000 {
		t.Errorf("cache holds %d keys after 200k rotated suppliers — nothing is bounding it", total)
	}
}

// Pruning must never forgive a penalized endpoint: this cache is the read path,
// and a miss reads back as InitialScore.
func TestRecordSignal_PruningKeepsPenalizedScores(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{KeyGranularity: KeyPerEndpoint})
	ctx := context.Background()

	bad := domain.EndpointAddr("pokt1bad-https://bad.example.com")
	for range 3 {
		if err := svc.RecordSignal(ctx, "eth", bad, domain.RPCTypeJSONRPC, Signal{Type: SignalCriticalError}); err != nil {
			t.Fatalf("RecordSignal: %v", err)
		}
	}
	penalized, _ := svc.GetScore(ctx, "eth", bad, domain.RPCTypeJSONRPC)
	if penalized >= 100 {
		t.Fatalf("setup failed: score %v is not penalized", penalized)
	}

	// Now flood the same shard set with healthy keys to force pruning.
	for i := range 200_000 {
		ep := domain.EndpointAddr(fmt.Sprintf("pokt1ok%06d-https://node.example.com", i))
		_ = svc.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess})
	}

	after, _ := svc.GetScore(ctx, "eth", bad, domain.RPCTypeJSONRPC)
	if after != penalized {
		t.Errorf("penalized score went from %v to %v — pruning forgave the endpoint worth remembering", penalized, after)
	}
}

// newTestService builds a started service on the given config, stopped by
// cleanup. The scoring-v2 tests care about state, not about storage.
func newTestService(t *testing.T, cfg ServiceConfig) *serviceImpl {
	t.Helper()
	svc := NewService(NewMemoryStorage(), NewTimeline(100), cfg)
	svc.Start()
	t.Cleanup(svc.Stop)
	return svc
}

func TestService_RateTermLowersEffectiveScore(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	// Additive term clamps at 100 no matter what; the rate term must not.
	for i := 0; i < 50_000; i++ {
		sig := NewSuccessSignal("ok", 0)
		if i%100 == 0 { // 1% critical
			sig = NewCriticalErrorSignal("bad", 0)
		}
		require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, sig))
	}
	// The EWMA is still climbing at 50k attempts — 50000*lambda is 2.5
	// half-lives, not infinity — so the expectation is the value the term
	// actually reaches (about 0.0082), not the 0.01 it is converging on.
	// Asserting 0.01 with enough slack to pass would document a number this
	// test never sees.
	rc := RateConfig{}.Normalized()
	wantRate := 0.01 * (1 - math.Exp(-50_000*rc.Lambda()))
	wantPenalty := rc.Penalty(wantRate)

	score, _ := svc.GetScore(ctx, "svc", ep, domain.RPCTypeJSONRPC)
	assert.InDelta(t, 100+wantPenalty, score, 1)
	assert.InDelta(t, 60, score, 5, "1% chronic failure: additive 100, penalty about -40")
	views, err := svc.GetStates(ctx, "svc")
	require.NoError(t, err)
	v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
	assert.InDelta(t, 100, v.Additive, 0.01)
	assert.InDelta(t, wantPenalty, v.Penalty, 1)
	assert.InDelta(t, wantRate, v.Rate, 0.0005)
	assert.Equal(t, uint64(50_000), v.Attempts)
	assert.Equal(t, uint64(50_000), v.TrafficAttempts)
	assert.False(t, v.ProbeOnly)
}

func TestService_RateTermOff(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Rate.HalfLifeAttempts = -1
	svc := newTestService(t, cfg)
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	for i := 0; i < 20_000; i++ {
		sig := NewSuccessSignal("ok", 0)
		if i%50 == 0 {
			sig = NewCriticalErrorSignal("bad", 0)
		}
		require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, sig))
	}
	score, _ := svc.GetScore(ctx, "svc", ep, domain.RPCTypeJSONRPC)
	assert.Equal(t, 100.0, score, "term off: additive alone, which clamps")
}

func TestService_SignalImpactsConfigured(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.Impacts = SignalImpacts{Success: 1, CriticalError: -50}
	svc := newTestService(t, cfg)
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("bad", 0)))
	score, _ := svc.GetScore(ctx, "svc", ep, domain.RPCTypeJSONRPC)
	assert.Equal(t, 50.0, score)
	require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
	score, _ = svc.GetScore(ctx, "svc", ep, domain.RPCTypeJSONRPC)
	assert.Equal(t, 51.0, score)
	// Unset fields keep the default: minor is still -3.
	require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, NewMinorErrorSignal("meh", 0)))
	score, _ = svc.GetScore(ctx, "svc", ep, domain.RPCTypeJSONRPC)
	assert.Equal(t, 48.0, score)
}

func TestService_ProbeSignalsCountButDoNotFeedLatency(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	probe := NewSuccessSignal("hc", 900*time.Millisecond)
	probe.Probe = true
	require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, probe))
	views, _ := svc.GetStates(ctx, "svc")
	v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
	assert.Equal(t, uint64(1), v.Attempts)
	assert.Equal(t, uint64(0), v.TrafficAttempts)
	assert.True(t, v.ProbeOnly)
	assert.Equal(t, 0.0, v.LatencyMS, "probe latency is not reported latency")

	require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 100*time.Millisecond)))
	views, _ = svc.GetStates(ctx, "svc")
	v = views[svc.key(ep, domain.RPCTypeJSONRPC)]
	assert.False(t, v.ProbeOnly)
	assert.InDelta(t, 100, v.LatencyMS, 0.01, "first traffic sample seeds the EWMA")
}

// TestService_VouchedUsesEffectiveScore pins Vouched to the effective score.
// The additive term alone would answer 100 for the first key below, which is
// the bug: an endpoint failing a quarter of its relays is not vouched for.
func TestService_VouchedUsesEffectiveScore(t *testing.T) {
	ctx := context.Background()
	// A zero CriticalError impact would be "unset" and fall back to -25, which
	// drains the additive term too; -1 is set, and success at +5 refills it, so
	// only the rate term moves.
	cfg := DefaultServiceConfig()
	cfg.Impacts = SignalImpacts{CriticalError: -1}
	svc := newTestService(t, cfg)
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	for i := 0; i < 40_000; i++ {
		sig := NewSuccessSignal("ok", 0)
		if i%4 == 0 { // 25% critical -> rate term capped at -70
			sig = NewCriticalErrorSignal("bad", 0)
		}
		require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, sig))
	}
	score, _ := svc.GetScore(ctx, "svc", ep, domain.RPCTypeJSONRPC)
	assert.InDelta(t, 30, score, 1, "additive 100, penalty -70")
	assert.Equal(t, score >= svc.selector.cfg.ProbationThreshold,
		svc.Vouched(ctx, "svc", ep, domain.RPCTypeJSONRPC),
		"Vouched must agree with the effective score against the probation threshold")

	// At an additive 100 the effective score lands exactly on the threshold,
	// where reading either term answers the same. Nudge the additive term down
	// and they part company: 70 still clears probation, 70-70 does not.
	for i := 0; i < 10; i++ {
		require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, NewMinorErrorSignal("meh", 0)))
	}
	views, err := svc.GetStates(ctx, "svc")
	require.NoError(t, err)
	v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
	assert.InDelta(t, 70, v.Additive, 0.01)
	assert.InDelta(t, -70, v.Penalty, 0.01, "the rate penalty is at its cap")
	assert.False(t, svc.Vouched(ctx, "svc", ep, domain.RPCTypeJSONRPC),
		"a key failing a quarter of its relays must not be vouched for on its additive score")

	// Second key, default impacts, additive term only: below the threshold and
	// then back above it.
	fresh := newTestService(t, DefaultServiceConfig())
	ep2 := domain.EndpointAddr("pokt1def-https://b.example")
	for i := 0; i < 4; i++ { // 4 x -25 -> additive 0
		require.NoError(t, fresh.RecordSignal(ctx, "svc", ep2, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("bad", 0)))
	}
	score2, _ := fresh.GetScore(ctx, "svc", ep2, domain.RPCTypeJSONRPC)
	assert.Equal(t, 0.0, score2)
	assert.False(t, fresh.Vouched(ctx, "svc", ep2, domain.RPCTypeJSONRPC))
	for i := 0; i < 10; i++ { // 10 x +5 -> additive 50, penalty still ~0
		require.NoError(t, fresh.RecordSignal(ctx, "svc", ep2, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
	}
	score2, _ = fresh.GetScore(ctx, "svc", ep2, domain.RPCTypeJSONRPC)
	assert.Equal(t, 50.0, score2)
	assert.True(t, fresh.Vouched(ctx, "svc", ep2, domain.RPCTypeJSONRPC))
}

func TestService_ResetClearsRate(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	for i := 0; i < 1000; i++ {
		require.NoError(t, svc.RecordSignal(ctx, "svc", ep, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("bad", 0)))
	}
	require.NoError(t, svc.ResetScore(ctx, "svc", ep))
	views, _ := svc.GetStates(ctx, "svc")
	v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
	assert.Equal(t, 0.0, v.Rate)
	assert.Equal(t, 100.0, v.Score)
	assert.Equal(t, uint64(0), v.Attempts)
}

func TestService_SignalHookSeesProbeFlag(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	var got []string
	svc.SetSignalHook(func(sid domain.ServiceID, rpc domain.RPCType, st SignalType, probe bool) {
		got = append(got, fmt.Sprintf("%s/%s/%s/%v", sid, rpc, st, probe))
	})
	ep := domain.EndpointAddr("pokt1abc-https://a.example")
	p := NewMajorErrorSignal("hc", 0)
	p.Probe = true
	_ = svc.RecordSignal(context.Background(), "svc", ep, domain.RPCTypeREST, p)
	_ = svc.RecordSignal(context.Background(), "svc", ep, domain.RPCTypeREST, NewSuccessSignal("ok", 0))
	assert.Equal(t, []string{"svc/rest/major_error/true", "svc/rest/success/false"}, got)
}

// Pruning is decided on the effective score, so the question for a key at a
// full additive score is whether its rate carries a penalty.
//
// A rate above the onset does: that is the chronically-flaky endpoint the rate
// term exists to catch, and evicting it would answer InitialScore with no
// penalty on the next read. A rate below the onset does not — it is latent
// information the cap agrees to forget, and keeping it would retire the bound
// entirely, since the EWMA decays towards zero but never reaches it.
func TestRecordSignal_PruningKeepsPenalisedRate(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{KeyGranularity: KeyPerEndpoint})
	ctx := context.Background()

	// Chronic: enough criticals to push the rate past DefaultOnsetRate, then
	// successes to clamp the additive term back to the ceiling.
	// Interleaved rather than 20 criticals in a row: a floored additive term
	// stops feeding the rate (ruling F2), so a chronic rate is built the way a
	// real one is built — a failure every few attempts, each one recovered
	// before the next.
	chronic := domain.EndpointAddr("pokt1chronic-https://chronic.example.com")
	for range 20 {
		require.NoError(t, svc.RecordSignal(ctx, "eth", chronic, domain.RPCTypeJSONRPC, NewCriticalErrorSignal("bad", 0)))
		for range 5 {
			require.NoError(t, svc.RecordSignal(ctx, "eth", chronic, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
		}
	}
	// Latent: one major error (half weight) leaves a rate far below the onset,
	// and two successes clamp the additive term back to the ceiling.
	latent := domain.EndpointAddr("pokt1latent-https://latent.example.com")
	require.NoError(t, svc.RecordSignal(ctx, "eth", latent, domain.RPCTypeJSONRPC, NewMajorErrorSignal("timeout", 0)))
	for range 3 {
		require.NoError(t, svc.RecordSignal(ctx, "eth", latent, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
	}

	chronicKey := svc.key(chronic, domain.RPCTypeJSONRPC)
	latentKey := svc.key(latent, domain.RPCTypeJSONRPC)
	before, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	require.Equal(t, 100.0, before[chronicKey].Additive, "setup: chronic additive is back at the ceiling")
	require.Equal(t, 100.0, before[latentKey].Additive, "setup: latent additive is back at the ceiling")
	require.Less(t, before[chronicKey].Penalty, 0.0, "setup: the chronic rate carries a penalty")
	require.NotZero(t, before[latentKey].Rate, "setup: the latent rate is non-zero")
	require.Equal(t, 0.0, before[latentKey].Penalty, "setup: the latent rate is below the onset")

	// Flood the same shard set with clean keys to force pruning.
	for i := range 200_000 {
		ep := domain.EndpointAddr(fmt.Sprintf("pokt1ok%06d-https://node.example.com", i))
		_ = svc.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess})
	}

	after, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	assert.Equal(t, before[chronicKey].Rate, after[chronicKey].Rate,
		"pruning dropped a key whose failure rate was still costing it points")
	_, kept := after[latentKey]
	assert.False(t, kept,
		"a sub-onset rate at a full additive score reads back identically: keeping it retires the bound")
}

// One probe answered by one backend is one attempt for the key that backend
// maps to, however many staked registrations front it (docs/scoring.md §3
// principle 4, ruling F1). At per-URL granularity the three siblings below are
// one key, so the additive term moves once.
func TestRecordSignalOnce_PerURLSiblingsAreOneAttempt(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	siblings := domain.EndpointAddrList{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node1.example.com",
	}
	require.NoError(t, svc.RecordSignalOnce(ctx, "eth", siblings, domain.RPCTypeJSONRPC,
		NewCriticalErrorSignal("health_check", 0)))

	views, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	require.Len(t, views, 1, "three registrations in front of one backend are one key")
	v := views[svc.key(siblings[0], domain.RPCTypeJSONRPC)]
	assert.Equal(t, uint64(1), v.Attempts, "one probe, one attempt")
	assert.Equal(t, 75.0, v.Additive, "one critical moved the additive term once, not three times")
}

// At per-endpoint each registration is its own key, so each one gets the
// attempt — the dedupe is on the key, not on the address list.
func TestRecordSignalOnce_PerEndpointScoresEveryRegistration(t *testing.T) {
	cfg := DefaultServiceConfig()
	cfg.KeyGranularity = KeyPerEndpoint
	svc := newTestService(t, cfg)
	ctx := context.Background()
	siblings := domain.EndpointAddrList{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node1.example.com",
	}
	require.NoError(t, svc.RecordSignalOnce(ctx, "eth", siblings, domain.RPCTypeJSONRPC,
		NewCriticalErrorSignal("health_check", 0)))

	views, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	require.Len(t, views, 3, "per-endpoint: one key per registration")
	for _, ep := range siblings {
		v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
		assert.Equal(t, uint64(1), v.Attempts, "%s", ep)
		assert.Equal(t, 75.0, v.Additive, "%s", ep)
	}
}

// A mixed list is deduped per key, not collapsed to the first one.
func TestRecordSignalOnce_MixedBackendsScoreEachKeyOnce(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	eps := domain.EndpointAddrList{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node2.example.com",
	}
	require.NoError(t, svc.RecordSignalOnce(ctx, "eth", eps, domain.RPCTypeJSONRPC,
		NewCriticalErrorSignal("health_check", 0)))

	views, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	require.Len(t, views, 2, "two backends, two keys")
	for _, ep := range eps {
		assert.Equal(t, uint64(1), views[svc.key(ep, domain.RPCTypeJSONRPC)].Attempts, "%s", ep)
	}
}

// The degenerate lists must not panic or record anything unexpected.
func TestRecordSignalOnce_EmptyAndSingle(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	require.NoError(t, svc.RecordSignalOnce(ctx, "eth", nil, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)))
	views, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	assert.Empty(t, views)

	ep := domain.EndpointAddr("supplierA-https://node1.example.com")
	require.NoError(t, svc.RecordSignalOnce(ctx, "eth", domain.EndpointAddrList{ep}, domain.RPCTypeJSONRPC,
		NewCriticalErrorSignal("health_check", 0)))
	views, err = svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, uint64(1), views[svc.key(ep, domain.RPCTypeJSONRPC)].Attempts)
}

// An endpoint the additive term has already floored is in an outage, not
// exhibiting a rate (ruling F2, docs/scoring.md §7.3). A day of probes against
// a dead host — two checks every 30s is 5,760 criticals — would otherwise drive
// the chronic term to its -70 cap, and a capped rate needs roughly seven
// half-lives of clean attempts to clear. A host that comes back must be back in
// tier 1 within a handful of successes, which is what the additive term is for.
func TestRecordSignal_FlooredScoreDoesNotAccrueRate(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1dead-https://dead.example.com")

	// One day of health checks against a host that is simply down.
	for range 5_760 {
		require.NoError(t, svc.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC,
			NewCriticalErrorSignal("health_check: eth_blockNumber", 0)))
	}
	floored, err := svc.GetScore(ctx, "eth", ep, domain.RPCTypeJSONRPC)
	require.NoError(t, err)
	require.Equal(t, 0.0, floored, "setup: the additive term removed it on the fourth critical")

	// The host comes back. 20 successes refill the additive term.
	for range 20 {
		require.NoError(t, svc.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC,
			NewSuccessSignal("health_check: eth_blockNumber", 0)))
	}

	score, err := svc.GetScore(ctx, "eth", ep, domain.RPCTypeJSONRPC)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, score, DefaultSelectorConfig().Tier1Threshold,
		"a recovered host is back in tier 1 on the additive term; without the gate the "+
			"chronic penalty is at its -70 cap and it sits at 30 for weeks")

	views, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
	assert.Equal(t, 0.0, v.Penalty, "an outage left no chronic penalty behind")
	assert.Equal(t, uint64(5_780), v.Attempts, "every signal is still an attempt, and still on the timeline")
}

// The other half of ruling F2: the gate must not touch the case the chronic
// term exists for. A steady 0.2% violator (spacebelt's mainnet rate) never
// floors its additive term, so every one of its attempts feeds the rate and it
// still lands on the §7.3 number.
func TestRecordSignal_ChronicViolatorIsUnaffectedByTheFlooredGate(t *testing.T) {
	svc := newTestService(t, DefaultServiceConfig())
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1chronic-https://chronic.example.com")

	for i := range 100_000 {
		sig := NewSuccessSignal("ok", 0)
		if i%500 == 0 { // 0.2% critical
			sig = NewCriticalErrorSignal("fabricated", 0)
		}
		require.NoError(t, svc.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, sig))
	}

	views, err := svc.GetStates(ctx, "eth")
	require.NoError(t, err)
	v := views[svc.key(ep, domain.RPCTypeJSONRPC)]
	require.Equal(t, 100.0, v.Additive, "a 1-in-500 failure rate never floors the additive term")
	assert.InDelta(t, -23.5, v.Penalty, 3, "docs/scoring.md §7.3: spacebelt at 0.216% is about -23")
	assert.InDelta(t, 76.5, v.Score, 3, "tier 2, as §7.3 says")
}
