package reputation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func newTestService() (*serviceImpl, *MemoryStorage) {
	store := NewMemoryStorage()
	tl := NewTimeline(100)
	svc := NewService(store, tl, DefaultServiceConfig())
	svc.Start()
	return svc, store
}

func TestService_RecordSignal_UpdatesCache(t *testing.T) {
	svc, _ := newTestService()
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
	svc, _ := newTestService()
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
	svc, store := newTestService()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("ep1")

	_ = svc.RecordSignal(ctx, svcID, ep, domain.RPCTypeJSONRPC, NewMinorErrorSignal("retry", 0))

	// Stop flushes pending writes.
	svc.Stop()

	key := scoreKey(svcID, svc.key(ep, domain.RPCTypeJSONRPC))
	score, err := store.GetScore(ctx, key)
	if err != nil {
		t.Fatalf("expected score in storage after Stop, got error: %v", err)
	}
	if score != 97 { // 100 - 3
		t.Errorf("stored score = %f, want 97", score)
	}
}

func TestService_GetScores(t *testing.T) {
	svc, _ := newTestService()
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
	svc, _ := newTestService()
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
	svc, _ := newTestService()
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
	svc, _ := newTestService()
	defer svc.Stop()

	best := svc.SelectBest(context.Background(), "eth", nil, domain.RPCTypeJSONRPC)
	if best != "" {
		t.Errorf("expected empty for nil endpoints, got %s", best)
	}
}

func TestService_ResetScore(t *testing.T) {
	svc, _ := newTestService()
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
	svc, _ := newTestService()
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
