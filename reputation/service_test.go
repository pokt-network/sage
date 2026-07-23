package reputation

import (
	"context"
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
	score, err := svc.GetScore(ctx, svcID, ep)
	if err != nil {
		t.Fatal(err)
	}
	if score != 100 {
		t.Errorf("initial score = %f, want 100", score)
	}

	// Record a major error (-10).
	err = svc.RecordSignal(ctx, svcID, ep, NewMajorErrorSignal("timeout", 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	score, _ = svc.GetScore(ctx, svcID, ep)
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
		_ = svc.RecordSignal(ctx, svcID, ep, NewFatalErrorSignal("crash", 0))
	}

	score, _ := svc.GetScore(ctx, svcID, ep)
	if score != 0 {
		t.Errorf("score should be clamped to 0, got %f", score)
	}

	// Reset and drive above max with successes.
	_ = svc.ResetScore(ctx, svcID, ep)
	for range 10 {
		_ = svc.RecordSignal(ctx, svcID, ep, NewSuccessSignal("ok", time.Millisecond))
	}

	score, _ = svc.GetScore(ctx, svcID, ep)
	if score != 100 {
		t.Errorf("score should be clamped to 100, got %f", score)
	}
}

func TestService_AsyncWriteToStorage(t *testing.T) {
	svc, store := newTestService()

	ctx := context.Background()
	svcID := domain.ServiceID("eth")
	ep := domain.EndpointAddr("ep1")

	_ = svc.RecordSignal(ctx, svcID, ep, NewMinorErrorSignal("retry", 0))

	// Stop flushes pending writes.
	svc.Stop()

	key := scoreKey(svcID, ep)
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

	_ = svc.RecordSignal(ctx, svcID, domain.EndpointAddr("ep1"), NewSuccessSignal("ok", 0))
	_ = svc.RecordSignal(ctx, svcID, domain.EndpointAddr("ep2"), NewMajorErrorSignal("fail", 0))

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
	_ = svc.RecordSignal(ctx, svcID, ep1, NewCriticalErrorSignal("fail", 0))

	best := svc.SelectBest(ctx, svcID, domain.EndpointAddrList{ep1, ep2})
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
		pick := svc.SelectBest(ctx, svcID, eps)
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

	best := svc.SelectBest(context.Background(), "eth", nil)
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

	_ = svc.RecordSignal(ctx, svcID, ep, NewFatalErrorSignal("crash", 0))
	score, _ := svc.GetScore(ctx, svcID, ep)
	if score == 100 {
		t.Fatal("score should have decreased")
	}

	_ = svc.ResetScore(ctx, svcID, ep)
	score, _ = svc.GetScore(ctx, svcID, ep)
	if score != 100 {
		t.Errorf("score after reset = %f, want 100", score)
	}
}
