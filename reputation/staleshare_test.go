package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func staleShareService(on bool) *serviceImpl {
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	s.SetStaleShare(func(domain.ServiceID) bool { return on })
	return s
}

// feedHeads records n head answers for a party, stale of them stale, and one
// success on ep so the party has a scored key.
func feedHeads(s *serviceImpl, ep domain.EndpointAddr, n, stale int) {
	_ = s.RecordSignal(context.Background(), rateSvc, ep, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	for i := 0; i < n; i++ {
		s.RecordHeadAnswer(rateSvc, ep.Party(), i < stale)
	}
}

func score(t *testing.T, s *serviceImpl, ep domain.EndpointAddr) float64 {
	t.Helper()
	v, _ := s.GetScore(context.Background(), rateSvc, ep, domain.RPCTypeJSONRPC)
	return v
}

func TestStaleShareChargesTheStaleParty(t *testing.T) {
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	fresh := domain.EndpointAddr("pokt1b-https://r001.fresh.example")

	s := staleShareService(true)
	feedHeads(s, cache, 200, 160) // 80% stale
	feedHeads(s, fresh, 200, 4)   // 2%, a propagation tail
	before := score(t, s, cache)
	s.refreshBaselines()

	if got := score(t, s, cache); got > before-39 {
		t.Fatalf("stale party scored %.1f, want ~%.1f (-40)", got, before-40)
	}
	if got := score(t, s, fresh); got != 100 {
		t.Fatalf("fresh party scored %.1f, want 100", got)
	}

	off := staleShareService(false)
	feedHeads(off, cache, 200, 160)
	feedHeads(off, fresh, 200, 4)
	off.refreshBaselines()
	if got := score(t, off, cache); got != 100 {
		t.Fatalf("flag off: stale party scored %.1f, want 100", got)
	}
	if len(off.PartyStaleShares()) != 2 {
		t.Fatal("flag off: shares must still be measured for the metric")
	}
}

// A chain whose every party reads stale is the chain's lag, not a party's; and
// one measured party cannot be told from the chain at all.
func TestStaleShareIsRelativeAndNeedsTwoParties(t *testing.T) {
	a := domain.EndpointAddr("pokt1a-https://r001.a.example")
	b := domain.EndpointAddr("pokt1b-https://r001.b.example")

	s := staleShareService(true)
	feedHeads(s, a, 200, 50) // 25%
	feedHeads(s, b, 200, 40) // 20%
	s.refreshBaselines()
	if got := score(t, s, a); got != 100 {
		t.Fatalf("shared lag charged: %.1f", got)
	}

	alone := staleShareService(true)
	feedHeads(alone, a, 200, 160)
	feedHeads(alone, b, 10, 0) // too little evidence to measure
	alone.refreshBaselines()
	if got := score(t, alone, a); got != 100 {
		t.Fatalf("single measured party charged: %.1f", got)
	}
}

func TestStalePenaltyCurve(t *testing.T) {
	for _, c := range []struct{ excess, want float64 }{
		{0.10, 0}, {0.15, 0}, {0.30, -20}, {0.45, -40}, {0.80, -40},
	} {
		if got := stalePenalty(c.excess); got < c.want-0.01 || got > c.want+0.01 {
			t.Errorf("stalePenalty(%.2f) = %.2f, want %.2f", c.excess, got, c.want)
		}
	}
}

// A priced party whose evidence falls under the minimum keeps its penalty for
// the hold, and loses it once the hold has passed.
func TestStaleShareHoldsAPenaltyPastThinEvidence(t *testing.T) {
	now := time.Now()
	on := func(domain.ServiceID) bool { return true }
	prev := partyStale(map[opID]OperatorStat{
		{svc: rateSvc, op: "cache"}: {Attempts: 200, Failures: 160},
		{svc: rateSvc, op: "fresh"}: {Attempts: 200, Failures: 0},
	}, on, nil, now)
	thin := map[opID]OperatorStat{
		{svc: rateSvc, op: "cache"}: {Attempts: 10, Failures: 8},
		{svc: rateSvc, op: "fresh"}: {Attempts: 200, Failures: 0},
	}
	penaltyOf := func(ps []PartyStale) float64 {
		for _, p := range ps {
			if p.Party == "cache" {
				return p.Penalty
			}
		}
		return 0
	}
	if got := penaltyOf(partyStale(thin, on, prev, now.Add(10*time.Minute))); got != -40 {
		t.Fatalf("inside the hold: %.1f, want -40", got)
	}
	if got := penaltyOf(partyStale(thin, on, prev, now.Add(staleShareHold+time.Minute))); got != 0 {
		t.Fatalf("after the hold: %.1f, want 0", got)
	}
	off := func(domain.ServiceID) bool { return false }
	if got := penaltyOf(partyStale(thin, off, prev, now.Add(time.Minute))); got != 0 {
		t.Fatalf("flag off: %.1f, want 0", got)
	}
}
