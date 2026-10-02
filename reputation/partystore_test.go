package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func warmService(store Storage) *serviceImpl {
	s := NewService(store, nil, ServiceConfig{StateIdleTTL: -1})
	s.SetStaleShare(func(domain.ServiceID) bool { return true })
	s.SetTrustPenalty(func(domain.ServiceID) bool { return true })
	return s
}

// A fresh pod adopts the leader's priced parties at boot and charges them from
// its first selection, before it has any evidence of its own.
func TestWarmStart_AdoptsStoredPenalties(t *testing.T) {
	store := NewMemoryStorage()
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	now := time.Now()
	_ = store.SetPartyPenalties(context.Background(), PartyPenalties{
		Stale: []StoredStale{{Service: "base", Party: cache.Party(), Penalty: -40, PricedAt: now.Add(-time.Minute).Unix()}},
		Trust: []StoredTrust{{Party: cache.Party(), Until: now.Add(time.Hour).Unix()}},
	})

	fresh := warmService(store)
	if _, err := fresh.Hydrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	score := func(svc domain.ServiceID) float64 {
		v, _ := fresh.scoreForSelector(context.Background(), svc, cache, domain.RPCTypeJSONRPC)
		return v
	}
	if got := score("base"); got != 60 {
		t.Errorf("stored stale penalty on base: %.0f, want 60", got)
	}
	if got := score("eth"); got != 70 {
		t.Errorf("stored trust penalty elsewhere: %.0f, want 70", got)
	}
}

// Stored entries past their holds are not applied: a stale-share price older
// than staleShareHold, a trust penalty whose end has passed.
func TestWarmStart_IgnoresExpiredEntries(t *testing.T) {
	store := NewMemoryStorage()
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	now := time.Now()
	_ = store.SetPartyPenalties(context.Background(), PartyPenalties{
		Stale: []StoredStale{{Service: "base", Party: cache.Party(), Penalty: -40, PricedAt: now.Add(-staleShareHold - time.Minute).Unix()}},
		Trust: []StoredTrust{{Party: cache.Party(), Until: now.Add(-time.Minute).Unix()}},
	})
	fresh := warmService(store)
	_, _ = fresh.Hydrate(context.Background())
	if v, _ := fresh.scoreForSelector(context.Background(), "base", cache, domain.RPCTypeJSONRPC); v != 100 {
		t.Fatalf("expired entries applied: %.0f, want 100", v)
	}
}

// An adopted trust penalty ends when the leader said, never later: without
// evidence of its own, the pod only carries the stored end.
func TestWarmStart_TrustNeverOutlivesTheStoredEnd(t *testing.T) {
	store := NewMemoryStorage()
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	until := time.Now().Add(time.Hour).Truncate(time.Second)
	_ = store.SetPartyPenalties(context.Background(), PartyPenalties{Trust: []StoredTrust{{Party: cache.Party(), Until: until.Unix()}}})
	fresh := warmService(store)
	_, _ = fresh.Hydrate(context.Background())
	found := false
	for _, tr := range fresh.PartyTrusts() {
		if tr.Party == cache.Party() {
			found = true
			if !tr.until.Equal(until) {
				t.Fatalf("adopted until %v, want the stored %v", tr.until, until)
			}
		}
	}
	if !found {
		t.Fatal("stored trust penalty not adopted")
	}
}

// The pod's own evidence takes over: a party it measures fresh is priced
// from that, not from the stored penalty.
func TestWarmStart_OwnEvidenceTakesOver(t *testing.T) {
	store := NewMemoryStorage()
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	other := domain.EndpointAddr("pokt1b-https://r001.other.example")
	_ = store.SetPartyPenalties(context.Background(), PartyPenalties{
		Stale: []StoredStale{{Service: "base", Party: cache.Party(), Penalty: -40, PricedAt: time.Now().Unix()}},
	})
	fresh := warmService(store)
	_, _ = fresh.Hydrate(context.Background())
	for i := 0; i < 200; i++ {
		fresh.RecordHeadAnswer("base", cache.Party(), false)
		fresh.RecordHeadAnswer("base", other.Party(), false)
	}
	fresh.refreshBaselines()
	if v, _ := fresh.scoreForSelector(context.Background(), "base", cache, domain.RPCTypeJSONRPC); v != 100 {
		t.Fatalf("own fresh evidence did not take over: %.0f, want 100", v)
	}
}

// The leader writes what it prices on every refresh; a follower does not.
func TestWarmStart_LeaderWritesFollowerDoesNot(t *testing.T) {
	mem := NewMemoryStorage()
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	other := domain.EndpointAddr("pokt1b-https://r001.other.example")
	feed := func(s *serviceImpl) {
		for i := 0; i < 200; i++ {
			s.RecordHeadAnswer("base", cache.Party(), i < 160)
			s.RecordHeadAnswer("base", other.Party(), false)
		}
		s.refreshBaselines()
	}
	feed(warmService(NewLeaderOnlyStorage(mem, func() bool { return false })))
	if p, _ := mem.GetPartyPenalties(context.Background()); len(p.Stale) != 0 {
		t.Fatalf("follower wrote %+v", p)
	}
	feed(warmService(NewLeaderOnlyStorage(mem, func() bool { return true })))
	if p, _ := mem.GetPartyPenalties(context.Background()); len(p.Stale) != 1 || p.Stale[0].Party != cache.Party() {
		t.Fatalf("leader wrote %+v, want the cache priced", p)
	}
}
