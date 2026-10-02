package reputation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func allServices(domain.ServiceID) bool { return true }

// An instance with no evidence of its own charges the peer's priced parties:
// the stale-share penalty on the service the peer priced, trust everywhere.
func TestPeerParties_AppliedWhenLocalViewEmpty(t *testing.T) {
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	now := time.Now()
	s := warmService(NewMemoryStorage())
	s.SetPeerParties(func(context.Context) (PartyPenalties, error) {
		return PartyPenalties{
			Stale: []StoredStale{{Service: "base", Party: cache.Party(), Penalty: -40, PricedAt: now.Add(-time.Minute).Unix()}},
			Trust: []StoredTrust{{Party: cache.Party(), Until: now.Add(time.Hour).Unix()}},
		}, nil
	})
	ctx := context.Background()
	// A key on each service, so both are served here.
	for _, svc := range []domain.ServiceID{"base", "eth"} {
		if err := s.RecordSignal(ctx, svc, cache, domain.RPCTypeJSONRPC, NewSignal(SignalSuccess, "ok", 0)); err != nil {
			t.Fatal(err)
		}
	}
	s.refreshBaselines()

	score := func(svc domain.ServiceID) float64 {
		v, _ := s.scoreForSelector(ctx, svc, cache, domain.RPCTypeJSONRPC)
		return v
	}
	if got := score("base"); got != 60 {
		t.Errorf("peer stale penalty on base: %.0f, want 60", got)
	}
	if got := score("eth"); got != 70 {
		t.Errorf("peer trust penalty on eth: %.0f, want 70", got)
	}
	for _, p := range s.PartyStaleShares() {
		if p.Party == cache.Party() && !p.Peer() {
			t.Errorf("borrowed stale penalty not marked peer: %+v", p)
		}
	}
}

// The floor never softens a party: a harsher local penalty stands.
func TestPeerParties_LocalHarsherWins(t *testing.T) {
	now := time.Now()
	local := []PartyStale{{ServiceID: "base", Party: "cache.example", Penalty: -40, pricedAt: now}}
	peer := PartyPenalties{Stale: []StoredStale{{Service: "base", Party: "cache.example", Penalty: -10, PricedAt: now.Unix()}}}
	stale, _ := mergePeerParties(local, nil, peer, allServices, allServices, now)
	if len(stale) != 1 || stale[0].Penalty != -40 || stale[0].peer {
		t.Fatalf("got %+v, want the local -40 untouched", stale)
	}
}

// Expired peer entries lend nothing: a stale price past staleShareHold, a
// trust penalty past its end. A service not served here is skipped, and so
// is one whose stale_share flag is off here.
func TestPeerParties_SkipsExpiredUnservedAndFlaggedOff(t *testing.T) {
	now := time.Now()
	peer := PartyPenalties{
		Stale: []StoredStale{
			{Service: "base", Party: "a.example", Penalty: -40, PricedAt: now.Add(-staleShareHold - time.Minute).Unix()},
			{Service: "unserved", Party: "b.example", Penalty: -40, PricedAt: now.Unix()},
			{Service: "off", Party: "c.example", Penalty: -40, PricedAt: now.Unix()},
		},
		Trust: []StoredTrust{{Party: "a.example", Until: now.Add(-time.Minute).Unix()}},
	}
	served := func(svc domain.ServiceID) bool { return svc != "unserved" }
	staleOn := func(svc domain.ServiceID) bool { return svc != "off" }
	stale, trust := mergePeerParties(nil, nil, peer, served, staleOn, now)
	if len(stale)+len(trust) != 0 {
		t.Fatalf("borrowed %+v / %+v, want nothing", stale, trust)
	}
}

// A borrowed stale penalty is held, so this instance's trust tally never
// counts it as its own evidence, and the next refresh carries it on the
// peer's clock rather than restarting the hold.
func TestPeerParties_BorrowedStaleIsNotTrustEvidence(t *testing.T) {
	now := time.Now()
	pricedAt := now.Add(-10 * time.Minute)
	peer := PartyPenalties{Stale: []StoredStale{{Service: "base", Party: "cache.example", Penalty: -40, PricedAt: pricedAt.Unix()}}}
	stale, _ := mergePeerParties(nil, nil, peer, allServices, allServices, now)
	if len(stale) != 1 || !stale[0].held || !stale[0].pricedAt.Equal(time.Unix(pricedAt.Unix(), 0)) {
		t.Fatalf("got %+v, want one held entry on the peer's clock", stale)
	}
	if trust := partyTrust(stale, nil, nil, now, allServices); len(trust) != 0 {
		t.Fatalf("borrowed stale counted as trust evidence: %+v", trust)
	}
}

// With no peer reader, or one that fails, nothing is borrowed.
func TestPeerParties_OffOrUnreadableReadsNothing(t *testing.T) {
	s := warmService(NewMemoryStorage())
	if p := s.readPeerParties(); len(p.Stale)+len(p.Trust) != 0 {
		t.Fatalf("unset reader lent %+v", p)
	}
	s.SetPeerParties(func(context.Context) (PartyPenalties, error) {
		return PartyPenalties{Trust: []StoredTrust{{Party: "x", Until: time.Now().Add(time.Hour).Unix()}}}, errors.New("down")
	})
	if p := s.readPeerParties(); len(p.Stale)+len(p.Trust) != 0 {
		t.Fatalf("failed reader lent %+v", p)
	}
}

// With trust_penalty off here, a borrowed trust penalty is held in the view
// but charged nowhere: the per-service trust gate decides, as for local ones.
func TestPeerParties_TrustFlagOffChargesNothing(t *testing.T) {
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{StateIdleTTL: -1})
	s.SetTrustPenalty(func(domain.ServiceID) bool { return false })
	s.SetPeerParties(func(context.Context) (PartyPenalties, error) {
		return PartyPenalties{Trust: []StoredTrust{{Party: cache.Party(), Until: time.Now().Add(time.Hour).Unix()}}}, nil
	})
	ctx := context.Background()
	_ = s.RecordSignal(ctx, "eth", cache, domain.RPCTypeJSONRPC, NewSignal(SignalSuccess, "ok", 0))
	s.refreshBaselines()
	if v, _ := s.scoreForSelector(ctx, "eth", cache, domain.RPCTypeJSONRPC); v != 100 {
		t.Fatalf("trust flag off, score %.0f, want 100", v)
	}
}
