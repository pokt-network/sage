package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A policy penalty set by hand reaches every key of the party, HTTP and
// WebSocket, and a key it has no state for yet; it follows its flag, lapses
// at its until, and a delete lifts it at the next refresh.
func TestPolicyPenalty_ChargedUntilLapsedOrDeleted(t *testing.T) {
	s := staleShareService(false)
	on := true
	s.SetPolicyPenaltyGate(func(domain.ServiceID) bool { return on })
	ctx := context.Background()
	resold := domain.EndpointAddr("pokt1a-https://r001.resold.example")
	honest := domain.EndpointAddr("pokt1b-https://r001.honest.example")
	for _, ep := range []domain.EndpointAddr{resold, honest} {
		_ = s.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	}
	_ = s.RecordSignal(ctx, "eth", resold, domain.RPCTypeWebSocket, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	score := func(ep domain.EndpointAddr, rpc domain.RPCType) float64 {
		v, _ := s.scoreForSelector(ctx, "eth", ep, rpc)
		return v
	}
	set := func(until time.Time) {
		t.Helper()
		if err := s.SetPolicyPenalty(ctx, PolicyPenalty{Party: resold.Party(), Penalty: -30, Reason: "resells a public RPC", SetAt: time.Now(), Until: until}); err != nil {
			t.Fatal(err)
		}
		s.refreshBaselines()
	}

	set(time.Time{})
	if got := score(resold, domain.RPCTypeJSONRPC); got != 70 {
		t.Errorf("HTTP key: %.0f, want 70", got)
	}
	if got := score(resold, domain.RPCTypeWebSocket); got != 70 {
		t.Errorf("WebSocket key: %.0f, want 70", got)
	}
	if got := score("pokt1a-https://r099.resold.example", domain.RPCTypeJSONRPC); got != 70 {
		t.Errorf("a host of the party with no state yet: %.0f, want 70", got)
	}
	if got := score(honest, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("another party: %.0f, want 100", got)
	}
	if got := s.PartyPolicies(); len(got) != 1 || got[0].Reason != "resells a public RPC" {
		t.Errorf("PartyPolicies = %+v, want the one set", got)
	}

	on = false
	s.refreshBaselines()
	if got := score(resold, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("flag off: %.0f, want 100", got)
	}
	on = true

	set(time.Now().Add(-time.Minute))
	if got := score(resold, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("lapsed: %.0f, want 100", got)
	}
	if got, _ := s.ListPolicyPenalties(ctx); len(got) != 0 {
		t.Errorf("a lapsed penalty is listed: %+v", got)
	}

	set(time.Now().Add(time.Hour))
	if got := score(resold, domain.RPCTypeJSONRPC); got != 70 {
		t.Errorf("set again with an expiry: %.0f, want 70", got)
	}
	if found, err := s.DeletePolicyPenalty(ctx, resold.Party()); err != nil || !found {
		t.Fatalf("delete: found=%v err=%v", found, err)
	}
	s.refreshBaselines()
	if got := score(resold, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("deleted: %.0f, want 100", got)
	}
	if found, _ := s.DeletePolicyPenalty(ctx, resold.Party()); found {
		t.Error("a second delete found one")
	}
}

// The policy penalty is shared through the store: a second instance over the
// same storage charges what the first set.
func TestPolicyPenalty_SharedThroughTheStore(t *testing.T) {
	store := NewMemoryStorage()
	a := NewService(store, nil, ServiceConfig{})
	b := NewService(NewLeaderOnlyStorage(store, func() bool { return false }), nil, ServiceConfig{})
	b.SetPolicyPenaltyGate(func(domain.ServiceID) bool { return true })
	ctx := context.Background()
	ep := domain.EndpointAddr("pokt1a-https://r001.resold.example")
	_ = b.RecordSignal(ctx, "eth", ep, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess, Timestamp: time.Now()})

	if err := a.SetPolicyPenalty(ctx, PolicyPenalty{Party: ep.Party(), Penalty: -30, Reason: "r", SetAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	b.refreshBaselines()
	if v, _ := b.scoreForSelector(ctx, "eth", ep, domain.RPCTypeJSONRPC); v != 70 {
		t.Errorf("follower: %.0f, want 70", v)
	}
	// A follower's admin call writes through as well.
	if found, err := b.DeletePolicyPenalty(ctx, ep.Party()); err != nil || !found {
		t.Fatalf("follower delete: found=%v err=%v", found, err)
	}
	if got, _ := a.ListPolicyPenalties(ctx); len(got) != 0 {
		t.Errorf("still listed after the follower deleted it: %+v", got)
	}
}
