package reputation

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// dupShareService is a service with ws_duplicate_share on or off and the
// party penalties on websocket keys off, as on mainnet: the repeat share is
// measured on WebSocket and must charge regardless.
func dupShareService(on bool) *serviceImpl {
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{})
	s.SetDuplicateShare(func(domain.ServiceID) bool { return on }, func(domain.ServiceID) (float64, float64) {
		return DefaultDuplicateShareFloor, DefaultDuplicateShareFull
	})
	s.SetPartyPenaltiesWebsocket(func(domain.ServiceID) bool { return false })
	return s
}

// feedNotifications gives ep a scored websocket key and a JSON-RPC key, and
// reports notes notifications for its party, dups of them repeats.
func feedNotifications(s *serviceImpl, ep domain.EndpointAddr, notes, dups int) {
	for _, rpc := range []domain.RPCType{domain.RPCTypeWebSocket, domain.RPCTypeJSONRPC} {
		_ = s.RecordSignal(context.Background(), rateSvc, ep, rpc, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	}
	s.RecordNotifications(rateSvc, ep.Party(), notes, dups)
}

func scoreOf(t *testing.T, s *serviceImpl, ep domain.EndpointAddr, rpc domain.RPCType) float64 {
	t.Helper()
	v, _ := s.GetScore(context.Background(), rateSvc, ep, rpc)
	return v
}

// A party repeating a quarter of its notifications against a clean one is
// charged the full -40 on every websocket key it has, a key of a node it
// never repeated on included, and on no other key. Off, nothing is charged
// and the share is still measured.
func TestDuplicateShareChargesTheRepeatingParty(t *testing.T) {
	repeater := domain.EndpointAddr("pokt1a-https://r001.repeat.example")
	sibling := domain.EndpointAddr("pokt1a-https://r002.repeat.example")
	clean := domain.EndpointAddr("pokt1b-https://r001.clean.example")

	s := dupShareService(true)
	feedNotifications(s, repeater, 6000, 1500) // 25%
	feedNotifications(s, sibling, 10, 0)
	feedNotifications(s, clean, 6000, 0)
	s.refreshBaselines()

	if got := scoreOf(t, s, sibling, domain.RPCTypeWebSocket); got > 61 {
		t.Fatalf("repeating party's websocket key scored %.1f, want ~60 (-40)", got)
	}
	if got := scoreOf(t, s, repeater, domain.RPCTypeJSONRPC); got != 100 {
		t.Fatalf("repeating party's JSON-RPC key scored %.1f, want 100: the share is a WebSocket measure", got)
	}
	if got := scoreOf(t, s, clean, domain.RPCTypeWebSocket); got != 100 {
		t.Fatalf("clean party scored %.1f, want 100", got)
	}

	off := dupShareService(false)
	feedNotifications(off, repeater, 6000, 1500)
	feedNotifications(off, clean, 6000, 0)
	off.refreshBaselines()
	if got := scoreOf(t, off, repeater, domain.RPCTypeWebSocket); got != 100 {
		t.Fatalf("flag off: repeating party scored %.1f, want 100", got)
	}
	if len(off.PartyDuplicateShares()) != 2 {
		t.Fatal("flag off: shares must still be measured for the metric")
	}
}

// Relative and linear: a chain whose parties all repeat alike charges no one,
// one measured party charges nothing, too little evidence is not measured,
// and between floor and full the penalty is proportional.
func TestDuplicateShareIsRelative(t *testing.T) {
	a := domain.EndpointAddr("pokt1a-https://r001.a.example")
	b := domain.EndpointAddr("pokt1b-https://r001.b.example")
	for name, tc := range map[string]struct {
		aNotes, aDups, bNotes, bDups int
		wantA                        float64
	}{
		"both repeat alike":  {6000, 1200, 6000, 1150, 0},
		"one party measured": {6000, 1500, 100, 0, 0},
		"11 points over":     {6000, 660, 6000, 0, -20},
		"under the 2% floor": {6000, 90, 6000, 0, 0},
	} {
		s := dupShareService(true)
		feedNotifications(s, a, tc.aNotes, tc.aDups)
		feedNotifications(s, b, tc.bNotes, tc.bDups)
		s.refreshBaselines()
		got := 0.0
		for _, p := range s.PartyDuplicateShares() {
			if p.Party == a.Party() {
				got = p.Penalty
			}
		}
		if got < tc.wantA-0.5 || got > tc.wantA+0.5 {
			t.Errorf("%s: penalty %.1f, want %.1f", name, got, tc.wantA)
		}
	}
}

// Every pod prices a party on the fleet's counts: a pod holding none of the
// repeater's connections charges it from the pods that do. Another pod's
// counts past dupShareFleetMaxAge (a pod gone) are dropped and deleted.
func TestDuplicateShareIsPricedOnTheFleet(t *testing.T) {
	repeater := domain.EndpointAddr("pokt1a-https://r001.repeat.example")
	clean := domain.EndpointAddr("pokt1b-https://r001.clean.example")
	shared := NewMemoryStorage()
	pod := func(id string) *serviceImpl {
		s := NewService(shared, nil, ServiceConfig{})
		s.SetDuplicateShare(func(domain.ServiceID) bool { return true }, func(domain.ServiceID) (float64, float64) {
			return DefaultDuplicateShareFloor, DefaultDuplicateShareFull
		})
		s.SetInstanceID(id)
		return s
	}
	holder, other := pod("pod-a"), pod("pod-b")
	feedNotifications(holder, repeater, 6000, 1500) // 25%, all on pod-a
	feedNotifications(holder, clean, 6000, 0)
	_ = other.RecordSignal(context.Background(), rateSvc, repeater, domain.RPCTypeWebSocket, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	holder.refreshBaselines() // writes pod-a's counts
	other.refreshBaselines()

	if got := scoreOf(t, other, repeater, domain.RPCTypeWebSocket); got > 61 {
		t.Fatalf("pod with no repeater connections scored it %.1f, want ~60 from the fleet's counts", got)
	}

	_ = shared.PutNotificationCounts(context.Background(), "pod-gone", NotificationCounts{
		At:     time.Now().Add(-dupShareFleetMaxAge - time.Minute).Unix(),
		Counts: []PartyNotifications{{Service: rateSvc, Party: clean.Party(), Notes: 100000, Dups: 90000}},
	})
	other.refreshBaselines()
	all, _ := shared.NotificationCounts(context.Background())
	if _, ok := all["pod-gone"]; ok {
		t.Fatal("a pod's counts past the max age were not deleted")
	}
	if got := scoreOf(t, other, clean, domain.RPCTypeWebSocket); got != 100 {
		t.Fatalf("a gone pod's counts charged the clean party: %.1f, want 100", got)
	}
}

// The leader-only wrapper production uses passes every pod's notification
// counts through: a follower's are the ones the fleet most needs.
func TestLeaderOnlyStoragePassesNotificationCounts(t *testing.T) {
	inner := NewMemoryStorage()
	follower := NewLeaderOnlyStorage(inner, func() bool { return false })
	require := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	require(follower.PutNotificationCounts(context.Background(), "pod-b", NotificationCounts{At: 1}))
	all, err := follower.NotificationCounts(context.Background())
	require(err)
	if _, ok := all["pod-b"]; !ok {
		t.Fatalf("a follower's counts were not written: %v", all)
	}
	require(follower.DeleteNotificationCounts(context.Background(), "pod-b"))
	if all, _ := inner.NotificationCounts(context.Background()); len(all) != 0 {
		t.Fatalf("delete did not pass through: %v", all)
	}
}
