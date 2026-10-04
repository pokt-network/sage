package reputation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestPartyTrust_EvidenceAndHold(t *testing.T) {
	now := time.Now()
	stale := func(party string, services ...domain.ServiceID) []PartyStale {
		var out []PartyStale
		for _, s := range services {
			out = append(out, PartyStale{ServiceID: s, Party: party, Excess: 0.6})
		}
		return out
	}
	all := func(domain.ServiceID) bool { return true }
	penaltyOf := func(ts []PartyTrust, party string) float64 {
		for _, t := range ts {
			if t.Party == party {
				return t.Penalty
			}
		}
		return 0
	}

	// One lagging node on one chain is not distrust; trustStaleServices are.
	lag := partyTrust(stale("lagging", "base"), nil, nil, now, all)
	if penaltyOf(lag, "lagging") != 0 {
		t.Fatal("stale on one service must not distrust a party")
	}
	var many []domain.ServiceID
	for i := 0; i < trustStaleServices; i++ {
		many = append(many, domain.ServiceID(fmt.Sprintf("s%d", i)))
	}
	if penaltyOf(partyTrust(stale("fleet", many[:trustStaleServices-1]...), nil, nil, now, all), "fleet") != 0 {
		t.Fatal("stale on one service fewer than the bar must not distrust a party")
	}
	cache := partyTrust(stale("cache", many...), nil, nil, now, all)
	if penaltyOf(cache, "cache") != trustPenalty {
		t.Fatal("stale on the bar's number of services must")
	}

	// Stale evidence counts only where stale_share prices the service.
	if penaltyOf(partyTrust(stale("cache", many...), nil, nil, now, func(domain.ServiceID) bool { return false }), "cache") != 0 {
		t.Fatal("stale evidence from services stale_share does not price must not count")
	}

	// Refusals: ten or more on each of two services.
	refusals := map[opID]OperatorStat{
		{svc: "sei", op: "refuser"}:   {Attempts: 12},
		{svc: "op", op: "refuser"}:    {Attempts: 15},
		{svc: "gnosis", op: "single"}: {Attempts: 40},
	}
	r := partyTrust(nil, refusals, nil, now, all)
	if penaltyOf(r, "refuser") != trustPenalty || penaltyOf(r, "single") != 0 {
		t.Fatalf("refusals: %+v", r)
	}

	// The penalty holds for the day after the evidence, then lapses.
	if penaltyOf(partyTrust(nil, nil, cache, now.Add(23*time.Hour), all), "cache") != trustPenalty {
		t.Fatal("penalty lapsed inside the hold")
	}
	if penaltyOf(partyTrust(nil, nil, cache, now.Add(25*time.Hour), all), "cache") != 0 {
		t.Fatal("penalty outlived the hold")
	}
}

// A distrusted party is charged on every key it has — a service with no
// evidence of its own, WebSocket, a key with no state yet — as the larger of
// the trust and stale-share penalties, and only where the flag is on.
func TestTrustPenalty_ChargesEveryKeyOnce(t *testing.T) {
	s := staleShareService(true)
	on := true
	s.SetTrustPenalty(func(domain.ServiceID) bool { return on })
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	fresh := domain.EndpointAddr("pokt1b-https://r001.fresh.example")

	// Stale on as many services as the bar, charged -40 by stale_share there.
	for i := 1; i <= trustStaleServices; i++ {
		svc := domain.ServiceID(fmt.Sprintf("s%d", i))
		_ = s.RecordSignal(context.Background(), svc, cache, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess, Timestamp: time.Now()})
		_ = s.RecordSignal(context.Background(), svc, fresh, domain.RPCTypeJSONRPC, Signal{Type: SignalSuccess, Timestamp: time.Now()})
		for i := 0; i < 200; i++ {
			s.RecordHeadAnswer(svc, cache.Party(), i < 160)
			s.RecordHeadAnswer(svc, fresh.Party(), false)
		}
	}
	// A service where nothing measures it, over WebSocket.
	_ = s.RecordSignal(context.Background(), "ws", cache, domain.RPCTypeWebSocket, Signal{Type: SignalSuccess, Timestamp: time.Now()})
	s.refreshBaselines()

	score := func(svc domain.ServiceID, ep domain.EndpointAddr, rpc domain.RPCType) float64 {
		v, _ := s.scoreForSelector(context.Background(), svc, ep, rpc)
		return v
	}
	if got := score("s1", cache, domain.RPCTypeJSONRPC); got != 60 {
		t.Errorf("priced service: %.0f, want 60 (stale -40, not -70)", got)
	}
	if got := score("ws", cache, domain.RPCTypeWebSocket); got != 70 {
		t.Errorf("unmeasured service over WebSocket: %.0f, want 70", got)
	}
	unscored := domain.EndpointAddr("pokt1a-https://r099.cache.example")
	if got := score("other", unscored, domain.RPCTypeJSONRPC); got != 70 {
		t.Errorf("a fresh host with no state yet: %.0f, want 70", got)
	}
	if got := score("ws", fresh, domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("an honest party: %.0f, want 100", got)
	}

	on = false
	s.refreshBaselines()
	if got := score("ws", cache, domain.RPCTypeWebSocket); got != 100 {
		t.Errorf("flag off: %.0f, want 100", got)
	}
}

// A refused_recent signal is counted against the party serving it.
func TestRefusalSignalCountsTowardTrust(t *testing.T) {
	s := staleShareService(false)
	ep := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	for i := 0; i < 12; i++ {
		_ = s.RecordSignal(context.Background(), "sei", ep, domain.RPCTypeJSONRPC, Signal{Type: SignalMajorError, Reason: reasonRefusedRecent, Timestamp: time.Now()})
	}
	if st, ok := s.refusals.get(opID{svc: "sei", op: ep.Party()}, time.Now()); !ok || st.Attempts < 11.9 {
		t.Fatalf("refusals counted %+v %v", st, ok)
	}
}

// A penalty's details reach its timeline event: a refusal's block and depth
// and the node's words are what an operator needs to judge it.
func TestTimelineKeepsTheVerdictDetail(t *testing.T) {
	tl := NewTimeline(100)
	s := NewService(NewMemoryStorage(), tl, ServiceConfig{})
	ep := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	sig := NewSignal(SignalMajorError, reasonRefusedRecent, 0)
	sig.Detail = "claims block 900 is gone, 100 behind the head: no state found for block"
	_ = s.RecordSignal(context.Background(), "op", ep, domain.RPCTypeJSONRPC, sig)
	evs := tl.GetAll("")
	if len(evs) == 0 || !strings.Contains(evs[len(evs)-1].Detail, "100 behind the head: no state found") {
		t.Fatalf("timeline: %+v", evs)
	}
}

// OwnScore leaves the party penalties out; GetScore keeps them.
func TestOwnScoreLeavesThePartyOut(t *testing.T) {
	s := staleShareService(true)
	s.SetTrustPenalty(func(domain.ServiceID) bool { return true })
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	s.chronic.Store(&chronicView{trustPen: map[string]float64{cache.Party(): trustPenalty}, trustGate: func(domain.ServiceID) bool { return true }})
	got, _ := s.GetScore(context.Background(), "eth", cache, domain.RPCTypeWebSocket)
	if got != 70 || s.OwnScore("eth", cache, domain.RPCTypeWebSocket) != 100 {
		t.Fatalf("GetScore %.0f OwnScore %.0f, want 70 and 100", got, s.OwnScore("eth", cache, domain.RPCTypeWebSocket))
	}
}

// With party_penalties_websocket off for a service, its websocket keys carry
// neither the trust nor the stale-share penalty, while the same party's other
// keys there keep both; on (the default, and with no gate) they carry them as
// before.
func TestPartyPenaltiesWebsocket_SparesOnlyWebsocketKeys(t *testing.T) {
	s := staleShareService(true)
	s.SetTrustPenalty(func(domain.ServiceID) bool { return true })
	cache := domain.EndpointAddr("pokt1a-https://r001.cache.example")
	fresh := domain.EndpointAddr("pokt1b-https://r001.fresh.example")
	for i := 1; i <= trustStaleServices; i++ {
		svc := domain.ServiceID(fmt.Sprintf("s%d", i))
		for _, rpc := range []domain.RPCType{domain.RPCTypeJSONRPC, domain.RPCTypeWebSocket} {
			_ = s.RecordSignal(context.Background(), svc, cache, rpc, Signal{Type: SignalSuccess, Timestamp: time.Now()})
			_ = s.RecordSignal(context.Background(), svc, fresh, rpc, Signal{Type: SignalSuccess, Timestamp: time.Now()})
		}
		for i := 0; i < 200; i++ {
			s.RecordHeadAnswer(svc, cache.Party(), i < 160)
			s.RecordHeadAnswer(svc, fresh.Party(), false)
		}
	}
	score := func(svc domain.ServiceID, rpc domain.RPCType) float64 {
		v, _ := s.scoreForSelector(context.Background(), svc, cache, rpc)
		return v
	}

	s.refreshBaselines() // no gate set: charged everywhere, as before
	if http, ws := score("s1", domain.RPCTypeJSONRPC), score("s1", domain.RPCTypeWebSocket); http != 60 || ws != 60 {
		t.Fatalf("no gate: http %.0f ws %.0f, want 60 and 60", http, ws)
	}

	off := map[domain.ServiceID]bool{"s1": true}
	s.SetPartyPenaltiesWebsocket(func(svc domain.ServiceID) bool { return !off[svc] })
	s.refreshBaselines()
	if http, ws := score("s1", domain.RPCTypeJSONRPC), score("s1", domain.RPCTypeWebSocket); http != 60 || ws != 100 {
		t.Errorf("off on s1: http %.0f ws %.0f, want 60 and 100", http, ws)
	}
	if ws := score("s2", domain.RPCTypeWebSocket); ws != 60 {
		t.Errorf("on for s2: ws %.0f, want 60", ws)
	}
	if st, tr := s.chronic.Load().partyPenalties("s1", s.keyOf(cache, domain.RPCTypeWebSocket)); st != 0 || tr != 0 {
		t.Errorf("admin view penalties %.0f/%.0f for the spared websocket key, want 0/0", st, tr)
	}
}

// With seed_from_operator on, a key with no history starts at the median
// score of its operator's keys on the service and RPC type, or of its party's
// (an owner over every domain dedicated to it), whichever is lower: a new host
// of an operator with a record does not start clean, nor does a new domain of
// an owner with one. An operator with no keys there starts at 100, and with
// the flag off everyone does.
func TestSeedFromOperator(t *testing.T) {
	// The owner's two domains: dedicated once enough of their suppliers are
	// seen staked by it.
	for i := range 10 {
		domain.RecordOwner(fmt.Sprintf("pokt1seedown%d", i), "pokt1owner", "own-a.example")
		domain.RecordOwner(fmt.Sprintf("pokt1seedother%d", i), "pokt1owner", "own-b.example")
	}
	s := NewService(NewMemoryStorage(), nil, ServiceConfig{StateIdleTTL: -1})
	on := true
	s.SetSeedFromOperator(func(domain.ServiceID) bool { return on })
	ctx := context.Background()
	charge := func(ep domain.EndpointAddr, majors int) {
		_ = s.RecordSignal(ctx, "eth", ep, domain.RPCTypeWebSocket, Signal{Type: SignalSuccess, Timestamp: time.Now()})
		for range majors {
			_ = s.RecordSignal(ctx, "eth", ep, domain.RPCTypeWebSocket, Signal{Type: SignalMajorError, Timestamp: time.Now()})
		}
	}
	// A lagging operator: three hosts at 80, 70, 60.
	charge("pokt1a-wss://r1.lag.example", 2)
	charge("pokt1b-wss://r2.lag.example", 3)
	charge("pokt1c-wss://r3.lag.example", 4)
	// The owner's first domain at 50.
	charge("pokt1seedown0-wss://r1.own-a.example", 5)
	s.refreshBaselines()

	score := func(ep domain.EndpointAddr) float64 {
		v, _ := s.scoreForSelector(ctx, "eth", ep, domain.RPCTypeWebSocket)
		return v
	}
	if got := score("pokt1d-wss://r9.lag.example"); got != 70 {
		t.Errorf("a new host of the lagging operator: %.0f, want its median 70", got)
	}
	if got := score("pokt1seedother0-wss://r1.own-b.example"); got != 50 {
		t.Errorf("a new domain of the owner: %.0f, want the owner's 50", got)
	}
	if got := score("pokt1e-wss://r1.clean.example"); got != 100 {
		t.Errorf("an operator with no keys: %.0f, want 100", got)
	}
	if got, _ := s.scoreForSelector(ctx, "eth", "pokt1d-wss://r9.lag.example", domain.RPCTypeJSONRPC); got != 100 {
		t.Errorf("another RPC type: %.0f, want 100", got)
	}
	// The first signal starts from the seed too, not from 100.
	charge("pokt1d-wss://r9.lag.example", 0)
	if got := score("pokt1d-wss://r9.lag.example"); got != 75 {
		t.Errorf("after one success: %.0f, want 70 + 5", got)
	}

	on = false
	s.refreshBaselines()
	if got := score("pokt1f-wss://r8.lag.example"); got != 100 {
		t.Errorf("flag off: %.0f, want 100", got)
	}
}
