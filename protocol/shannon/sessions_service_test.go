package shannon

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// One supplier staking one URL for two services yields the same endpoint
// address in both sessions. A lookup for one service must return the endpoint
// of that service's session, whichever session was extracted last: the
// WebSocket probe signs with it, and another service's session is refused by
// the relay miner.
func TestLookupEndpoint_SameAddressAcrossServices(t *testing.T) {
	sm := newSessionManager(nil, map[domain.ServiceID]struct{}{"eth": {}, "bsc": {}}, newTestLogger())
	eth := buildRelayTestSession("pokt1shared", "https://one.example.com")
	bsc := buildRelayTestSession("pokt1shared", "https://one.example.com")
	bsc.SessionId, bsc.Header.SessionId, bsc.Header.ServiceId = "bsc-session", "bsc-session", "bsc"
	bsc.Suppliers[0].Services[0].ServiceId = "bsc"

	sm.getOrCreateEndpoints(eth)
	sm.getOrCreateEndpoints(bsc) // last write: bsc

	var addr domain.EndpointAddr
	for a := range sm.getOrCreateEndpoints(eth) {
		addr = a
	}
	for _, svc := range []domain.ServiceID{"eth", "bsc"} {
		ep, ok := sm.lookupEndpoint(svc, addr)
		if !ok {
			t.Fatalf("%s: no endpoint for %s", svc, addr)
		}
		if got := ep.Session().Header.ServiceId; got != string(svc) {
			t.Fatalf("lookup for %s returned the %s session's endpoint", svc, got)
		}
	}
	if _, ok := sm.lookupAnyEndpoint(addr); !ok {
		t.Fatal("any-service lookup lost the address")
	}
}
