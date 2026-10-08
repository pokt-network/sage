package shannon

import (
	"testing"

	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"

	"github.com/pokt-network/sage/domain"
)

// One supplier, one JSON-RPC URL, two services, a different REST host in each:
// the endpoint address is the same in both sessions. A REST relay for one
// service dials that service's REST host; when reputation's key names the
// other service's, the mismatch is counted.
func TestCheckKeyURL_CountsAKeyNamingAnotherHost(t *testing.T) {
	sm := newSessionManager(nil, map[domain.ServiceID]struct{}{"eth": {}, "bsc": {}}, newTestLogger())
	eth := buildMultiServiceSession("eth", "pokt1shared", map[sharedtypes.RPCType]string{
		sharedtypes.RPCType_JSON_RPC: "https://rm.example.com",
		sharedtypes.RPCType_REST:     "https://rest-eth.example.com",
	})
	bsc := buildMultiServiceSession("bsc", "pokt1shared", map[sharedtypes.RPCType]string{
		sharedtypes.RPCType_JSON_RPC: "https://rm.example.com",
		sharedtypes.RPCType_REST:     "https://rest-bsc.example.com",
	})
	sm.getOrCreateEndpoints(eth)
	sm.getOrCreateEndpoints(bsc)

	m := &recordingMetrics{}
	p := &Protocol{sessions: sm, metrics: m}
	addr := domain.EndpointAddr("pokt1shared-https://rm.example.com")
	keyURL, ok := p.ReputationURLFor(addr, domain.RPCTypeREST)
	if !ok {
		t.Fatal("precondition: the address resolves a REST URL")
	}

	for _, tc := range []struct {
		service domain.ServiceID
		dialed  string
	}{
		{"eth", "https://rest-eth.example.com"},
		{"bsc", "https://rest-bsc.example.com"},
	} {
		p.checkKeyURL(tc.service, addr, domain.RPCTypeREST, tc.dialed)
	}
	if len(m.mismatches) != 1 {
		t.Fatalf("mismatches = %v with the key naming %s; want exactly the service whose REST host differs", m.mismatches, keyURL)
	}

	// A JSON-RPC relay dials the URL its key names: nothing counted.
	p.checkKeyURL("eth", addr, domain.RPCTypeJSONRPC, "https://rm.example.com")
	if len(m.mismatches) != 1 {
		t.Fatalf("a matching key was counted: %v", m.mismatches)
	}
}

// One supplier, one JSON-RPC URL, two services, a WebSocket face in only one:
// the service without it, extracted last, must not hide the face, or the
// WebSocket key falls back to the JSON-RPC host and the face is scored under
// two keys.
func TestReputationURLFor_FaceStakedInOneServiceOnly(t *testing.T) {
	sm := newSessionManager(nil, map[domain.ServiceID]struct{}{"eth": {}, "bsc": {}}, newTestLogger())
	sm.getOrCreateEndpoints(buildMultiServiceSession("eth", "pokt1shared", map[sharedtypes.RPCType]string{
		sharedtypes.RPCType_JSON_RPC:  "https://rm.example.com",
		sharedtypes.RPCType_WEBSOCKET: "wss://ws.example.com",
	}))
	sm.getOrCreateEndpoints(buildMultiServiceSession("bsc", "pokt1shared", map[sharedtypes.RPCType]string{
		sharedtypes.RPCType_JSON_RPC: "https://rm.example.com",
	}))

	p := &Protocol{sessions: sm, metrics: noopSupplierMetrics{}}
	addr := domain.EndpointAddr("pokt1shared-https://rm.example.com")
	if got, ok := p.ReputationURLFor(addr, domain.RPCTypeWebSocket); !ok || got != "wss://ws.example.com" {
		t.Errorf("WebSocket face = %q, %v; want wss://ws.example.com", got, ok)
	}
	if got, ok := p.ReputationURLFor(addr, domain.RPCTypeJSONRPC); !ok || got != "https://rm.example.com" {
		t.Errorf("JSON-RPC face = %q, %v; want https://rm.example.com", got, ok)
	}
}
