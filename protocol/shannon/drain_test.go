package shannon

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
)

// buildDrainTestSession builds a session with one supplier per URL, each
// serving both json_rpc and websocket at the given URL. The map key is the
// supplier address, the value its URL.
func buildDrainTestSession(supplierURLs map[string]string) *sessiontypes.Session {
	suppliers := make([]*sharedtypes.Supplier, 0, len(supplierURLs))
	for supplierAddr, url := range supplierURLs {
		suppliers = append(suppliers, &sharedtypes.Supplier{
			OperatorAddress: supplierAddr,
			Services: []*sharedtypes.SupplierServiceConfig{
				{
					ServiceId: "eth",
					Endpoints: []*sharedtypes.SupplierEndpoint{
						{Url: url, RpcType: sharedtypes.RPCType_JSON_RPC},
						{Url: url, RpcType: sharedtypes.RPCType_WEBSOCKET},
					},
				},
			},
		})
	}
	return &sessiontypes.Session{
		SessionId: "test-session-drain",
		Header: &sessiontypes.SessionHeader{
			SessionId:               "test-session-drain",
			ServiceId:               "eth",
			SessionStartBlockHeight: 100,
			SessionEndBlockHeight:   110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers:   suppliers,
	}
}

// newDrainTestProtocol builds a minimal Protocol around session, with no
// drain store and no blocked domains, suitable for AvailableEndpoints tests.
func newDrainTestProtocol(t *testing.T, session *sessiontypes.Session) *Protocol {
	t.Helper()
	fnMock := &mockRelayFullNode{session: session}
	return &Protocol{
		fullNode:   fnMock,
		sessions:   newSessionManager(fnMock, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer:     &mockSigner{},
		bl:         newBlacklist(),
		ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: http.DefaultClient,
		metrics:    noopSupplierMetrics{},
		logger:     newTestLogger(),
	}
}

// containsHost reports whether any address in addrs contains host.
func containsHost(addrs domain.EndpointAddrList, host string) bool {
	for _, a := range addrs {
		if strings.Contains(string(a), host) {
			return true
		}
	}
	return false
}

func TestAvailableEndpoints_DrainedOperatorExcluded(t *testing.T) {
	session := buildDrainTestSession(map[string]string{
		"pokt1a": "http://a1.pocket.example",
		"pokt1b": "http://a2.pocket.example",
		"pokt1c": "http://b.other.example",
	})
	p := newDrainTestProtocol(t, session)

	store := drain.NewMemoryStore()
	p.SetDrains(store)

	before, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(before) != 3 {
		t.Fatalf("want 3 endpoints before drain, got %d: %v", len(before), before)
	}

	if err := store.Set(context.Background(), drain.Entry{
		Key:   drain.Key{ServiceID: "eth", Operator: "pocket.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	jsonRPC, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints(json_rpc): %v", err)
	}
	if len(jsonRPC) != 1 {
		t.Fatalf("want 1 json_rpc endpoint while pocket.example is drained, got %d: %v", len(jsonRPC), jsonRPC)
	}
	if !containsHost(jsonRPC, "b.other.example") {
		t.Errorf("surviving json_rpc endpoint should be at other.example, got %v", jsonRPC)
	}

	ws, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeWebSocket)
	if err != nil {
		t.Fatalf("AvailableEndpoints(websocket): %v", err)
	}
	if len(ws) != 3 {
		t.Errorf("a json_rpc-scoped drain must not affect websocket, want 3, got %d: %v", len(ws), ws)
	}

	if err := store.Release(context.Background(), drain.Key{
		ServiceID: "eth", Operator: "pocket.example", RPCType: domain.RPCTypeJSONRPC,
	}); err != nil {
		t.Fatalf("Release: %v", err)
	}

	after, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints after release: %v", err)
	}
	if len(after) != 3 {
		t.Errorf("want all 3 endpoints back after release, got %d: %v", len(after), after)
	}
}

func TestAvailableEndpoints_NilDrainStoreIsSafe(t *testing.T) {
	session := buildDrainTestSession(map[string]string{
		"pokt1a": "http://a1.pocket.example",
	})
	p := newDrainTestProtocol(t, session) // SetDrains never called

	got, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 endpoint with no drain store configured, got %d", len(got))
	}
}

func TestOperatorOf(t *testing.T) {
	cases := []struct {
		url  string
		want string
	}{
		{"http://RPC-1.Pocket.Example", "pocket.example"},
		{"https://a2.pocket.example:8545/v1", "pocket.example"},
		{"https://127.0.0.1:8080", "127.0.0.1"},
	}
	for _, c := range cases {
		if got := operatorOf(c.url); got != c.want {
			t.Errorf("operatorOf(%q) = %q, want %q", c.url, got, c.want)
		}
		// Second call exercises the memoized path; must agree.
		if got := operatorOf(c.url); got != c.want {
			t.Errorf("operatorOf(%q) (cached) = %q, want %q", c.url, got, c.want)
		}
	}
}

func TestProtocol_SetBlockedDomainsSwapsAtomically(t *testing.T) {
	session := buildDrainTestSession(map[string]string{
		"pokt1a": "http://a.pocket.example",
		"pokt1c": "http://b.other.example",
	})
	p := newDrainTestProtocol(t, session)

	before, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(before) != 2 {
		t.Fatalf("want 2 endpoints before blocking, got %d: %v", len(before), before)
	}

	if err := p.SetBlockedDomains([]config.BlockedDomain{{Domain: "other.example"}}); err != nil {
		t.Fatalf("SetBlockedDomains: %v", err)
	}

	after, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints after SetBlockedDomains: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("want 1 endpoint after blocking other.example, got %d: %v", len(after), after)
	}
	if !containsHost(after, "a.pocket.example") {
		t.Errorf("surviving endpoint should be at pocket.example, got %v", after)
	}
}

func TestProtocol_SetBlockedDomainsInvalidEntryLeavesOldListUntouched(t *testing.T) {
	p := &Protocol{logger: newTestLogger()}

	if err := p.SetBlockedDomains([]config.BlockedDomain{{Domain: "good.example"}}); err != nil {
		t.Fatalf("SetBlockedDomains: %v", err)
	}

	err := p.SetBlockedDomains([]config.BlockedDomain{{Domain: ""}})
	if err == nil {
		t.Fatal("expected an error for an entry with an empty domain")
	}

	if !p.blockedDomains.Load().IsBlocked("https://good.example", domain.RPCTypeJSONRPC) {
		t.Error("a failed SetBlockedDomains call must leave the previous list in place")
	}
}
