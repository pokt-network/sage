package shannon

import (
	"context"
	"errors"
	"testing"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
)

// stubFullNode satisfies fullNodeIface for testing sessionManager.
type stubFullNode struct {
	session   *sessiontypes.Session
	height    int64
	sessErr   error
	heightErr error
}

func (m *stubFullNode) GetSession(_ context.Context, _ string, _ string) (*sessiontypes.Session, error) {
	if m.sessErr != nil {
		return nil, m.sessErr
	}
	return m.session, nil
}

func (m *stubFullNode) GetApp(_ context.Context, _ string) (*apptypes.Application, error) {
	return &apptypes.Application{}, nil
}

func (m *stubFullNode) GetCurrentBlockHeight(_ context.Context) (int64, error) {
	if m.heightErr != nil {
		return 0, m.heightErr
	}
	return m.height, nil
}

func (m *stubFullNode) ValidateRelayResponse(_ string, _ []byte) (*servicetypes.RelayResponse, error) {
	return &servicetypes.RelayResponse{}, nil
}

func (m *stubFullNode) AccountClient() *sdk.AccountClient {
	return nil
}

// buildTestSession creates a minimal session with one supplier endpoint.
func buildTestSession(sessionID string, supplierAddr string, url string) *sessiontypes.Session {
	return &sessiontypes.Session{
		SessionId: sessionID,
		Header: &sessiontypes.SessionHeader{
			SessionId:               sessionID,
			ServiceId:               "eth",
			SessionStartBlockHeight: 100,
			SessionEndBlockHeight:   110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers: []*sharedtypes.Supplier{
			{
				OperatorAddress: supplierAddr,
				Services: []*sharedtypes.SupplierServiceConfig{
					{
						ServiceId: "eth",
						Endpoints: []*sharedtypes.SupplierEndpoint{
							{
								Url:     url,
								RpcType: sharedtypes.RPCType_JSON_RPC,
							},
						},
					},
				},
			},
		},
	}
}

func TestSessionManager_GetEndpoints(t *testing.T) {
	session := buildTestSession("session1", "pokt1supplier", "https://relay.example.com")

	fn := &stubFullNode{session: session, height: 200}
	sm := newSessionManager(
		fn,
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)
	// Inject mock by calling getOrCreateEndpoints directly.
	endpoints := sm.getOrCreateEndpoints(session)

	if len(endpoints) == 0 {
		t.Fatal("expected at least one endpoint")
	}

	for _, ep := range endpoints {
		if ep.Supplier() != "pokt1supplier" {
			t.Errorf("supplier = %q, want %q", ep.Supplier(), "pokt1supplier")
		}
		url, err := ep.GetURL(domain.RPCTypeJSONRPC)
		if err != nil {
			t.Fatalf("GetURL: %v", err)
		}
		if url != "https://relay.example.com" {
			t.Errorf("url = %q, want %q", url, "https://relay.example.com")
		}
	}

}

func TestSessionManager_EndpointCaching(t *testing.T) {
	session := buildTestSession("session-cache", "pokt1supplier", "https://example.com")

	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	// First call populates cache.
	ep1 := sm.getOrCreateEndpoints(session)
	// Second call should return the same map from cache.
	ep2 := sm.getOrCreateEndpoints(session)

	if len(ep1) != len(ep2) {
		t.Errorf("cache mismatch: first call got %d, second got %d", len(ep1), len(ep2))
	}

	// Verify it's actually the same underlying map via pointer.
	for addr := range ep1 {
		if ep1[addr] != ep2[addr] {
			t.Error("expected same pointer from cache")
		}
	}
}

func TestSessionEndpoints_RPCTypeSupport(t *testing.T) {
	// RPC-type filtering happens inline in Protocol.AvailableEndpoints via
	// endpoint.GetURL; assert the underlying support check here.
	session := buildTestSession("session-filter", "pokt1supplier", "https://example.com")

	sm := newSessionManager(
		&stubFullNode{height: 200},
		map[domain.ServiceID]struct{}{"eth": {}},
		newTestLogger(),
	)

	endpoints := sm.getOrCreateEndpoints(session)
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}
	for _, ep := range endpoints {
		// JSON-RPC should be supported.
		if _, err := ep.GetURL(domain.RPCTypeJSONRPC); err != nil {
			t.Errorf("expected JSON-RPC support, got error: %v", err)
		}
		// REST should not be supported.
		if _, err := ep.GetURL(domain.RPCTypeREST); err == nil {
			t.Error("expected REST to be unsupported")
		}
	}
}

func TestSessionManager_LatestBlockHeight_RetainsLastGoodOnPollError(t *testing.T) {
	fn := &stubFullNode{
		session: buildTestSession("session-1", "pokt1supplier", "https://relay.example.com"),
		height:  50,
	}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())

	if got := sm.LatestBlockHeight(); got != 0 {
		t.Errorf("height before first poll = %d, want 0", got)
	}

	sm.pollBlockHeight(context.Background())
	if got := sm.LatestBlockHeight(); got != 50 {
		t.Fatalf("height after poll = %d, want 50", got)
	}

	// A failing poll must leave the last good height in place: zeroing it would
	// read as "not yet polled" to every bridge at once.
	fn.heightErr = errors.New("full node unreachable")
	sm.pollBlockHeight(context.Background())
	if got := sm.LatestBlockHeight(); got != 50 {
		t.Errorf("height after failed poll = %d, want 50 retained", got)
	}
}

func TestSessionManager_ConfiguredServices(t *testing.T) {
	services := map[domain.ServiceID]struct{}{
		"eth":  {},
		"poly": {},
	}
	sm := newSessionManager(&stubFullNode{height: 200}, services, newTestLogger())
	got := sm.ConfiguredServices()
	if len(got) != 2 {
		t.Errorf("ConfiguredServices() = %d, want 2", len(got))
	}
	if _, ok := got["eth"]; !ok {
		t.Error("expected 'eth' in configured services")
	}
}
