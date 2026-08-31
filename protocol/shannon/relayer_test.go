package shannon

import (
	"errors"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdk "github.com/pokt-network/shannon-sdk"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"google.golang.org/protobuf/proto"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// mockRelayFullNode stubs fullNodeIface for relay testing.
type mockRelayFullNode struct {
	session          *sessiontypes.Session
	sessionErr       error
	app              *apptypes.Application
	height           int64
	validateResponse *servicetypes.RelayResponse
	validateErr      error
}

func (m *mockRelayFullNode) GetSession(_ context.Context, _ string, _ string) (*sessiontypes.Session, error) {
	if m.sessionErr != nil {
		return nil, m.sessionErr
	}
	return m.session, nil
}

func (m *mockRelayFullNode) GetApp(_ context.Context, _ string) (*apptypes.Application, error) {
	if m.app != nil {
		return m.app, nil
	}
	return &apptypes.Application{Address: "pokt1app"}, nil
}

func (m *mockRelayFullNode) GetCurrentBlockHeight(_ context.Context) (int64, error) {
	return m.height, nil
}

func (m *mockRelayFullNode) ValidateRelayResponse(_ string, _ []byte) (*servicetypes.RelayResponse, error) {
	return m.validateResponse, m.validateErr
}

func (m *mockRelayFullNode) AccountClient() *sdk.AccountClient {
	return nil
}

// mockSigner always signs by returning the request unchanged.
type mockSigner struct {
	called bool
}

func (ms *mockSigner) signRelayRequest(_ context.Context, req *servicetypes.RelayRequest, _ *apptypes.Application) (*servicetypes.RelayRequest, error) {
	ms.called = true
	return req, nil
}

// buildRelayTestSession builds a session with a supplier endpoint at the given URL.
func buildRelayTestSession(supplierAddr, url string) *sessiontypes.Session {
	return &sessiontypes.Session{
		SessionId: "test-session-1",
		Header: &sessiontypes.SessionHeader{
			SessionId:               "test-session-1",
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

// buildSerializedRelayResponse creates a properly serialized relay response payload
// containing the given body bytes and status code.
func buildSerializedRelayResponse(body []byte, statusCode int) ([]byte, error) {
	poktResp := &sdktypes.POKTHTTPResponse{
		StatusCode: uint32(statusCode),
		BodyBz:     body,
		Header:     map[string]*sdktypes.Header{},
	}
	opts := proto.MarshalOptions{Deterministic: true}
	payload, err := opts.Marshal(poktResp)
	if err != nil {
		return nil, err
	}
	return (&servicetypes.RelayResponse{Payload: payload}).Marshal()
}

func TestSendRelay_Success(t *testing.T) {
	responseBody := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)

	// Build the serialized relay response that the endpoint server will return.
	respBz, err := buildSerializedRelayResponse(responseBody, http.StatusOK)
	if err != nil {
		t.Fatalf("failed to build serialized relay response: %v", err)
	}

	// httptest server simulates the relay miner endpoint.
	var receivedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBz)
	}))
	defer server.Close()
	_ = receivedBody

	supplierAddr := "pokt1supplier"
	session := buildRelayTestSession(supplierAddr, server.URL)

	// Build deserialized relay response for mock validation.
	poktResp := &sdktypes.POKTHTTPResponse{
		StatusCode: http.StatusOK,
		BodyBz:     responseBody,
		Header:     map[string]*sdktypes.Header{},
	}
	opts := proto.MarshalOptions{Deterministic: true}
	payload, err := opts.Marshal(poktResp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	signerMock := &mockSigner{}
	fnMock := &mockRelayFullNode{
		session: session,
		height:  200,
		validateResponse: &servicetypes.RelayResponse{
			Payload: payload,
		},
	}

	sm := newSessionManager(fnMock, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())

	p := &Protocol{
		fullNode:   fnMock,
		sessions:   sm,
		signer:     signerMock,
		bl:         newBlacklist(),
		ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: server.Client(),
		logger:     newTestLogger(),
	}

	// Derive the expected endpoint address from the session.
	endpoints := sm.getOrCreateEndpoints(session)
	if len(endpoints) == 0 {
		t.Fatal("expected at least one endpoint in session")
	}

	var endpointAddr domain.EndpointAddr
	for addr := range endpoints {
		endpointAddr = addr
		break
	}

	payload2 := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	resp, err := p.SendRelay(context.Background(), "eth", endpointAddr, payload2)
	if err != nil {
		t.Fatalf("SendRelay error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
		return
	}
	if resp.HTTPStatusCode != http.StatusOK {
		t.Errorf("status code = %d, want %d", resp.HTTPStatusCode, http.StatusOK)
	}
	if string(resp.Body) != string(responseBody) {
		t.Errorf("body = %q, want %q", resp.Body, responseBody)
	}
	if resp.EndpointAddr != endpointAddr {
		t.Errorf("EndpointAddr = %q, want %q", resp.EndpointAddr, endpointAddr)
	}
	if !signerMock.called {
		t.Error("expected signer to be called")
	}
	if resp.Latency <= 0 {
		t.Error("expected non-zero latency")
	}
}

// The relay miner replays the *embedded* request's URL and verb against its
// backend. A REST or CometBFT request is addressed by path, so dropping either
// turns every such relay into a POST against the backend's root.
func TestSendRelay_EmbedsPayloadPathAndVerb(t *testing.T) {
	respBz, err := buildSerializedRelayResponse([]byte(`{"result":{}}`), http.StatusOK)
	if err != nil {
		t.Fatalf("failed to build serialized relay response: %v", err)
	}

	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBz)
	}))
	defer server.Close()

	const supplierAddr = "pokt1supplier"
	session := buildRelayTestSession(supplierAddr, server.URL)
	// Restake the endpoint as CometBFT so GetURL resolves for that RPC type.
	session.Suppliers[0].Services[0].Endpoints[0].RpcType = sharedtypes.RPCType_COMET_BFT

	fnMock := &mockRelayFullNode{
		session:          session,
		height:           200,
		validateResponse: &servicetypes.RelayResponse{Payload: respBz},
	}
	sm := newSessionManager(fnMock, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	p := &Protocol{
		fullNode:   fnMock,
		sessions:   sm,
		signer:     &mockSigner{},
		bl:         newBlacklist(),
		ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: server.Client(),
		logger:     newTestLogger(),
	}

	var endpointAddr domain.EndpointAddr
	for addr := range sm.getOrCreateEndpoints(session) {
		endpointAddr = addr
	}

	payload := domain.NewPayload(nil, domain.RPCTypeCometBFT, "block").
		WithHTTP("/block?height=42", http.MethodGet)
	if _, err := p.SendRelay(context.Background(), "eth", endpointAddr, payload); err != nil {
		t.Fatalf("SendRelay error: %v", err)
	}

	var relayReq servicetypes.RelayRequest
	if err := relayReq.Unmarshal(received); err != nil {
		t.Fatalf("supplier received a body that is not a RelayRequest: %v", err)
	}
	embedded, err := sdktypes.DeserializeHTTPRequest(relayReq.Payload)
	if err != nil {
		t.Fatalf("deserialize embedded request: %v", err)
	}

	if want := server.URL + "/block?height=42"; embedded.Url != want {
		t.Errorf("embedded URL = %q, want %q", embedded.Url, want)
	}
	if embedded.Method != http.MethodGet {
		t.Errorf("embedded method = %q, want %q", embedded.Method, http.MethodGet)
	}
}

// A payload with no path or verb keeps the historical JSON-RPC behaviour:
// POST straight at the supplier's staked URL.
func TestPayloadURLAndMethod_DefaultToPostAtRoot(t *testing.T) {
	const base = "https://supplier.example"

	tests := []struct {
		name       string
		path       string
		httpMethod string
		wantURL    string
		wantMethod string
	}{
		{"unset", "", "", base, http.MethodPost},
		{"root", "/", "", base, http.MethodPost},
		{"path", "/status", http.MethodGet, base + "/status", http.MethodGet},
		{"path with query", "/block?height=1", http.MethodGet, base + "/block?height=1", http.MethodGet},
		{"path missing leading slash", "status", http.MethodGet, base + "/status", http.MethodGet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := domain.NewPayload(nil, domain.RPCTypeCometBFT, "").WithHTTP(tt.path, tt.httpMethod)
			if got := payloadURL(base, p); got != tt.wantURL {
				t.Errorf("payloadURL = %q, want %q", got, tt.wantURL)
			}
			if got := payloadHTTPMethod(p); got != tt.wantMethod {
				t.Errorf("payloadHTTPMethod = %q, want %q", got, tt.wantMethod)
			}
		})
	}
}

// A staked URL that carries a path is unexpected, but silently dropping it
// would send the relay somewhere the supplier never advertised.
func TestPayloadURL_KeepsStakedURLPath(t *testing.T) {
	p := domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/cosmos/bank/v1beta1/params", http.MethodGet)
	got := payloadURL("https://supplier.example/rpc/", p)
	if want := "https://supplier.example/rpc/cosmos/bank/v1beta1/params"; got != want {
		t.Errorf("payloadURL = %q, want %q", got, want)
	}
}

func TestSendRelay_EndpointNotFound(t *testing.T) {
	session := buildRelayTestSession("pokt1supplier", "https://example.com")

	signerMock := &mockSigner{}
	fnMock := &mockRelayFullNode{session: session}

	sm := newSessionManager(fnMock, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	p := &Protocol{
		fullNode:   fnMock,
		sessions:   sm,
		signer:     signerMock,
		bl:         newBlacklist(),
		ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: http.DefaultClient,
		logger:     newTestLogger(),
	}

	_, err := p.SendRelay(context.Background(), "eth", "nonexistent-endpoint", domain.NewPayload(nil, domain.RPCTypeJSONRPC, ""))
	if err == nil {
		t.Fatal("expected error for unknown endpoint")
	}
	// An endpoint that is not in the current session is the session having
	// rolled over between selection and send, not a client or supplier fault:
	// retryable, and carrying the sentinel so Retry reselects from the fresh
	// session rather than trying this list's other (also stale) members.
	if !domain.IsRetryable(err) {
		t.Errorf("endpoint-not-in-session must be retryable, got %v", err)
	}
	if !errors.Is(err, domain.ErrEndpointsStale) {
		t.Errorf("expected ErrEndpointsStale sentinel, got %v", err)
	}
}

func TestSendRelay_NoAppForService(t *testing.T) {
	signerMock := &mockSigner{}
	fnMock := &mockRelayFullNode{}

	sm := newSessionManager(fnMock, map[domain.ServiceID]struct{}{}, newTestLogger())
	p := &Protocol{
		fullNode:   fnMock,
		sessions:   sm,
		signer:     signerMock,
		bl:         newBlacklist(),
		ownedApps:  map[domain.ServiceID][]string{},
		httpClient: http.DefaultClient,
		logger:     newTestLogger(),
	}

	_, err := p.SendRelay(context.Background(), "eth", "addr", domain.NewPayload(nil, domain.RPCTypeJSONRPC, ""))
	if err == nil {
		t.Fatal("expected error when no app configured for service")
	}
}

func TestAvailableEndpoints_FiltersBlacklisted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	supplierAddr := "pokt1badsupplier"
	session := buildRelayTestSession(supplierAddr, server.URL)

	fnMock := &mockRelayFullNode{session: session}
	sm := newSessionManager(fnMock, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	bl := newBlacklist()

	p := &Protocol{
		fullNode:   fnMock,
		sessions:   sm,
		signer:     &mockSigner{},
		bl:         bl,
		ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: server.Client(),
		logger:     newTestLogger(),
	}

	// Should return the endpoint before blacklisting.
	endpoints, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(endpoints) == 0 {
		t.Fatal("expected at least one endpoint before blacklisting")
	}

	// Blacklist the supplier.
	p.BlacklistSupplier("eth", supplierAddr)

	// Should now return empty.
	endpoints, err = p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints after blacklist: %v", err)
	}
	if len(endpoints) != 0 {
		t.Errorf("expected 0 endpoints after blacklisting, got %d", len(endpoints))
	}
}

func TestProtocol_SupplierManager(t *testing.T) {
	p := &Protocol{bl: newBlacklist(), logger: slog.Default()}

	p.BlacklistSupplier("eth", "pokt1x")
	if !p.IsBlacklisted("eth", "pokt1x") {
		t.Error("should be blacklisted")
	}

	removed := p.UnblacklistSupplier("eth", "pokt1x")
	if !removed {
		t.Error("UnblacklistSupplier should return true")
	}
	if p.IsBlacklisted("eth", "pokt1x") {
		t.Error("should not be blacklisted after removal")
	}
}

func TestProtocol_ConfiguredServices(t *testing.T) {
	sm := newSessionManager(
		&stubFullNode{},
		map[domain.ServiceID]struct{}{"eth": {}, "poly": {}},
		newTestLogger(),
	)
	p := &Protocol{sessions: sm}

	svcs := p.ConfiguredServices()
	if len(svcs) != 2 {
		t.Errorf("ConfiguredServices() = %d, want 2", len(svcs))
	}
}

// Asserted through AvailableEndpoints and SendRelay rather than through
// IsBlocked: a blocklist that is never consulted on the production path passes
// its own unit tests perfectly.
func TestAvailableEndpoints_ExcludesBlockedDomain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	session := buildRelayTestSession("pokt1supplier", server.URL)
	fnMock := &mockRelayFullNode{session: session}

	newProtocol := func(t *testing.T, entries ...config.BlockedDomain) *Protocol {
		t.Helper()
		bl, err := newDomainBlocklist(entries)
		if err != nil {
			t.Fatalf("newDomainBlocklist: %v", err)
		}
		p := &Protocol{
			fullNode:   fnMock,
			sessions:   newSessionManager(fnMock, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
			signer:     &mockSigner{},
			bl:         newBlacklist(),
			ownedApps:  map[domain.ServiceID][]string{"eth": {"pokt1app"}},
			httpClient: server.Client(),
			metrics:    noopSupplierMetrics{},
			logger:     newTestLogger(),
		}
		p.blockedDomains.Store(bl)
		return p
	}

	// The endpoint is served over 127.0.0.1, which has no registrable domain
	// and is therefore its own operator — an exact-host ban.
	const blockedHost = "127.0.0.1"

	unblocked := newProtocol(t)
	before, err := unblocked.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("expected an endpoint before the domain is blocked")
	}
	endpointAddr := before[0]

	blockedForWS := newProtocol(t, config.BlockedDomain{Domain: blockedHost, RPCTypes: []string{"websocket"}})
	stillThere, err := blockedForWS.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(stillThere) != len(before) {
		t.Errorf("a websocket-only ban removed %d JSON-RPC endpoints", len(before)-len(stillThere))
	}

	blocked := newProtocol(t, config.BlockedDomain{Domain: blockedHost})
	after, err := blocked.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("AvailableEndpoints: %v", err)
	}
	if len(after) != 0 {
		t.Errorf("expected 0 endpoints at a blocked domain, got %d", len(after))
	}

	// And an endpoint address obtained before the ban still cannot be relayed
	// to — selection is where the ban works, SendRelay is where it holds.
	_, err = blocked.SendRelay(context.Background(), "eth", endpointAddr,
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, ""))
	if err == nil {
		t.Fatal("SendRelay to a blocked domain succeeded")
	}
	if !strings.Contains(err.Error(), "blocked domain") {
		t.Errorf("SendRelay error = %v, want it to name the blocked domain", err)
	}
}
