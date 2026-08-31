package shannon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"google.golang.org/protobuf/proto"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// typedSession builds a one-service session where each supplier stakes the
// RPC types listed for it, all on one URL per supplier.
func typedSession(stakes map[string][]sharedtypes.RPCType) *sessiontypes.Session {
	s := &sessiontypes.Session{
		SessionId: "typed-session",
		Header: &sessiontypes.SessionHeader{
			SessionId: "typed-session", ServiceId: "eth",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
	}
	for addr, types := range stakes {
		var eps []*sharedtypes.SupplierEndpoint
		for _, t := range types {
			eps = append(eps, &sharedtypes.SupplierEndpoint{Url: "https://" + addr + ".example.com", RpcType: t})
		}
		s.Suppliers = append(s.Suppliers, &sharedtypes.Supplier{
			OperatorAddress: addr,
			Services:        []*sharedtypes.SupplierServiceConfig{{ServiceId: "eth", Endpoints: eps}},
		})
	}
	return s
}

func fallbackProtocol(session *sessiontypes.Session, table rpcFallbackTable) *Protocol {
	sm := newSessionManager(&mockRelayFullNode{session: session}, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	return &Protocol{
		sessions:     sm,
		bl:           newBlacklist(),
		ownedApps:    map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		rpcFallbacks: table,
		logger:       newTestLogger(),
	}
}

// PATH's rpc_type_fallbacks is a pool-level switch: the fallback type is used
// only when NO supplier in the session staked the requested one. Applying it
// per supplier instead sent tron JSON-RPC to REST-only suppliers' roots on the
// mainnet canary — 405 on a fifth of tron responses — while PATH, with the
// same config and suppliers, never fell back at all.
func TestAvailableEndpoints_RPCTypeFallbackIsPoolLevel(t *testing.T) {
	table := rpcFallbackTable{"eth": {domain.RPCTypeJSONRPC: domain.RPCTypeREST}}

	mixed := typedSession(map[string][]sharedtypes.RPCType{
		"pokt1jsonrpc":  {sharedtypes.RPCType_JSON_RPC},
		"pokt1restonly": {sharedtypes.RPCType_REST},
	})
	got, err := fallbackProtocol(mixed, table).AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.HasPrefix(string(got[0]), "pokt1jsonrpc-") {
		t.Fatalf("with a json_rpc supplier present the REST-only one must not be offered, got %v", got)
	}

	restOnly := typedSession(map[string][]sharedtypes.RPCType{
		"pokt1restonly": {sharedtypes.RPCType_REST},
	})
	got, err = fallbackProtocol(restOnly, table).AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("with no json_rpc supplier at all the pool falls back to REST, got %v", got)
	}

	got, err = fallbackProtocol(restOnly, nil).AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("without a mapping there is no fallback, got %v", got)
	}
}

func TestSendRelay_RPCTypeFallbackUsesFallbackURL(t *testing.T) {
	respBz, err := buildSerializedRelayResponse([]byte(`{"result":{}}`), http.StatusOK)
	if err != nil {
		t.Fatal(err)
	}
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write(respBz)
	}))
	defer server.Close()

	poktResp := &sdktypes.POKTHTTPResponse{StatusCode: http.StatusOK, BodyBz: []byte(`{"result":{}}`), Header: map[string]*sdktypes.Header{}}
	validated, err := proto.MarshalOptions{Deterministic: true}.Marshal(poktResp)
	if err != nil {
		t.Fatal(err)
	}
	session := buildRelayTestSession("pokt1jsononly", server.URL)
	fn := &mockRelayFullNode{session: session, height: 105, validateResponse: &servicetypes.RelayResponse{Payload: validated}}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	p := &Protocol{
		fullNode:     fn,
		sessions:     sm,
		signer:       &mockSigner{},
		bl:           newBlacklist(),
		ownedApps:    map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient:   server.Client(),
		logger:       newTestLogger(),
		rpcFallbacks: rpcFallbackTable{"eth": {domain.RPCTypeCometBFT: domain.RPCTypeJSONRPC}},
	}
	var addr domain.EndpointAddr
	for a := range sm.getOrCreateEndpoints(session) {
		addr = a
	}

	payload := domain.NewPayload(nil, domain.RPCTypeCometBFT, "status").WithHTTP("/status", http.MethodGet)
	if _, err := p.SendRelay(context.Background(), "eth", addr, payload); err != nil {
		t.Fatalf("SendRelay through the fallback URL: %v", err)
	}
	// The only URL this supplier staked is the json_rpc one, so a relay
	// arriving at all is the fallback being taken. (The comet_bft path rides
	// inside the signed relay request, not on the relay miner's URL — see
	// TestSendRelay_EmbedsPayloadPathAndVerb.)
	if hits != 1 {
		t.Errorf("relay miner hit %d times, want 1", hits)
	}
}

func TestBuildRPCFallbacks(t *testing.T) {
	table := buildRPCFallbacks([]config.ServiceConfig{
		{ID: "kava", RPCTypeFallbacks: map[string]string{"comet_bft": "json_rpc"}},
		{ID: "eth"},
	})
	if got := table.resolve("kava", domain.RPCTypeCometBFT); got != domain.RPCTypeJSONRPC {
		t.Errorf("kava comet_bft -> %q, want json_rpc", got)
	}
	if got := table.resolve("kava", domain.RPCTypeREST); got != "" {
		t.Errorf("unmapped type -> %q, want none", got)
	}
	if got := table.resolve("eth", domain.RPCTypeCometBFT); got != "" {
		t.Errorf("service without fallbacks -> %q, want none", got)
	}
	var none rpcFallbackTable
	if got := none.resolve("kava", domain.RPCTypeCometBFT); got != "" {
		t.Errorf("nil table -> %q, want none", got)
	}
}

// A service whose app has no suppliers fails getSession on every cycle. The
// first failure is an error; failing again is not news — even when the full
// node's message differs, as it does at every session boundary because it
// carries the block height.
func TestRefreshSession_RepeatedFailureLogsOnce(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	fn := &mockRelayFullNode{}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"router": {}}, logger)

	for _, height := range []int{901781, 901801, 901821} {
		fn.sessionErr = fmt.Errorf("could not find suppliers for service router at height %d", height)
		if _, err := sm.getSession(context.Background(), "router", "pokt1app"); err == nil {
			t.Fatal("expected getSession to fail")
		}
	}
	if n := strings.Count(buf.String(), "full node returned error"); n != 1 {
		t.Fatalf("logged the same failure %d times at warn+, want 1:\n%s", n, buf.String())
	}

	// Recovery is news again, and so is the next failure after it.
	fn.sessionErr = nil
	fn.session = buildRelayTestSession("pokt1supplier", "https://node.example.com")
	if _, err := sm.refreshSession(context.Background(), "router", "pokt1app"); err != nil {
		t.Fatal(err)
	}
	fn.sessionErr = errors.New("no suppliers not found for session")
	_, _ = sm.refreshSession(context.Background(), "router", "pokt1app")
	if n := strings.Count(buf.String(), "full node returned error"); n != 2 {
		t.Fatalf("a failure after recovery must log again, got %d entries:\n%s", n, buf.String())
	}
}
