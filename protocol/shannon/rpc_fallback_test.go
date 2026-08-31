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

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"
	"google.golang.org/protobuf/proto"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// A cosmos supplier staked for json_rpc only. With the service mapping
// comet_bft onto json_rpc, it serves comet_bft requests through that URL —
// the PATH rpc_type_fallbacks contract, which the mainnet config relies on
// while suppliers catch up on their comet_bft stakes.
func TestAvailableEndpoints_RPCTypeFallback(t *testing.T) {
	session := buildRelayTestSession("pokt1jsononly", "https://node.example.com")
	sm := newSessionManager(&mockRelayFullNode{session: session}, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	p := &Protocol{
		sessions:  sm,
		bl:        newBlacklist(),
		ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		logger:    newTestLogger(),
	}

	got, err := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeCometBFT)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("without a fallback a json_rpc-only supplier must not serve comet_bft, got %v", got)
	}

	p.rpcFallbacks = rpcFallbackTable{"eth": {domain.RPCTypeCometBFT: domain.RPCTypeJSONRPC}}
	got, err = p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeCometBFT)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("with comet_bft->json_rpc the supplier must be offered, got %v", got)
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
