package cosmos

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// --- helpers --- //

func newPlugin(syncAllowance uint64, rpcTypes ...domain.RPCType) *Plugin {
	if len(rpcTypes) == 0 {
		rpcTypes = []domain.RPCType{domain.RPCTypeREST, domain.RPCTypeCometBFT, domain.RPCTypeJSONRPC}
	}
	return NewPlugin(nil, Config{SyncAllowance: syncAllowance, SupportedRPCTypes: rpcTypes})
}

// newPluginWithChainID builds a plugin that asserts the given CometBFT network.
func newPluginWithChainID(chainID string) *Plugin {
	return NewPlugin(nil, Config{SyncAllowance: 100, ExpectedChainID: chainID})
}

func makeRequest(method, path, body string) *http.Request {
	var reqBody *bytes.Reader
	if body != "" {
		reqBody = bytes.NewReader([]byte(body))
	} else {
		reqBody = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reqBody)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func endpointAddr(s string) domain.EndpointAddr {
	return domain.EndpointAddr(s)
}

// --- ParseRequest tests --- //

func TestParseRequest_JSONRPC(t *testing.T) {
	p := newPlugin(10)
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := makeRequest(http.MethodPost, "/", body)

	payloads, err := p.ParseRequest(context.Background(), req, []byte(body), domain.RPCTypeUnknown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	if payloads[0].RPCType() != domain.RPCTypeJSONRPC {
		t.Errorf("expected RPCTypeJSONRPC, got %q", payloads[0].RPCType())
	}
	if payloads[0].Method() != "eth_blockNumber" {
		t.Errorf("expected method eth_blockNumber, got %q", payloads[0].Method())
	}
}

func TestParseRequest_CometBFTMethod(t *testing.T) {
	p := newPlugin(10)
	body := `{"jsonrpc":"2.0","method":"status","params":[],"id":1}`
	req := makeRequest(http.MethodPost, "/", body)

	payloads, err := p.ParseRequest(context.Background(), req, []byte(body), domain.RPCTypeUnknown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payloads[0].RPCType() != domain.RPCTypeCometBFT {
		t.Errorf("expected RPCTypeCometBFT, got %q", payloads[0].RPCType())
	}
	if payloads[0].Method() != "status" {
		t.Errorf("expected method status, got %q", payloads[0].Method())
	}
}

func TestParseRequest_CometBFTPath_GET(t *testing.T) {
	p := newPlugin(10)
	req := makeRequest(http.MethodGet, "/status", "")

	payloads, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeUnknown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payloads[0].RPCType() != domain.RPCTypeCometBFT {
		t.Errorf("expected RPCTypeCometBFT for /status, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_CometBFTPath_Block(t *testing.T) {
	p := newPlugin(10)
	req := makeRequest(http.MethodGet, "/block?height=100", "")

	payloads, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeUnknown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payloads[0].RPCType() != domain.RPCTypeCometBFT {
		t.Errorf("expected RPCTypeCometBFT for /block, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_CosmosRESTPath(t *testing.T) {
	p := newPlugin(10)
	req := makeRequest(http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/latest", "")

	payloads, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeUnknown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payloads[0].RPCType() != domain.RPCTypeREST {
		t.Errorf("expected RPCTypeREST, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_IBCRESTPath(t *testing.T) {
	p := newPlugin(10)
	req := makeRequest(http.MethodGet, "/ibc/apps/transfer/v1/params", "")

	payloads, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeUnknown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payloads[0].RPCType() != domain.RPCTypeREST {
		t.Errorf("expected RPCTypeREST for /ibc path, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_UnsupportedRPCType_Rejected(t *testing.T) {
	// Plugin only supports REST.
	p := newPlugin(10, domain.RPCTypeREST)
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := makeRequest(http.MethodPost, "/", body)

	_, err := p.ParseRequest(context.Background(), req, []byte(body), domain.RPCTypeUnknown)
	if err == nil {
		t.Fatal("expected error for unsupported RPC type, got nil")
	}
}

// --- ParseBlockHeight tests --- //

func TestParseBlockHeight_CometBFT(t *testing.T) {
	p := newPlugin(10)
	resp := []byte(`{"jsonrpc":"2.0","id":-1,"result":{"sync_info":{"latest_block_height":"98765"}}}`)

	h, err := p.ParseBlockHeight(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 98765 {
		t.Errorf("expected 98765, got %d", h)
	}
}

func TestParseBlockHeight_CosmosREST(t *testing.T) {
	p := newPlugin(10)
	resp := []byte(`{"height":"12345","hash":"AABBCC"}`)

	h, err := p.ParseBlockHeight(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 12345 {
		t.Errorf("expected 12345, got %d", h)
	}
}

func TestParseBlockHeight_CometBFT_TakesPriorityOverREST(t *testing.T) {
	// Both fields present — CometBFT sync_info should win.
	p := newPlugin(10)
	resp := []byte(`{"height":"100","result":{"sync_info":{"latest_block_height":"200"}}}`)

	h, err := p.ParseBlockHeight(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 200 {
		t.Errorf("expected CometBFT height 200, got %d", h)
	}
}

func TestParseBlockHeight_EmptyResponse(t *testing.T) {
	p := newPlugin(10)
	_, err := p.ParseBlockHeight(nil)
	if err == nil {
		t.Fatal("expected error for empty response")
	}
}

func TestParseBlockHeight_NoHeightField(t *testing.T) {
	p := newPlugin(10)
	resp := []byte(`{"foo":"bar"}`)
	_, err := p.ParseBlockHeight(resp)
	if err == nil {
		t.Fatal("expected error when no height field present")
	}
}

func TestParseBlockHeight_InvalidDecimal(t *testing.T) {
	p := newPlugin(10)
	resp := []byte(`{"height":"not-a-number"}`)
	_, err := p.ParseBlockHeight(resp)
	if err == nil {
		t.Fatal("expected error for non-decimal height")
	}
}

// --- SelectEndpoints tests --- //

func TestSelectEndpoints_Empty(t *testing.T) {
	p := newPlugin(10)
	result, err := p.SelectEndpoints(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d endpoints", len(result))
	}
}

func TestSelectEndpoints_BlockHeightFilter(t *testing.T) {
	p := newPlugin(5)

	// Set up two endpoints: one current, one too far behind.
	current := endpointAddr("pokt1abc-https://current.example.com")
	stale := endpointAddr("pokt1def-https://stale.example.com")

	p.UpdateBlockHeight(current, 1000)
	p.UpdateBlockHeight(stale, 900) // 100 blocks behind, allowance is 5

	endpoints := domain.EndpointAddrList{current, stale}
	payloads := []domain.Payload{domain.NewPayload(nil, domain.RPCTypeREST, "")}

	result, err := p.SelectEndpoints(endpoints, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Contains(current) {
		t.Error("expected current endpoint to pass filter")
	}
	if result.Contains(stale) {
		t.Error("expected stale endpoint to be filtered out")
	}
}

func TestSelectEndpoints_UnknownEndpointPassesThrough(t *testing.T) {
	p := newPlugin(5)

	// Set perceived block via one known endpoint.
	known := endpointAddr("pokt1abc-https://known.example.com")
	p.UpdateBlockHeight(known, 1000)

	// Unknown endpoint (not in store) should pass through.
	unknown := endpointAddr("pokt1xyz-https://unknown.example.com")
	endpoints := domain.EndpointAddrList{unknown}

	result, err := p.SelectEndpoints(endpoints, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Contains(unknown) {
		t.Error("expected unknown endpoint to pass through")
	}
}

func TestSelectEndpoints_DegradedFallback(t *testing.T) {
	// All endpoints are stale — should degrade and return something.
	p := newPlugin(5)

	ep1 := endpointAddr("pokt1a-https://ep1.example.com")
	ep2 := endpointAddr("pokt1b-https://ep2.example.com")
	p.UpdateBlockHeight(ep1, 800)
	p.UpdateBlockHeight(ep2, 750)

	// Force perceived to be high by also updating a third (not-in-candidates) endpoint.
	anchor := endpointAddr("pokt1z-https://anchor.example.com")
	p.UpdateBlockHeight(anchor, 1000)

	endpoints := domain.EndpointAddrList{ep1, ep2}
	result, err := p.SelectEndpoints(endpoints, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Must return at least one endpoint (degraded fallback).
	if len(result) == 0 {
		t.Error("expected degraded fallback to return endpoints")
	}
}

// --- HealthChecks tests --- //

func TestHealthChecks_DefaultCometBFT(t *testing.T) {
	p := newPlugin(10)

	checks := p.HealthChecks()
	if len(checks) != 1 {
		t.Fatalf("expected 1 health check, got %d", len(checks))
	}
	if checks[0].Name != "comet_bft_status" {
		t.Errorf("expected comet_bft_status, got %q", checks[0].Name)
	}
	if checks[0].Payload.RPCType() != domain.RPCTypeCometBFT {
		t.Errorf("expected CometBFT payload, got %q", checks[0].Payload.RPCType())
	}
}

// --- CometBFT method / path detection tests --- //

func TestIsCometBFTMethod(t *testing.T) {
	cases := []struct {
		method string
		want   bool
	}{
		{"status", true},
		{"health", true},
		{"block", true},
		{"block_results", true},
		{"blockchain", true},
		{"commit", true},
		{"validators", true},
		{"genesis", true},
		{"consensus_state", true},
		{"dump_consensus_state", true},
		{"net_info", true},
		{"abci_info", true},
		{"abci_query", true},
		{"eth_blockNumber", false},
		{"", false},
		{"STATUS", true}, // case-insensitive
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			if got := isCometBFTMethod(tc.method); got != tc.want {
				t.Errorf("isCometBFTMethod(%q) = %v, want %v", tc.method, got, tc.want)
			}
		})
	}
}

func TestIsCometBFTPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/status", true},
		{"/health", true},
		{"/block", true},
		{"/block/10", true},
		{"/blockchain", true},
		{"/block?height=100", true},
		{"/validators?height=1", true},
		{"/cosmos/base/tendermint/v1beta1/blocks/latest", false},
		{"/ibc/apps/transfer/v1/params", false},
		{"/", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			if got := isCometBFTPath(tc.path); got != tc.want {
				t.Errorf("isCometBFTPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestIsCosmosRESTPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/cosmos/base/tendermint/v1beta1/blocks/latest", true},
		{"/cosmos/bank/v1beta1/balances/cosmos1abc", true},
		{"/ibc/apps/transfer/v1/params", true},
		{"/osmosis/gamm/v1beta1/pools", true},
		{"/status", false},
		{"/block", false},
		{"/", false},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			if got := isCosmosRESTPath(tc.path); got != tc.want {
				t.Errorf("isCosmosRESTPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// --- ExtractData tests --- //

func TestExtractData_CometBFTResponse(t *testing.T) {
	p := newPlugin(10)
	ep := endpointAddr("pokt1abc-https://node.example.com")
	resp := []byte(`{"jsonrpc":"2.0","id":-1,"result":{"sync_info":{"latest_block_height":"55000"}}}`)

	data, err := p.ExtractData(ep, nil, resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.BlockHeight == nil {
		t.Fatal("expected non-nil BlockHeight")
	}
	if *data.BlockHeight != 55000 {
		t.Errorf("expected 55000, got %d", *data.BlockHeight)
	}
	// Should also update the store.
	stored, ok := p.store.Get(ep)
	if !ok {
		t.Fatal("expected endpoint in store after ExtractData")
	}
	if stored.BlockHeight != 55000 {
		t.Errorf("expected stored height 55000, got %d", stored.BlockHeight)
	}
}

func TestExtractData_EmptyResponse(t *testing.T) {
	p := newPlugin(10)
	ep := endpointAddr("pokt1abc-https://node.example.com")

	_, err := p.ExtractData(ep, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty response")
	}
}

func TestExtractData_NoBlockHeight_ReturnsEmptyData(t *testing.T) {
	p := newPlugin(10)
	ep := endpointAddr("pokt1abc-https://node.example.com")
	resp := []byte(`{"result":{"some_other_field":"value"}}`)

	data, err := p.ExtractData(ep, nil, resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.BlockHeight != nil {
		t.Errorf("expected nil BlockHeight for response without height, got %d", *data.BlockHeight)
	}
}

// --- BlockHeightTracker tests --- //

func TestUpdateBlockHeight_UpdatesPerceivedHeight(t *testing.T) {
	p := newPlugin(10)

	ep1 := endpointAddr("pokt1a-https://ep1.example.com")
	ep2 := endpointAddr("pokt1b-https://ep2.example.com")
	ep3 := endpointAddr("pokt1c-https://ep3.example.com")

	p.UpdateBlockHeight(ep1, 1000)
	p.UpdateBlockHeight(ep2, 1001)
	p.UpdateBlockHeight(ep3, 1002)

	perceived := p.PerceivedBlockHeight()
	if perceived == 0 {
		t.Error("expected non-zero perceived block height after updates")
	}
}

// --- LifecycleHooks tests --- //

func TestOnSessionChange_TouchesAddedEndpoints(t *testing.T) {
	p := newPlugin(10)
	ep := endpointAddr("pokt1abc-https://node.example.com")

	// Pre-populate so we can verify it isn't overwritten.
	p.UpdateBlockHeight(ep, 500)

	added := domain.EndpointAddrList{ep}
	p.OnSessionChange("cosmos", added, nil)

	stored, ok := p.store.Get(ep)
	if !ok {
		t.Fatal("endpoint should still be in store after OnSessionChange")
	}
	if stored.BlockHeight != 500 {
		t.Errorf("block height should be preserved, got %d", stored.BlockHeight)
	}
}

func TestOnEndpointDiscovered_CreatesStoreEntry(t *testing.T) {
	p := newPlugin(10)
	ep := endpointAddr("pokt1new-https://new.example.com")

	p.OnEndpointDiscovered("cosmos", ep)

	_, ok := p.store.Get(ep)
	if !ok {
		t.Error("expected endpoint to be in store after OnEndpointDiscovered")
	}
}

// --- StartSync smoke test --- //

func TestStartSync_DoesNotPanic(t *testing.T) {
	p := newPlugin(10)
	ctx, cancel := context.WithCancel(context.Background())
	p.StartSync(ctx)
	cancel() // trigger context cancellation
}

// --- Verify interface satisfaction --- //

func TestInterfaceSatisfaction(t *testing.T) {
	p := newPlugin(10)

	var _ qos.Plugin = p
	var _ qos.BlockHeightTracker = p
	var _ qos.HealthChecker = p
	var _ qos.DataExtractor = p
}

// --- chain ID assertion --- //

// statusResponse is a trimmed CometBFT /status body: the network name and the
// height arrive together, which is why one check can assert both.
func statusResponse(network string, height string) []byte {
	return []byte(`{"result":{"node_info":{"network":"` + network + `"},"sync_info":{"latest_block_height":"` + height + `"}}}`)
}

// Same failure as EVM's, and the reason cosmos could not be left out: an
// endpoint on the wrong Cosmos chain reports heights that are perfectly real —
// for that chain — so every height filter passes it.
func TestExtractData_ChainIDMismatchReportsWrongChain(t *testing.T) {
	p := newPluginWithChainID("cosmoshub-4")
	_, err := p.ExtractData("supplier1", nil, statusResponse("osmosis-1", "20000000"))
	if !errors.Is(err, qos.ErrWrongChain) {
		t.Fatalf("ExtractData err = %v, want qos.ErrWrongChain", err)
	}
}

func TestExtractData_ChainIDMatch(t *testing.T) {
	p := newPluginWithChainID("cosmoshub-4")
	data, err := p.ExtractData("supplier1", nil, statusResponse("cosmoshub-4", "20000000"))
	if err != nil {
		t.Fatalf("ExtractData: %v", err)
	}
	if data.ChainID == nil || *data.ChainID != "cosmoshub-4" {
		t.Errorf("ChainID = %v, want cosmoshub-4", data.ChainID)
	}
	if data.BlockHeight == nil || *data.BlockHeight != 20000000 {
		t.Errorf("BlockHeight = %v, want 20000000", data.BlockHeight)
	}
}

// A CometBFT network is a name, not an encoding — there is no padding or casing
// to see through, so near-misses are different chains and must be rejected.
// This is the deliberate difference from EVM, where 0x531 and 0x0531 match.
func TestExtractData_ChainIDComparesExactly(t *testing.T) {
	for _, reported := range []string{"cosmoshub-04", "COSMOSHUB-4", "cosmoshub-5", "cosmoshub", " cosmoshub-4"} {
		t.Run(reported, func(t *testing.T) {
			p := newPluginWithChainID("cosmoshub-4")
			_, err := p.ExtractData("supplier1", nil, statusResponse(reported, "20000000"))
			if !errors.Is(err, qos.ErrWrongChain) {
				t.Errorf("%q must not satisfy a cosmoshub-4 assertion; err = %v", reported, err)
			}
		})
	}
}

// Most responses are not /status and carry no network at all. Absent means
// "this response cannot tell us", which must stay distinct from "wrong chain"
// or every abci_query would eject its endpoint.
func TestExtractData_ResponseWithoutChainIDIsNotAMismatch(t *testing.T) {
	p := newPluginWithChainID("cosmoshub-4")
	data, err := p.ExtractData("supplier1", nil, []byte(`{"height":"20000000"}`))
	if err != nil {
		t.Fatalf("a response with no network must not assert: %v", err)
	}
	if data.ChainID != nil {
		t.Errorf("ChainID = %v, want nil when the response carries none", *data.ChainID)
	}
}

// Empty chain_id is the zero value: services that do not opt in are unchanged.
func TestExtractData_NoChainIDConfiguredSkipsAssertion(t *testing.T) {
	p := newPlugin(100)
	if _, err := p.ExtractData("supplier1", nil, statusResponse("osmosis-1", "20000000")); err != nil {
		t.Errorf("no chain_id configured must not assert, got: %v", err)
	}
}

// A wrong-chain endpoint's heights are real for its own chain, so letting them
// reach consensus would skew the perceived head that the height filters
// compare against — the assertion has to come first.
func TestExtractData_WrongChainHeightNeverReachesConsensus(t *testing.T) {
	p := newPluginWithChainID("cosmoshub-4")
	_, _ = p.ExtractData("liar", nil, statusResponse("osmosis-1", "999999999"))

	if got := p.PerceivedBlockHeight(); got != 0 {
		t.Errorf("perceived = %d, want 0 — a wrong-chain height must not be recorded", got)
	}
}

func TestConfig_Validate(t *testing.T) {
	cases := []struct {
		chainID string
		wantErr bool
	}{
		{"", false},            // opt-out
		{"cosmoshub-4", false}, // canonical
		{"osmosis-1", false},   //
		{"pocket-beta", false}, // no name-revision convention required
		{"0x1", false},         // odd for cosmos, but not ours to reject
		{" cosmoshub-4", true}, // invisible in YAML, would never match
		{"cosmoshub-4 ", true}, //
		{"  ", true},           //
	}
	for _, tc := range cases {
		t.Run(tc.chainID, func(t *testing.T) {
			err := Config{ExpectedChainID: tc.chainID}.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("Validate(%q) = nil, want error", tc.chainID)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Validate(%q) = %v, want nil", tc.chainID, err)
			}
		})
	}
}

func TestParseRequest_GRPC(t *testing.T) {
	body := []byte{0, 0, 0, 0, 0}
	req, err := http.NewRequest(http.MethodPost, "http://gw/cosmos.bank.v1beta1.Query/Params", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/grpc-web+proto")

	payload, err := parseRequest(req, body, domain.RPCTypeGRPC)
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}

	if payload.RPCType() != domain.RPCTypeGRPC {
		t.Errorf("RPCType = %q, want %q", payload.RPCType(), domain.RPCTypeGRPC)
	}
	// The path IS the request for gRPC; losing it relays to the backend root.
	if payload.Path() != "/cosmos.bank.v1beta1.Query/Params" {
		t.Errorf("Path = %q", payload.Path())
	}
	// The backend is a native gRPC server whatever framing the client used —
	// for a unary call the request body is identical between the two.
	if payload.ContentType() != "application/grpc" {
		t.Errorf("ContentType = %q, want application/grpc", payload.ContentType())
	}
	if payload.Method() != "Query/Params" {
		t.Errorf("Method = %q, want Query/Params", payload.Method())
	}
}

// "/cosmos.bank..." is a gRPC method path; "/cosmos/bank..." is a REST path.
// One dot apart, and the REST branch would otherwise claim both.
func TestParseRequest_GRPCPathIsNotMistakenForREST(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://gw/cosmos/bank/v1beta1/params", nil)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := parseRequest(req, nil, domain.RPCTypeREST)
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if payload.RPCType() != domain.RPCTypeREST {
		t.Errorf("RPCType = %q, want %q", payload.RPCType(), domain.RPCTypeREST)
	}
}

func TestGRPCMethodFromPath(t *testing.T) {
	tests := []struct{ path, want string }{
		{"/cosmos.bank.v1beta1.Query/Params", "Query/Params"},
		{"/pocket.service.RelayService/SendRelay", "RelayService/SendRelay"},
		{"/NoPackage/Method", "NoPackage/Method"},
		{"/malformed", ""},
		{"/trailing/", ""},
		{"", ""},
	}
	for _, tt := range tests {
		if got := grpcMethodFromPath(tt.path); got != tt.want {
			t.Errorf("grpcMethodFromPath(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// An unset sync_allowance must not become a strict `height >= perceived` test.
// Perceived is a max over observations, so a strict test admits only the
// endpoint that reported last and starves the rest — the shape of PATH's
// 2026-08-18 Solana incident, on the plugin that fronts most cosmos services.
func TestSelectEndpoints_UnsetAllowanceDoesNotRequireTheTip(t *testing.T) {
	p := newPlugin(0)

	const tip = 1_000_000
	leader := domain.EndpointAddr("pokt1a-https://a.example.com")
	trailing := domain.EndpointAddr("pokt1b-https://b.example.com")

	p.UpdateBlockHeight(leader, tip)
	p.UpdateBlockHeight(trailing, tip-1)

	got, err := p.SelectEndpoints(domain.EndpointAddrList{leader, trailing}, nil)
	if err != nil {
		t.Fatalf("SelectEndpoints: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("selected %v — an endpoint one block behind was dropped with no allowance configured", got)
	}
}

// TestResetState pins what an operator-triggered chain-state reset discards:
// the perceived block height and the per-endpoint QoS store.
func TestResetState(t *testing.T) {
	p := newPlugin(5)

	addrs := domain.EndpointAddrList{"a", "b"}
	p.UpdateBlockHeight("a", 100)
	p.UpdateBlockHeight("b", 10) // far behind; would be filtered pre-reset

	if got := p.PerceivedBlockHeight(); got == 0 {
		t.Fatalf("expected nonzero perceived block height before reset, got %d", got)
	}

	p.ResetState()

	if got := p.PerceivedBlockHeight(); got != 0 {
		t.Fatalf("PerceivedBlockHeight() = %d after ResetState, want 0", got)
	}

	selected, err := p.SelectEndpoints(addrs, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != len(addrs) {
		t.Fatalf("SelectEndpoints after ResetState = %v, want every endpoint to pass (%v)", selected, addrs)
	}
}
