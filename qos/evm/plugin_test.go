package evm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// --- helpers ---

func newTestPlugin(syncAllowance uint64) *Plugin {
	return NewPlugin(nil, Config{SyncAllowance: syncAllowance})
}

// newTestPluginWithChainID builds a plugin that asserts the given chain ID.
func newTestPluginWithChainID(chainID string) *Plugin {
	return NewPlugin(nil, Config{SyncAllowance: 100, ExpectedChainID: chainID})
}

func makeRequest(body string) *http.Request {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", bytes.NewBufferString(body))
	return req
}

// --- ParseRequest ---

func TestParseRequest_ValidSingle(t *testing.T) {
	p := newTestPlugin(5)
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	payloads, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	if payloads[0].Method() != "eth_blockNumber" {
		t.Fatalf("expected method eth_blockNumber, got %q", payloads[0].Method())
	}
}

func TestParseRequest_Batch(t *testing.T) {
	p := newTestPlugin(5)
	body := `[
		{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1},
		{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}
	]`
	payloads, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 2 {
		t.Fatalf("expected 2 payloads, got %d", len(payloads))
	}
	if payloads[0].Method() != "eth_blockNumber" {
		t.Fatalf("expected eth_blockNumber, got %q", payloads[0].Method())
	}
	if payloads[1].Method() != "eth_chainId" {
		t.Fatalf("expected eth_chainId, got %q", payloads[1].Method())
	}
}

func TestParseRequest_InvalidJSON(t *testing.T) {
	p := newTestPlugin(5)
	_, err := p.ParseRequest(context.Background(), makeRequest("not json"), []byte("not json"), domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseRequest_MissingMethod(t *testing.T) {
	p := newTestPlugin(5)
	body := `{"jsonrpc":"2.0","params":[],"id":1}`
	_, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for missing method")
	}
}

func TestParseRequest_MissingJsonrpcField(t *testing.T) {
	p := newTestPlugin(5)
	body := `{"method":"eth_blockNumber","params":[],"id":1}`
	_, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for missing jsonrpc field")
	}
}

func TestParseRequest_WrongVersion(t *testing.T) {
	p := newTestPlugin(5)
	body := `{"jsonrpc":"1.0","method":"eth_blockNumber","params":[],"id":1}`
	_, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for wrong jsonrpc version")
	}
}

func TestParseRequest_UnsupportedRPCType(t *testing.T) {
	p := newTestPlugin(5)
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	_, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeREST)
	if err == nil {
		t.Fatal("expected error for unsupported rpc type")
	}
}

func TestParseRequest_WebSocketAllowed(t *testing.T) {
	p := newTestPlugin(5)
	body := `{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":1}`
	payloads, err := p.ParseRequest(context.Background(), makeRequest(body), []byte(body), domain.RPCTypeWebSocket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
}

func TestParseRequest_EmptyBody(t *testing.T) {
	p := newTestPlugin(5)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	_, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for nil body")
	}
}

func TestParseRequest_EmptyBatch(t *testing.T) {
	p := newTestPlugin(5)
	_, err := p.ParseRequest(context.Background(), makeRequest(`[]`), []byte(`[]`), domain.RPCTypeJSONRPC)
	if err == nil {
		t.Fatal("expected error for empty batch")
	}
}

// --- SelectEndpoints ---

func TestSelectEndpoints_BlockHeightFiltering(t *testing.T) {
	p := newTestPlugin(5) // sync allowance = 5

	// Simulate perceived block = 100 via observations.
	addrs := domain.EndpointAddrList{"a", "b", "c"}
	p.UpdateBlockHeight("a", 100) // ok (perceived)
	p.UpdateBlockHeight("b", 94)  // too low (100 - 5 = 95, 94 < 95)
	p.UpdateBlockHeight("c", 95)  // borderline ok

	payloads := []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}

	selected, err := p.SelectEndpoints(addrs, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, addr := range selected {
		if addr == "b" {
			t.Fatal("endpoint b should have been filtered (block height too low)")
		}
	}
	if len(selected) < 2 {
		t.Fatalf("expected at least 2 endpoints (a and c), got %d: %v", len(selected), selected)
	}
}

func TestSelectEndpoints_ZeroPerceivedAllowsAll(t *testing.T) {
	p := newTestPlugin(5)
	// No UpdateBlockHeight calls — perceived is 0, so all pass.
	addrs := domain.EndpointAddrList{"a", "b"}
	payloads := []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}
	selected, err := p.SelectEndpoints(addrs, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(selected))
	}
}

func TestSelectEndpoints_ArchivalFiltering(t *testing.T) {
	p := newTestPlugin(5)

	// Two endpoints; "nonarchival" has told us it does not retain the state.
	p.UpdateBlockHeight("archival", 100)
	p.UpdateBlockHeight("nonarchival", 100)
	p.store.Update("archival", func(ep *evmEndpoint) {
		ep.IsArchival = true
		ep.ArchivalExpiry = time.Now().Add(archivalTTL)
	})
	p.store.Update("nonarchival", func(ep *evmEndpoint) {
		ep.IsArchival = false
		ep.ArchivalExpiry = time.Now().Add(archivalTTL)
	})

	addrs := domain.EndpointAddrList{"archival", "nonarchival"}
	// Archival request: eth_getBalance at a specific historical block.
	body := `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","0x1"],"id":1}`
	payloads := []domain.Payload{
		domain.NewPayload([]byte(body), domain.RPCTypeJSONRPC, "eth_getBalance"),
	}

	selected, err := p.SelectEndpoints(addrs, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 1 || selected[0] != "archival" {
		t.Fatalf("expected only archival endpoint, got %v", selected)
	}
}

func TestSelectEndpoints_EmptyInput(t *testing.T) {
	p := newTestPlugin(5)
	result, err := p.SelectEndpoints(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

// --- ParseBlockHeight (BlockHeightParser) ---

func TestParseBlockHeight_Valid(t *testing.T) {
	p := newTestPlugin(5)
	cases := []struct {
		response string
		expected uint64
	}{
		{`{"jsonrpc":"2.0","id":1,"result":"0x1"}`, 1},
		{`{"jsonrpc":"2.0","id":1,"result":"0x0"}`, 0},
		{`{"jsonrpc":"2.0","id":1,"result":"0xffffffff"}`, 0xffffffff},
		{`{"jsonrpc":"2.0","id":1,"result":"0x1194af2"}`, 0x1194af2},
	}
	for _, tc := range cases {
		got, err := p.ParseBlockHeight([]byte(tc.response))
		if err != nil && tc.expected != 0 {
			t.Errorf("unexpected error for %q: %v", tc.response, err)
			continue
		}
		if got != tc.expected {
			t.Errorf("response %q: expected %d, got %d", tc.response, tc.expected, got)
		}
	}
}

func TestParseBlockHeight_Invalid(t *testing.T) {
	p := newTestPlugin(5)
	cases := []string{
		`{"jsonrpc":"2.0","id":1,"result":100}`,      // number, not string
		`{"jsonrpc":"2.0","id":1,"result":{"a":1}}`,  // object
		`{"jsonrpc":"2.0","id":1}`,                   // missing result
		`{"jsonrpc":"2.0","id":1,"result":"notHex"}`, // not hex
	}
	for _, tc := range cases {
		_, err := p.ParseBlockHeight([]byte(tc))
		if err == nil {
			t.Errorf("expected error for %q", tc)
		}
	}
}

// --- parseHexUint64 ---

func TestParseHexUint64(t *testing.T) {
	cases := []struct {
		input   string
		want    uint64
		wantErr bool
	}{
		{"0x0", 0, false},
		{"0x1", 1, false},
		{"0xff", 255, false},
		{"0xffffffffffffffff", ^uint64(0), false},
		{"0x1194af2", 0x1194af2, false},
		{"0X1a", 26, false}, // uppercase X
		{"", 0, true},
		{"abc", 0, true},    // no prefix
		{"0x", 0, true},     // empty after prefix
		{"0xGGGG", 0, true}, // invalid hex chars
	}
	for _, tc := range cases {
		got, err := parseHexUint64(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseHexUint64(%q): expected error", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("parseHexUint64(%q): unexpected error: %v", tc.input, err)
			} else if got != tc.want {
				t.Errorf("parseHexUint64(%q): got %d, want %d", tc.input, got, tc.want)
			}
		}
	}
}

// --- IsCoalescable ---

func TestIsCoalescable_ReadOnly(t *testing.T) {
	p := newTestPlugin(5)
	readOnly := []string{
		"eth_blockNumber",
		"eth_chainId",
		"eth_gasPrice",
		"eth_getBalance",
		"eth_getBlockByNumber",
		"eth_getTransactionByHash",
		"eth_getTransactionReceipt",
		"net_version",
		"web3_clientVersion",
	}
	for _, m := range readOnly {
		if !p.IsCoalescable(m) {
			t.Errorf("expected %q to be coalescable", m)
		}
	}
}

func TestIsCoalescable_Mutations(t *testing.T) {
	p := newTestPlugin(5)
	mutations := []string{
		"eth_sendRawTransaction",
		"eth_sendTransaction",
		"eth_signTransaction",
		"eth_sign",
		"personal_sign",
		"eth_accounts", // not in our list
	}
	for _, m := range mutations {
		if p.IsCoalescable(m) {
			t.Errorf("expected %q to NOT be coalescable", m)
		}
	}
}

// --- CacheTTL ---

func TestCacheTTL(t *testing.T) {
	p := newTestPlugin(5)

	// eth_blockNumber: no cache.
	if ttl := p.CacheTTL("eth_blockNumber", nil, nil); ttl != 0 {
		t.Errorf("eth_blockNumber: expected 0, got %v", ttl)
	}

	// eth_getTransactionReceipt: 5 min.
	if ttl := p.CacheTTL("eth_getTransactionReceipt", nil, nil); ttl != 5*time.Minute {
		t.Errorf("eth_getTransactionReceipt: expected 5m, got %v", ttl)
	}

	// eth_getBlockByNumber with specific block: 10 min.
	params := json.RawMessage(`["0x1194af2",true]`)
	if ttl := p.CacheTTL("eth_getBlockByNumber", params, nil); ttl != 10*time.Minute {
		t.Errorf("eth_getBlockByNumber(hex block): expected 10m, got %v", ttl)
	}

	// eth_getBlockByNumber with "latest": no cache.
	latestParams := json.RawMessage(`["latest",true]`)
	if ttl := p.CacheTTL("eth_getBlockByNumber", latestParams, nil); ttl != 0 {
		t.Errorf("eth_getBlockByNumber(latest): expected 0, got %v", ttl)
	}

	// Mutations: no cache.
	for _, m := range []string{"eth_sendRawTransaction", "eth_sendTransaction"} {
		if ttl := p.CacheTTL(m, nil, nil); ttl != 0 {
			t.Errorf("%s: expected 0, got %v", m, ttl)
		}
	}
}

// --- ValidateResponseFormat ---

func TestValidateResponseFormat_HexString(t *testing.T) {
	p := newTestPlugin(5)

	validCases := []struct {
		method string
		result string
	}{
		{"eth_blockNumber", `"0x1194af2"`},
		{"eth_chainId", `"0x1"`},
		{"eth_gasPrice", `"0x3b9aca00"`},
	}
	for _, tc := range validCases {
		if err := p.ValidateResponseFormat(tc.method, json.RawMessage(tc.result)); err != nil {
			t.Errorf("%s valid hex: unexpected error: %v", tc.method, err)
		}
	}

	invalidCases := []struct {
		method string
		result string
	}{
		{"eth_blockNumber", `100`},             // number
		{"eth_blockNumber", `{"value":"0x1"}`}, // object
		{"eth_blockNumber", `["0x1"]`},         // array
		{"eth_blockNumber", `"notHex"`},        // non-hex string
		{"eth_chainId", `null`},                // null
	}
	for _, tc := range invalidCases {
		if err := p.ValidateResponseFormat(tc.method, json.RawMessage(tc.result)); err == nil {
			t.Errorf("%s invalid %q: expected error", tc.method, tc.result)
		}
	}
}

func TestValidateResponseFormat_UnknownMethod(t *testing.T) {
	p := newTestPlugin(5)
	// Unknown methods should not error (plugin doesn't know the shape).
	if err := p.ValidateResponseFormat("eth_getLogs", json.RawMessage(`[{"blockNumber":"0x1"}]`)); err != nil {
		t.Fatalf("unexpected error for unknown method: %v", err)
	}
}

// --- Archival request detection ---

func TestIsArchivalRequest_Archival(t *testing.T) {
	p := newTestPlugin(5)

	cases := []struct {
		method string
		body   string
	}{
		{
			method: "eth_getBalance",
			body:   `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","0x1"],"id":1}`,
		},
		{
			method: "eth_getCode",
			body:   `{"jsonrpc":"2.0","method":"eth_getCode","params":["0xabc","0x100"],"id":1}`,
		},
		{
			method: "eth_call",
			body:   `{"jsonrpc":"2.0","method":"eth_call","params":[{},"0xdeadbeef"],"id":1}`,
		},
		{
			method: "eth_getBlockByNumber",
			body:   `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1194af2",true],"id":1}`,
		},
	}

	for _, tc := range cases {
		payloads := []domain.Payload{domain.NewPayload([]byte(tc.body), domain.RPCTypeJSONRPC, tc.method)}
		if !p.IsArchivalRequest(payloads) {
			t.Errorf("%s with specific block: expected archival", tc.method)
		}
	}
}

func TestIsArchivalRequest_NotArchival(t *testing.T) {
	p := newTestPlugin(5)

	cases := []struct {
		method string
		body   string
	}{
		{
			method: "eth_getBalance",
			body:   `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","latest"],"id":1}`,
		},
		{
			method: "eth_getBalance",
			body:   `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","pending"],"id":1}`,
		},
		{
			method: "eth_getBalance",
			body:   `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","finalized"],"id":1}`,
		},
		{
			// eth_blockNumber has no block parameter — never archival
			method: "eth_blockNumber",
			body:   `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`,
		},
		{
			method: "eth_getTransactionByHash",
			body:   `{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["0xabc"],"id":1}`,
		},
	}

	for _, tc := range cases {
		payloads := []domain.Payload{domain.NewPayload([]byte(tc.body), domain.RPCTypeJSONRPC, tc.method)}
		if p.IsArchivalRequest(payloads) {
			t.Errorf("%s: expected NOT archival", tc.method)
		}
	}
}

// --- HealthChecks ---

func TestHealthChecks(t *testing.T) {
	p := newTestPlugin(5)
	checks := p.HealthChecks("ep1")
	if len(checks) != 2 {
		t.Fatalf("expected 2 health checks, got %d", len(checks))
	}
	names := map[string]bool{}
	for _, c := range checks {
		names[c.Name] = true
		if len(c.Payload.Bytes()) == 0 {
			t.Errorf("health check %q has empty payload", c.Name)
		}
	}
	if !names["eth_blockNumber"] {
		t.Error("missing eth_blockNumber health check")
	}
	if !names["eth_chainId"] {
		t.Error("missing eth_chainId health check")
	}
}

// --- ExtractData ---

func TestExtractData_BlockNumber(t *testing.T) {
	p := newTestPlugin(5)
	request := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	response := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`)

	data, err := p.ExtractData("ep1", request, response)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data == nil || data.BlockHeight == nil {
		t.Fatal("expected BlockHeight in ExtractedData")
	}
	if *data.BlockHeight != 100 {
		t.Fatalf("expected 100, got %d", *data.BlockHeight)
	}
	// Verify store updated.
	ep, ok := p.store.Get("ep1")
	if !ok || ep.BlockNumber != 100 {
		t.Fatalf("store not updated: found=%v block=%d", ok, ep.BlockNumber)
	}
}

func TestExtractData_ChainID(t *testing.T) {
	p := newTestPlugin(5)
	request := []byte(`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}`)
	response := []byte(`{"jsonrpc":"2.0","id":2,"result":"0x1"}`)

	data, err := p.ExtractData("ep1", request, response)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data == nil || data.ChainID == nil {
		t.Fatal("expected ChainID in ExtractedData")
	}
	if *data.ChainID != "0x1" {
		t.Fatalf("expected 0x1, got %q", *data.ChainID)
	}
}

func TestExtractData_UnknownMethod(t *testing.T) {
	p := newTestPlugin(5)
	data, err := p.ExtractData("ep1",
		[]byte(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[],"id":3}`),
		[]byte(`{"jsonrpc":"2.0","id":3,"result":[]}`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil for unknown method, got %+v", data)
	}
}

// --- Interface compliance (compile-time) ---

var (
	_ qos.Plugin                  = (*Plugin)(nil)
	_ qos.BlockHeightTracker      = (*Plugin)(nil)
	_ qos.BlockHeightParser       = (*Plugin)(nil)
	_ qos.ArchivalDetector        = (*Plugin)(nil)
	_ qos.HealthChecker           = (*Plugin)(nil)
	_ qos.DataExtractor           = (*Plugin)(nil)
	_ qos.CoalescenceClassifier   = (*Plugin)(nil)
	_ qos.CachePolicy             = (*Plugin)(nil)
	_ qos.ResponseFormatValidator = (*Plugin)(nil)
	_ qos.LifecycleHooks          = (*Plugin)(nil)
)

// --- chain ID assertion ---

// chainIDRequest is the health check body the plugin dispatches for eth_chainId.
var chainIDRequest = []byte(`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}`)

func chainIDResponse(result string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":2,"result":"` + result + `"}`)
}

// THE case this whole assertion exists for. An endpoint serving a different
// chain answers honestly and passes every other check — its block heights are
// real, just for the wrong chain, so they sail through the height filters.
// Without this, it stays in rotation at full reputation serving foreign data.
func TestExtractData_ChainIDMismatchReportsWrongChain(t *testing.T) {
	p := newTestPluginWithChainID("0x1")                                          // eth mainnet
	_, err := p.ExtractData("supplier1", chainIDRequest, chainIDResponse("0x89")) // polygon
	if !errors.Is(err, qos.ErrWrongChain) {
		t.Fatalf("ExtractData err = %v, want qos.ErrWrongChain", err)
	}
}

func TestExtractData_ChainIDMatch(t *testing.T) {
	p := newTestPluginWithChainID("0x1")
	data, err := p.ExtractData("supplier1", chainIDRequest, chainIDResponse("0x1"))
	if err != nil {
		t.Fatalf("ExtractData: %v", err)
	}
	if data == nil || data.ChainID == nil {
		t.Fatal("expected extracted chain ID")
	}
	if *data.ChainID != "0x1" {
		t.Errorf("chain ID = %q, want %q", *data.ChainID, "0x1")
	}
}

// Endpoints format the same chain ID differently — zero-padded, upper-case 0X.
// Comparing as text would eject honest endpoints over formatting, so the
// comparison parses both sides.
func TestExtractData_ChainIDFormatVarianceStillMatches(t *testing.T) {
	cases := []struct{ expected, reported string }{
		{"0x531", "0x0531"},
		{"0x0531", "0x531"},
		{"0x531", "0X531"},
		{"0x1", "0x01"},
		{"0x2b6653dc", "0x2B6653DC"},
	}
	for _, tc := range cases {
		t.Run(tc.expected+"/"+tc.reported, func(t *testing.T) {
			p := newTestPluginWithChainID(tc.expected)
			if _, err := p.ExtractData("supplier1", chainIDRequest, chainIDResponse(tc.reported)); err != nil {
				t.Errorf("expected %s to match %s, got err: %v", tc.reported, tc.expected, err)
			}
		})
	}
}

// PATH asserts chain IDs by substring, which is why its rules file carries a
// trailing-quote hack ('0x1"'): a bare "0x1" also matches "0x1388". Parsing
// both sides means the trap has nowhere to live — prove it.
func TestExtractData_ChainIDIsNotASubstringMatch(t *testing.T) {
	p := newTestPluginWithChainID("0x1")
	_, err := p.ExtractData("supplier1", chainIDRequest, chainIDResponse("0x1388"))
	if !errors.Is(err, qos.ErrWrongChain) {
		t.Fatalf("0x1388 must not satisfy an 0x1 assertion; err = %v", err)
	}
}

// Empty chain_id is the zero value: every service that does not opt in keeps
// behaving exactly as before.
func TestExtractData_NoChainIDConfiguredSkipsAssertion(t *testing.T) {
	p := newTestPlugin(100)
	if _, err := p.ExtractData("supplier1", chainIDRequest, chainIDResponse("0x89")); err != nil {
		t.Errorf("no chain_id configured must not assert, got err: %v", err)
	}
}

// The reported chain is recorded even when it is wrong, so an operator can see
// which chain the endpoint was actually serving rather than only that it failed.
func TestExtractData_MismatchStillRecordsReportedChainID(t *testing.T) {
	p := newTestPluginWithChainID("0x1")
	_, _ = p.ExtractData("supplier1", chainIDRequest, chainIDResponse("0x89"))

	ep, ok := p.store.Get("supplier1")
	if !ok {
		t.Fatal("expected endpoint state to be recorded")
	}
	if ep.ChainID != "0x89" {
		t.Errorf("recorded chain ID = %q, want %q", ep.ChainID, "0x89")
	}
}

// A malformed chain ID is an extraction failure, not a wrong chain — the two
// grade differently upstream, so they must stay distinguishable.
func TestExtractData_MalformedChainIDIsNotWrongChain(t *testing.T) {
	p := newTestPluginWithChainID("0x1")
	_, err := p.ExtractData("supplier1", chainIDRequest, chainIDResponse("nonsense"))
	if err == nil {
		t.Fatal("expected an error for a malformed chain ID")
	}
	if errors.Is(err, qos.ErrWrongChain) {
		t.Errorf("malformed chain ID must not report ErrWrongChain, got: %v", err)
	}
}

// A chain ID that can never match would eject every endpoint of the service, so
// a malformed one has to fail at startup rather than at 3am. The rule lives here
// and not in config: hex is an EVM fact, and cosmos chain IDs look like
// "cosmoshub-4" — config carries the value opaquely.
func TestConfig_Validate(t *testing.T) {
	cases := []struct {
		chainID string
		wantErr bool
	}{
		{"", false},           // opt-out: no assertion
		{"0x1", false},        // eth mainnet
		{"0x531", false},      // sei
		{"0X531", false},      // upper-case prefix is legal hex
		{"0x2b6653dc", false}, // tron
		{"1", true},           // decimal, no 0x prefix
		{"0x", true},          // prefix with no digits
		{"0xzz", true},        // not hex
		{"eth", true},         // a service name, not a chain ID
		{"cosmoshub-4", true}, // a valid chain ID — but not for an EVM service
	}

	for _, tc := range cases {
		t.Run(tc.chainID, func(t *testing.T) {
			err := Config{SyncAllowance: 100, ExpectedChainID: tc.chainID}.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("Validate(%q) = nil, want error", tc.chainID)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Validate(%q) = %v, want nil", tc.chainID, err)
			}
		})
	}
}

// TestSelectEndpoints_ArchivalUnobservedNotExcluded pins the third state.
//
// Archival status is inferred from traffic that happened to name a historical
// block, so at any moment most endpoints carry no observation at all. Requiring
// proof of archival before serving an archival request excluded every one of
// them — which is why this pairs an unobserved endpoint with a known-pruned
// one. Excluding both is indistinguishable from excluding neither at the
// selection boundary: qos.Select exhausts all three tiers and hands back the
// unfiltered list, so the old filter cost a warning per request and changed
// nothing. Only a set of exactly one discriminates.
func TestSelectEndpoints_ArchivalUnobservedNotExcluded(t *testing.T) {
	p := newTestPlugin(5)

	p.UpdateBlockHeight("never-asked", 100)
	p.UpdateBlockHeight("known-pruned", 100)
	p.store.Update("known-pruned", func(ep *evmEndpoint) {
		ep.IsArchival = false
		ep.ArchivalExpiry = time.Now().Add(archivalTTL)
	})

	addrs := domain.EndpointAddrList{"never-asked", "known-pruned"}
	body := `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","0x1"],"id":1}`
	payloads := []domain.Payload{
		domain.NewPayload([]byte(body), domain.RPCTypeJSONRPC, "eth_getBalance"),
	}

	selected, err := p.SelectEndpoints(addrs, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 1 || selected[0] != "never-asked" {
		t.Fatalf("expected only the unobserved endpoint, got %v", selected)
	}
}

// TestSelectEndpoints_ArchivalObservationExpires checks that a negative ages
// back into "unknown" rather than becoming permanent: a node that pruned in
// April may be serving full history today, and nothing re-probes it if the
// filter never lets an archival request reach it again.
//
// Paired with a fresh negative for the same reason as above — a result of two
// would mean the filter excluded everything and the fallback returned the lot.
func TestSelectEndpoints_ArchivalObservationExpires(t *testing.T) {
	p := newTestPlugin(5)

	p.UpdateBlockHeight("stale-negative", 100)
	p.UpdateBlockHeight("fresh-negative", 100)
	p.store.Update("stale-negative", func(ep *evmEndpoint) {
		ep.IsArchival = false
		ep.ArchivalExpiry = time.Now().Add(-time.Minute)
	})
	p.store.Update("fresh-negative", func(ep *evmEndpoint) {
		ep.IsArchival = false
		ep.ArchivalExpiry = time.Now().Add(archivalTTL)
	})

	addrs := domain.EndpointAddrList{"stale-negative", "fresh-negative"}
	body := `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","0x1"],"id":1}`
	payloads := []domain.Payload{
		domain.NewPayload([]byte(body), domain.RPCTypeJSONRPC, "eth_getBalance"),
	}

	selected, err := p.SelectEndpoints(addrs, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 1 || selected[0] != "stale-negative" {
		t.Fatalf("expected the expired observation to read as unknown, got %v", selected)
	}
	if p.IsArchivalEndpoint("stale-negative") {
		t.Fatal("expired observation must not read as archival either")
	}
}

// TestExtractData_ArchivalInference drives the inference through ExtractData,
// the entry point the observation pipeline calls, rather than through
// observeArchival directly — the method/params gate is half of what is being
// tested and a hand-built call skips it.
func TestExtractData_ArchivalInference(t *testing.T) {
	tests := []struct {
		name         string
		request      string
		response     string
		wantObserved bool
		wantArchival bool
	}{
		{
			name:         "historical block served",
			request:      `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","0x1"],"id":1}`,
			response:     `{"jsonrpc":"2.0","id":1,"result":"0x2386f26fc10000"}`,
			wantObserved: true,
			wantArchival: true,
		},
		{
			name:         "geth PBSS pruned state",
			request:      `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","0x1"],"id":1}`,
			response:     `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"metadata is not found, 0x1"}}`,
			wantObserved: true,
			wantArchival: false,
		},
		{
			name:         "gnosis historical state wording",
			request:      `{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0xabc"},"0x64"],"id":1}`,
			response:     `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"historical state is not available"}}`,
			wantObserved: true,
			wantArchival: false,
		},
		{
			// A pruned node answers this perfectly, which is why the block
			// parameter and not the method decides.
			name:         "latest is not an archival probe",
			request:      `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xabc","latest"],"id":1}`,
			response:     `{"jsonrpc":"2.0","id":1,"result":"0x2386f26fc10000"}`,
			wantObserved: false,
		},
		{
			// A revert says the contract disagreed, not that the state is gone.
			name:         "unrelated error is inconclusive",
			request:      `{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0xabc"},"0x64"],"id":1}`,
			response:     `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`,
			wantObserved: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPlugin(5)
			data, err := p.ExtractData("ep", []byte(tt.request), []byte(tt.response))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !tt.wantObserved {
				if data != nil && data.IsArchival != nil {
					t.Fatalf("expected no archival observation, got %v", *data.IsArchival)
				}
				if _, ok := p.store.Get("ep"); ok {
					t.Fatal("inconclusive response must not write endpoint state")
				}
				return
			}

			if data == nil || data.IsArchival == nil {
				t.Fatal("expected an archival observation")
			}
			if *data.IsArchival != tt.wantArchival {
				t.Fatalf("archival = %v, want %v", *data.IsArchival, tt.wantArchival)
			}
			if got := p.IsArchivalEndpoint("ep"); got != tt.wantArchival {
				t.Fatalf("IsArchivalEndpoint = %v, want %v", got, tt.wantArchival)
			}
		})
	}
}

// TestIsArchivalRequest_Earliest pins genesis as archival. It is a tag, so the
// hex parse below it never sees it; treating it like "latest" made the deepest
// query on the chain read as the shallowest.
func TestIsArchivalRequest_Earliest(t *testing.T) {
	if !isArchivalRequest("eth_getBalance", []byte(`["0xabc","earliest"]`)) {
		t.Fatal("earliest must count as archival")
	}
	if isArchivalRequest("eth_getBalance", []byte(`["0xabc","latest"]`)) {
		t.Fatal("latest must not count as archival")
	}
}

// TestResetState pins what an operator-triggered chain-state reset discards:
// the perceived block height and the per-endpoint QoS store. It is the
// revert-check for ResetState — make it a no-op and this fails, because
// PerceivedBlockHeight would still read the pre-reset height and the stale
// endpoint would still be filtered out of SelectEndpoints.
func TestResetState(t *testing.T) {
	p := newTestPlugin(5)

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

	payloads := []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}
	selected, err := p.SelectEndpoints(addrs, payloads)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != len(addrs) {
		t.Fatalf("SelectEndpoints after ResetState = %v, want every endpoint to pass (%v)", selected, addrs)
	}
}

// The chain id cannot legitimately change, so asking for it every cycle buys
// nothing after the first answer. It keeps a slow cadence of its own; the
// block number stays on the service's.
func TestHealthChecks_ChainIDIsSlowCadence(t *testing.T) {
	p := newTestPlugin(100)
	for _, c := range p.HealthChecks("supplier-https://node.example.com") {
		switch c.Name {
		case "eth_chainId":
			if c.Interval < 5*time.Minute {
				t.Errorf("eth_chainId interval = %v, want >= 5m", c.Interval)
			}
		case "eth_blockNumber":
			if c.Interval != 0 {
				t.Errorf("eth_blockNumber interval = %v, want 0 (service cadence)", c.Interval)
			}
		}
	}
}
