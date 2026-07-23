package noop_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos/noop"
)

// --- ParseRequest --- //

func TestParseRequest_PassesBodyThrough(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	body := []byte(`{"anything":"goes","even":"invalid json for other chains"}`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))

	payloads, err := p.ParseRequest(context.Background(), req, body, domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	if string(payloads[0].Bytes()) != string(body) {
		t.Errorf("payload bytes mismatch: got %q, want %q", payloads[0].Bytes(), body)
	}
}

func TestParseRequest_NilBodyAllowed(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	req, _ := http.NewRequest(http.MethodGet, "/status", nil)

	payloads, err := p.ParseRequest(context.Background(), req, nil, domain.RPCTypeREST)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	if payloads[0].Bytes() != nil {
		t.Error("expected nil bytes for nil body")
	}
	if payloads[0].RPCType() != domain.RPCTypeREST {
		t.Errorf("expected RPCType rest, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_PreservesRPCType(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	body := []byte(`some body`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))

	payloads, err := p.ParseRequest(context.Background(), req, body, domain.RPCTypeWebSocket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payloads[0].RPCType() != domain.RPCTypeWebSocket {
		t.Errorf("expected websocket, got %q", payloads[0].RPCType())
	}
}

func TestParseRequest_SplitsBatchArray(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	batch := []byte(`[
		{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1},
		{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2},
		{"jsonrpc":"2.0","method":"net_version","params":[],"id":3}
	]`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(batch))

	payloads, err := p.ParseRequest(context.Background(), req, batch, domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 3 {
		t.Fatalf("expected 3 payloads, got %d", len(payloads))
	}

	// Verify each payload is individual JSON, not the whole array.
	expectedMethods := []string{"eth_blockNumber", "eth_chainId", "net_version"}
	for i, p := range payloads {
		if p.Method() != expectedMethods[i] {
			t.Errorf("payload %d: expected method %q, got %q", i, expectedMethods[i], p.Method())
		}
		if p.RPCType() != domain.RPCTypeJSONRPC {
			t.Errorf("payload %d: expected rpc type json_rpc, got %q", i, p.RPCType())
		}
		if p.Bytes()[0] != '{' {
			t.Errorf("payload %d: expected individual JSON object, got %q", i, string(p.Bytes()[:20]))
		}
	}
}

func TestParseRequest_SingleElementArrayNotSplit(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	batch := []byte(`[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}]`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(batch))

	payloads, err := p.ParseRequest(context.Background(), req, batch, domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Single-element array: passed through as one payload (no split needed).
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload for single-element array, got %d", len(payloads))
	}
}

func TestParseRequest_NonJSONBodyNotSplit(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	body := []byte(`this is not json at all`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))

	payloads, err := p.ParseRequest(context.Background(), req, body, domain.RPCTypeREST)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload for non-JSON body, got %d", len(payloads))
	}
	if string(payloads[0].Bytes()) != string(body) {
		t.Errorf("expected body pass-through, got %q", payloads[0].Bytes())
	}
}

func TestParseRequest_BatchWithoutMethodField(t *testing.T) {
	// Generic chains may not have standard JSON-RPC "method" fields.
	p := noop.NewPlugin(nil, 0)
	batch := []byte(`[{"action":"get_block","height":100},{"action":"get_tx","hash":"0xabc"}]`)
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(batch))

	payloads, err := p.ParseRequest(context.Background(), req, batch, domain.RPCTypeJSONRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 2 {
		t.Fatalf("expected 2 payloads, got %d", len(payloads))
	}
	// Method should be empty since these aren't standard JSON-RPC.
	for i, p := range payloads {
		if p.Method() != "" {
			t.Errorf("payload %d: expected empty method for non-jsonrpc, got %q", i, p.Method())
		}
	}
}

// --- SelectEndpoints --- //

func TestSelectEndpoints_ReturnsAllEndpoints(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	ep1 := domain.EndpointAddr("s1-https://a.example.com")
	ep2 := domain.EndpointAddr("s2-https://b.example.com")
	ep3 := domain.EndpointAddr("s3-https://c.example.com")

	endpoints := domain.EndpointAddrList{ep1, ep2, ep3}
	result, err := p.SelectEndpoints(endpoints, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 endpoints, got %d", len(result))
	}
}

func TestSelectEndpoints_EmptyList(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	result, err := p.SelectEndpoints(domain.EndpointAddrList{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d", len(result))
	}
}

func TestSelectEndpoints_WithSyncAllowance_NoConsensus(t *testing.T) {
	// Even with sync allowance set, no consensus yet → all pass through.
	p := noop.NewPlugin(nil, 10)
	ep1 := domain.EndpointAddr("s1-https://a.example.com")
	ep2 := domain.EndpointAddr("s2-https://b.example.com")

	result, err := p.SelectEndpoints(domain.EndpointAddrList{ep1, ep2}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 endpoints before consensus, got %d", len(result))
	}
}

func TestSelectEndpoints_WithSyncAllowance_FiltersLaggingEndpoints(t *testing.T) {
	p := noop.NewPlugin(nil, 5)

	ep1 := domain.EndpointAddr("s1-https://a.example.com")
	ep2 := domain.EndpointAddr("s2-https://b.example.com")
	ep3 := domain.EndpointAddr("s3-https://c.example.com") // no data → allowed through

	// Build consensus: perceived ≈ 1000.
	p.UpdateBlockHeight(ep1, 1000)
	p.UpdateBlockHeight(ep2, 900) // 900 < 995 (1000-5) → should be filtered

	result, err := p.SelectEndpoints(domain.EndpointAddrList{ep1, ep2, ep3}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Contains(ep1) {
		t.Error("ep1 (height 1000) should be included")
	}
	if result.Contains(ep2) {
		t.Error("ep2 (height 900) should be filtered out")
	}
	if !result.Contains(ep3) {
		t.Error("ep3 (unknown) should be allowed through")
	}
}

// --- BlockHeightTracker --- //

func TestPerceivedBlockHeight_ZeroWithNoObservations(t *testing.T) {
	p := noop.NewPlugin(nil, 10)
	if h := p.PerceivedBlockHeight(); h != 0 {
		t.Errorf("expected 0 before any observations, got %d", h)
	}
}

func TestPerceivedBlockHeight_UpdatesWithObservations(t *testing.T) {
	p := noop.NewPlugin(nil, 10)
	p.UpdateBlockHeight("s1-https://a.example.com", 500)
	p.UpdateBlockHeight("s2-https://b.example.com", 510)

	if h := p.PerceivedBlockHeight(); h == 0 {
		t.Error("expected non-zero perceived height after updates")
	}
}

func TestStartSync_DoesNotPanic(t *testing.T) {
	p := noop.NewPlugin(nil, 0)
	p.StartSync(context.Background())
}
