package noop_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
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

// sync_allowance is accepted and ignored. It used to gate a block-height
// filter fed by an UpdateBlockHeight nothing ever called, so it read as live
// and decided nothing; the filter is gone and this pins that the knob cannot
// quietly come back to life.
func TestSelectEndpoints_SyncAllowanceChangesNothing(t *testing.T) {
	endpoints := domain.EndpointAddrList{"supA-https://a.example.com", "supB-https://b.example.com"}

	for _, allowance := range []uint64{0, 1, 100, 1_000_000} {
		p := noop.NewPlugin(nil, allowance)
		got, err := p.SelectEndpoints(endpoints, nil)
		if err != nil {
			t.Fatalf("sync_allowance %d: %v", allowance, err)
		}
		if len(got) != len(endpoints) {
			t.Errorf("sync_allowance %d returned %d of %d endpoints; the passthrough filters nothing",
				allowance, len(got), len(endpoints))
		}
	}
}

// The passthrough implements the core interface and none of the optional ones.
// Claiming a capability it cannot honour for an unknown chain is how
// sync_allowance came to look live: the executor and the observation handler
// both gate height tracking on DataExtractor, which this never implemented,
// so the tracker it did implement was unreachable.
func TestPlugin_ImplementsNoOptionalCapabilities(t *testing.T) {
	var p any = noop.NewPlugin(nil, 100)

	if _, ok := p.(qos.HealthChecker); ok {
		t.Error("declares HealthChecker: the passthrough cannot know what payload an unknown chain answers")
	}
	if _, ok := p.(qos.DataExtractor); ok {
		t.Error("declares DataExtractor: it cannot know where a height sits in a response it does not understand")
	}
	if _, ok := p.(qos.BlockHeightTracker); ok {
		t.Error("declares BlockHeightTracker with no producer — that is exactly the dead machinery removed on 2026-09-04")
	}
}
