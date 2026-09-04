//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

var baseURL = envOrDefault("SAGE_URL", "http://localhost:3069")

// TestHealthz verifies the readiness route returns 200 against a gateway that
// has sessions. Liveness (/health) is pinned in contract_test.go.
func TestHealthz(t *testing.T) {
	resp, body := sendRaw(t, http.MethodGet, "/healthz", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
}

// TestReady verifies the /ready endpoint returns 200.
func TestReady(t *testing.T) {
	resp, body := sendRaw(t, http.MethodGet, "/ready", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
}

// TestEthBlockNumber sends eth_blockNumber to the "eth" service and checks
// that the result is a non-empty hex string.
func TestEthBlockNumber(t *testing.T) {
	result := sendJSONRPC(t, "eth", "eth_blockNumber", []any{})

	if errVal, ok := result["error"]; ok {
		t.Fatalf("unexpected JSON-RPC error: %v", errVal)
	}
	blockHex, ok := result["result"].(string)
	if !ok || blockHex == "" {
		t.Fatalf("expected non-empty hex string in result, got: %v", result["result"])
	}
	if !strings.HasPrefix(blockHex, "0x") {
		t.Fatalf("expected hex result starting with 0x, got: %s", blockHex)
	}
}

// TestPolyBlockNumber sends eth_blockNumber to the "poly" service.
func TestPolyBlockNumber(t *testing.T) {
	result := sendJSONRPC(t, "poly", "eth_blockNumber", []any{})

	if errVal, ok := result["error"]; ok {
		t.Fatalf("unexpected JSON-RPC error: %v", errVal)
	}
	blockHex, ok := result["result"].(string)
	if !ok || blockHex == "" {
		t.Fatalf("expected non-empty hex string in result, got: %v", result["result"])
	}
	if !strings.HasPrefix(blockHex, "0x") {
		t.Fatalf("expected hex result starting with 0x, got: %s", blockHex)
	}
}

// TestSolanaGetSlot sends getSlot to the "solana" service and checks that
// the result is a numeric value.
func TestSolanaGetSlot(t *testing.T) {
	result := sendJSONRPC(t, "solana", "getSlot", []any{})

	if errVal, ok := result["error"]; ok {
		t.Fatalf("unexpected JSON-RPC error: %v", errVal)
	}
	// getSlot returns a number; json.Unmarshal decodes it as float64.
	slot, ok := result["result"].(float64)
	if !ok || slot <= 0 {
		t.Fatalf("expected positive numeric slot, got: %v", result["result"])
	}
}

// TestBatchRequest sends a batch [eth_blockNumber, eth_chainId] to "eth" and
// checks that the response is a JSON array with two elements.
func TestBatchRequest(t *testing.T) {
	batch := []map[string]any{
		{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": []any{}, "id": 1},
		{"jsonrpc": "2.0", "method": "eth_chainId", "params": []any{}, "id": 2},
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("failed to marshal batch: %v", err)
	}

	resp, respBody := sendRaw(t, http.MethodPost, "/v1", body, map[string]string{
		"Target-Service-Id": "eth",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var results []map[string]any
	if err := json.Unmarshal(respBody, &results); err != nil {
		t.Fatalf("expected JSON array response for batch, err: %v\nbody: %s", err, respBody)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 batch results, got %d", len(results))
	}
	for i, r := range results {
		if _, hasResult := r["result"]; !hasResult {
			if errVal, hasErr := r["error"]; hasErr {
				t.Errorf("batch item %d returned error: %v", i, errVal)
			} else {
				t.Errorf("batch item %d missing both result and error fields", i)
			}
		}
	}
}

// TestMissingServiceHeader verifies that a POST to /v1 without the
// Target-Service-Id header returns a 400 status.
func TestMissingServiceHeader(t *testing.T) {
	payload := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	resp, _ := sendRaw(t, http.MethodPost, "/v1", payload, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 when Target-Service-Id is absent, got %d", resp.StatusCode)
	}
}

// TestAdminFlags verifies that GET /admin/flags returns 200 with a JSON body.
func TestAdminFlags(t *testing.T) {
	resp, body := sendRaw(t, http.MethodGet, "/admin/flags", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
	var flags any
	if err := json.Unmarshal(body, &flags); err != nil {
		t.Fatalf("expected valid JSON from /admin/flags: %v\nbody: %s", err, body)
	}
}
