//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// The client-visible contract (docs/path-compat.md, layer 1). Every assertion
// here holds against PATH as well as SAGE, so the suite can be pointed at
// either: what it pins is what a client is entitled to from a gateway on
// /v1, not either gateway's private shape.

func TestContract_RelayResponseIsJSONTyped(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	resp, _ := sendRaw(t, http.MethodPost, "/v1", body, map[string]string{"Target-Service-Id": defaultServiceID()})
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func TestContract_CORSPreflightAndGrant(t *testing.T) {
	headers := map[string]string{
		"Origin":                         "https://dapp.example",
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "content-type,target-service-id",
	}
	resp, body := sendRaw(t, http.MethodOptions, "/v1", nil, headers)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("OPTIONS /v1 = %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://dapp.example" && got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q", got)
	}

	relay := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	resp, _ = sendRaw(t, http.MethodPost, "/v1", relay, map[string]string{
		"Target-Service-Id": defaultServiceID(),
		"Origin":            "https://dapp.example",
	})
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got == "" {
		t.Error("relay response carries no Access-Control-Allow-Origin; a browser dapp cannot read it")
	}
}

func TestContract_UnconfiguredServiceIs400InvalidRequest(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`)
	resp, raw := sendRaw(t, http.MethodPost, "/v1", body, map[string]string{"Target-Service-Id": "no-such-service-e2e"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", resp.StatusCode, raw)
	}
	var out struct {
		ID    any `json:"id"`
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("body is not JSON: %s", raw)
	}
	if out.Error.Code != -32600 {
		t.Errorf("error.code = %d, want -32600: %s", out.Error.Code, raw)
	}
	if id, _ := out.ID.(float64); id != 7 {
		t.Errorf("id = %v, want 7 echoed: %s", out.ID, raw)
	}
}

func TestContract_MalformedJSONRPCIs400(t *testing.T) {
	resp, raw := sendRaw(t, http.MethodPost, "/v1", []byte(`[]`), map[string]string{"Target-Service-Id": defaultServiceID()})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty batch: status = %d, want 400: %s", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestContract_HealthIsLiveness(t *testing.T) {
	// /health answers 200 whenever the process serves. It is what a PATH
	// livenessProbe points at, so it must not depend on sessions or the full
	// node; a restart loop during a full-node outage is the failure this
	// prevents. Readiness is /healthz and /ready.
	resp, body := sendRaw(t, http.MethodGet, "/health", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health = %d: %s", resp.StatusCode, body)
	}
}
