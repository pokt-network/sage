//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
)

// envOrDefault returns the value of the named environment variable, or
// defaultVal if the variable is unset or empty.
func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// sendRaw performs an HTTP request and returns the response and body bytes.
// The test is skipped if the server is unreachable.
func sendRaw(t *testing.T, method, path string, body []byte, headers map[string]string) (*http.Response, []byte) {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, baseURL+path, bodyReader)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("SAGE not reachable at %s: %v", baseURL, err)
		return nil, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return resp, respBody
}

// sendJSONRPC sends a JSON-RPC request to /v1 for the given service and method,
// returning the decoded response object.  The test is skipped if unreachable.
func sendJSONRPC(t *testing.T, serviceID, method string, params []any) map[string]any {
	t.Helper()

	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal JSON-RPC request: %v", err)
	}

	resp, respBody := sendRaw(t, http.MethodPost, "/v1", body, map[string]string{
		"Target-Service-Id": serviceID,
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, respBody)
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to decode JSON-RPC response: %v\nbody: %s", err, respBody)
	}
	return result
}
