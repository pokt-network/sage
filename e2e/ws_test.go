//go:build e2e

package e2e

import (
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

// WebSocket e2e covers the router → WSRelayer → flag-gate path.
//
// Full Shannon-signed round-trip (open a subscription, receive notifications)
// requires a localnet with suppliers advertising WebSocket endpoints. That is
// gated behind the SAGE_WS_ROUNDTRIP=1 env var so routine CI doesn't need a
// live localnet. When unset, the round-trip test is skipped with a helpful
// message and only the routing-level assertions run.

// TestWebSocket_FlagOff_Returns503 asserts that an Upgrade request returns
// 503 when the websocket_relays feature flag is disabled (default).
// This verifies the full routing path: GET /v1 → handleMaybeWebSocket →
// wsRelayer.Open → flag gate. If the flag is on globally the test is skipped
// because the assertion would fail for the expected reason.
func TestWebSocket_FlagOff_Returns503(t *testing.T) {
	wsURL := strings.Replace(baseURL, "http", "ws", 1) + "/v1"

	header := http.Header{}
	header.Set("Target-Service-Id", defaultServiceID())

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err == nil {
		t.Fatal("expected dial to fail (flag off should produce HTTP 503); got successful upgrade")
	}
	if resp == nil {
		t.Skipf("SAGE not reachable at %s: %v", baseURL, err)
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return // success: flag-off path produces 503
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusSwitchingProtocols {
		t.Skipf("websocket_relays flag appears to be enabled (status %d); skipping flag-off assertion", resp.StatusCode)
	}
	t.Errorf("expected 503 when flag off, got %d (err: %v)", resp.StatusCode, err)
}

// TestWebSocket_MissingServiceID_Returns400 asserts the Upgrade handshake
// fails with 400 when no Target-Service-Id header is present.
func TestWebSocket_MissingServiceID_Returns400(t *testing.T) {
	wsURL := strings.Replace(baseURL, "http", "ws", 1) + "/v1"

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected dial to fail without service id; got success")
	}
	if resp == nil {
		t.Skipf("SAGE not reachable at %s: %v", baseURL, err)
	}
	// Either 400 (new WS path) or 503 (if WS path is disabled and falls through) is
	// acceptable — both mean the request didn't reach the WS bridge machinery
	// without a service id, which is the goal.
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusServiceUnavailable:
		return
	default:
		t.Errorf("expected 400 or 503 for missing Target-Service-Id, got %d", resp.StatusCode)
	}
}

// TestWebSocket_SubscriptionRoundTrip opens a real WS subscription against
// the configured service and asserts the handshake succeeds.
//
// Requires:
//   - A running SAGE gateway pointed at a localnet or real network
//   - websocket_relays feature flag enabled for SAGE_WS_SERVICE
//   - Suppliers advertising a WebSocket endpoint for that service
//
// Opt in via SAGE_WS_ROUNDTRIP=1. The test is skipped otherwise.
func TestWebSocket_SubscriptionRoundTrip(t *testing.T) {
	if os.Getenv("SAGE_WS_ROUNDTRIP") == "" {
		t.Skip("set SAGE_WS_ROUNDTRIP=1 to run the full WS round-trip test; requires localnet + flag on")
	}
	serviceID := envOrDefault("SAGE_WS_SERVICE", defaultServiceID())
	wsURL := strings.Replace(baseURL, "http", "ws", 1) + "/v1"

	header := http.Header{}
	header.Set("Target-Service-Id", serviceID)

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			t.Fatalf("dial failed: status %d: %v", resp.StatusCode, err)
		}
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send an eth_subscribe request.
	if err := conn.WriteMessage(websocket.TextMessage, []byte(
		`{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":1}`,
	)); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	// Read one frame back. This should be the subscribe ack.
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read subscribe ack: %v", err)
	}
	if !strings.Contains(string(msg), `"id":1`) {
		t.Errorf("expected subscribe ack with id:1, got: %s", msg)
	}
}

// defaultServiceID returns the service id to target for WS tests. Defaults to
// "eth" but can be overridden via SAGE_WS_SERVICE (same convention as the
// rest of the e2e suite).
func defaultServiceID() string {
	return envOrDefault("SAGE_WS_SERVICE", "eth")
}
