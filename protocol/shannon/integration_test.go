//go:build integration

package shannon

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sdktypes "github.com/pokt-network/shannon-sdk/types"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// integrationServiceID is the service exercised by integration relay tests.
const integrationServiceID = "eth"

// integrationPayload is a minimal eth_blockNumber JSON-RPC body.
var integrationPayload = []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

// loadIntegrationConfig reads and parses the config from the SAGE_CONFIG env var.
// The test is skipped if SAGE_CONFIG is not set.
func loadIntegrationConfig(t *testing.T) *config.Config {
	t.Helper()

	path := os.Getenv("SAGE_CONFIG")
	if path == "" {
		t.Skip("SAGE_CONFIG not set — skipping integration test")
	}

	cfg, err := config.LoadFromFile(path)
	if err != nil {
		t.Fatalf("failed to load config from %s: %v", path, err)
	}
	return cfg
}

// newIntegrationFullNode creates a FullNode from config or skips if the node is unreachable.
func newIntegrationFullNode(t *testing.T, cfg *config.Config) *FullNode {
	t.Helper()

	fn, err := NewFullNode(cfg.FullNode, newTestLogger())
	if err != nil {
		t.Skipf("could not connect to full node (%v) — is it running?", err)
	}
	return fn
}

// TestShannon_Integration_GetBlockHeight fetches the current block height from the full node.
func TestShannon_Integration_GetBlockHeight(t *testing.T) {
	cfg := loadIntegrationConfig(t)
	fn := newIntegrationFullNode(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	height, err := fn.GetCurrentBlockHeight(ctx)
	if err != nil {
		t.Fatalf("GetCurrentBlockHeight: %v", err)
	}
	if height <= 0 {
		t.Fatalf("expected positive block height, got %d", height)
	}
	t.Logf("current block height: %d", height)
}

// TestShannon_Integration_GetSession fetches a session for the integration service
// and verifies it has at least one supplier.
func TestShannon_Integration_GetSession(t *testing.T) {
	cfg := loadIntegrationConfig(t)
	fn := newIntegrationFullNode(t, cfg)

	if cfg.Gateway.GetServiceConfig(integrationServiceID) == nil {
		t.Skipf("service %q not in config", integrationServiceID)
	}

	appAddr := gatewayAddress(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session, err := fn.GetSession(ctx, integrationServiceID, appAddr)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if session == nil {
		t.Fatal("GetSession returned nil")
	}

	suppliers := session.GetSuppliers()
	if len(suppliers) == 0 {
		t.Error("session has no suppliers")
	}
	t.Logf("session %s  start_block=%d  suppliers=%d",
		session.SessionId,
		session.Header.SessionStartBlockHeight,
		len(suppliers),
	)
}

// TestShannon_Integration_SendRelay builds and signs a real relay request, sends it
// to the first available JSON-RPC endpoint, validates the response signature, and
// asserts the body contains a hex block number.
func TestShannon_Integration_SendRelay(t *testing.T) {
	cfg := loadIntegrationConfig(t)
	fn := newIntegrationFullNode(t, cfg)

	if cfg.Gateway.GetServiceConfig(integrationServiceID) == nil {
		t.Skipf("service %q not in config", integrationServiceID)
	}

	appAddr := gatewayAddress(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fetch the current session.
	session, err := fn.GetSession(ctx, integrationServiceID, appAddr)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	// Extract endpoints from the session.
	endpoints := endpointsFromSession(session)
	if len(endpoints) == 0 {
		t.Fatal("no endpoints in session")
	}

	// Pick the first endpoint that advertises JSON-RPC.
	var ep *endpoint
	for _, candidate := range endpoints {
		if _, err := candidate.GetURL(domain.RPCTypeJSONRPC); err == nil {
			ep = candidate
			break
		}
	}
	if ep == nil {
		t.Skip("no JSON-RPC endpoint available in session")
	}

	endpointURL, _ := ep.GetURL(domain.RPCTypeJSONRPC)
	t.Logf("relay target: supplier=%s url=%s", ep.Supplier(), endpointURL)

	// Fetch the application record for ring signing.
	app, err := fn.GetApp(ctx, appAddr)
	if err != nil {
		t.Fatalf("GetApp: %v", err)
	}

	// Build the relay signer from the gateway private key.
	signer, err := newRelaySigner(fn.pubKeys, cfg.Gateway.GatewayPrivateKeyHex, newTestLogger())
	if err != nil {
		t.Fatalf("newRelaySigner: %v", err)
	}

	// Serialize the JSON-RPC body as a relay HTTP payload.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(integrationPayload))
	if err != nil {
		t.Fatalf("build HTTP request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	_, payloadBz, err := sdktypes.SerializeHTTPRequest(httpReq)
	if err != nil {
		t.Fatalf("SerializeHTTPRequest: %v", err)
	}

	// Build the unsigned relay request.
	unsignedReq := &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader:           session.Header,
			SupplierOperatorAddress: ep.Supplier(),
		},
		Payload: payloadBz,
	}

	// Sign the relay request.
	signedReq, err := signer.signRelayRequest(ctx, unsignedReq, app)
	if err != nil {
		t.Fatalf("signRelayRequest: %v", err)
	}

	// Marshal to wire format.
	reqBz, err := signedReq.Marshal()
	if err != nil {
		t.Fatalf("Marshal relay request: %v", err)
	}

	// Send the relay via HTTP POST.
	httpClient := &http.Client{Timeout: 20 * time.Second}
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(reqBz))
	if err != nil {
		t.Fatalf("build POST request: %v", err)
	}
	postReq.Header.Set("Content-Type", "application/octet-stream")

	httpResp, err := httpClient.Do(postReq)
	if err != nil {
		t.Fatalf("HTTP POST to supplier: %v", err)
	}
	defer httpResp.Body.Close()

	respBz, err := io.ReadAll(httpResp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	t.Logf("raw response: %d bytes  http_status=%d", len(respBz), httpResp.StatusCode)

	// Validate the relay response signature.
	relayResp, err := fn.ValidateRelayResponse(ep.Supplier(), respBz)
	if err != nil {
		t.Fatalf("ValidateRelayResponse: %v", err)
	}

	// Deserialize the HTTP payload embedded in the relay response.
	poktHTTPResp, err := sdktypes.DeserializeHTTPResponse(relayResp.Payload)
	if err != nil {
		t.Fatalf("DeserializeHTTPResponse: %v", err)
	}

	body := string(poktHTTPResp.BodyBz)
	t.Logf("response body: %s", body)

	if !strings.Contains(body, `"result"`) {
		t.Errorf("expected JSON-RPC 'result' field in response body: %s", body)
	}
	if !strings.Contains(body, "0x") {
		t.Errorf("expected hex block number (0x...) in response body: %s", body)
	}
}

// gatewayAddress returns the gateway address from the config or skips the test.
func gatewayAddress(t *testing.T, cfg *config.Config) string {
	t.Helper()

	addr := cfg.Gateway.GatewayAddress
	if addr == "" {
		t.Skip("gateway_address not set in config")
	}
	return addr
}
