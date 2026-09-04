package middleware_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

// The client-visible contract for a request SAGE refuses before relaying it:
// a JSON body, Content-Type set, a JSON-RPC envelope when the request was
// JSON-RPC with the request's own id echoed, -32600 as the code, and data
// that says what *would* have been accepted. docs/path-compat.md lists these
// against PATH's shapes; these tests pin SAGE's side of that table.

type rejection struct {
	status  int
	ctype   string
	body    map[string]any
	rawBody string
}

func rejectionOf(t *testing.T, ctx *relay.Context) rejection {
	t.Helper()
	w := ctx.Writer.(*mockWriter)
	var body map[string]any
	if err := json.Unmarshal(w.body, &body); err != nil {
		t.Fatalf("rejection body is not JSON: %q", w.body)
	}
	return rejection{status: w.statusCode, ctype: w.headers["Content-Type"], body: body, rawBody: string(w.body)}
}

func (r rejection) jsonRPCError(t *testing.T) (code float64, message string, data map[string]any) {
	t.Helper()
	if r.body["jsonrpc"] != "2.0" {
		t.Fatalf("not a JSON-RPC envelope: %s", r.rawBody)
	}
	e, _ := r.body["error"].(map[string]any)
	if e == nil {
		t.Fatalf("no error object: %s", r.rawBody)
	}
	code, _ = e["code"].(float64)
	message, _ = e["message"].(string)
	data, _ = e["data"].(map[string]any)
	return code, message, data
}

func noNext(t *testing.T) relay.Handler {
	return relay.HandlerFunc(func(_ *relay.Context) error {
		t.Fatal("next handler must not run after a rejection")
		return nil
	})
}

func TestParse_MissingHeader_IsPlainJSONWithContentType(t *testing.T) {
	mw := middleware.Parse(qos.NewRegistry())
	ctx := newCtx(newPOSTRequest("/v1", `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`))
	if err := mw(noNext(t)).HandleRelay(ctx); err == nil {
		t.Fatal("expected an error")
	}
	r := rejectionOf(t, ctx)
	if r.status != http.StatusBadRequest || r.ctype != "application/json" {
		t.Fatalf("status=%d content-type=%q, want 400 application/json", r.status, r.ctype)
	}
	// No service, so nothing to shape the answer by: the plain form, with the
	// header named so the fix is obvious.
	if msg, _ := r.body["error"].(string); !strings.Contains(msg, "Target-Service-Id") {
		t.Fatalf("body = %s, want the header named", r.rawBody)
	}
}

func TestParse_PluginRejection_IsJSONRPCEnvelopeWithIDAndReason(t *testing.T) {
	registry := qos.NewRegistry()
	_ = registry.Register("eth", &mockPlugin{parseErr: domain.NewRelayError(domain.ErrValidation, "EVM plugin: batch array is empty", nil, false)})
	mw := middleware.Parse(registry)

	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","id":"abc","method":"eth_blockNumber"}`)
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)
	if err := mw(noNext(t)).HandleRelay(ctx); err == nil {
		t.Fatal("expected an error")
	}
	r := rejectionOf(t, ctx)
	if r.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.status)
	}
	code, msg, _ := r.jsonRPCError(t)
	if code != -32600 {
		t.Errorf("code = %v, want -32600", code)
	}
	if !strings.Contains(msg, "batch array is empty") {
		t.Errorf("message = %q; the plugin's reason must reach the client, not only the log", msg)
	}
	if r.body["id"] != "abc" {
		t.Errorf("id = %v, want the request's own id echoed", r.body["id"])
	}
}

func TestParse_BodyOverCap_Is413(t *testing.T) {
	mw := middleware.ParseWithOptions(qos.NewRegistry(), middleware.ParseOptions{MaxBodyBytes: 16})
	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`)
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)
	if err := mw(noNext(t)).HandleRelay(ctx); err == nil {
		t.Fatal("expected an error")
	}
	r := rejectionOf(t, ctx)
	if r.status != http.StatusRequestEntityTooLarge || r.ctype != "application/json" {
		t.Fatalf("status=%d content-type=%q, want 413 application/json", r.status, r.ctype)
	}
	// The body was never read, so there is no id to echo and no framing to
	// honour: the plain form, with the cap stated.
	if msg, _ := r.body["error"].(string); !strings.Contains(msg, "16") {
		t.Errorf("body = %s, want the cap stated", r.rawBody)
	}
}

func TestParse_RPCTypeHeader_OverridesDetection(t *testing.T) {
	mw := middleware.Parse(qos.NewRegistry())
	// A JSON-RPC-looking body, but the client says REST: the client wins.
	req := newPOSTRequest("/v1/wallet/getnowblock", `{"visible":true}`)
	req.Header.Set("Target-Service-Id", "tron")
	req.Header.Set("RPC-Type", "REST")
	ctx := newCtx(req)
	var got domain.RPCType
	next := relay.HandlerFunc(func(c *relay.Context) error { got = c.RPCType; return nil })
	if err := mw(next).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	if got != domain.RPCTypeREST {
		t.Fatalf("RPCType = %q, want rest from the header", got)
	}
}

func TestParse_RPCTypeHeader_Unknown_Is400WithAllowedList(t *testing.T) {
	mw := middleware.Parse(qos.NewRegistry())
	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)
	req.Header.Set("Target-Service-Id", "eth")
	req.Header.Set("RPC-Type", "carrier-pigeon")
	ctx := newCtx(req)
	if err := mw(noNext(t)).HandleRelay(ctx); err == nil {
		t.Fatal("expected an error")
	}
	r := rejectionOf(t, ctx)
	if r.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.status)
	}
	_, msg, data := r.jsonRPCError(t)
	if !strings.Contains(msg, "RPC-Type") {
		t.Errorf("message = %q, want the header named", msg)
	}
	if _, ok := data["allowed_rpc_types"]; !ok {
		t.Errorf("data = %v, want allowed_rpc_types", data)
	}
}

func TestValidate_UnconfiguredService_Is400WithAvailableServices(t *testing.T) {
	mw := middleware.Validate([]config.ServiceConfig{
		{ID: "poly", RPCTypes: []string{"json_rpc"}},
		{ID: "eth", RPCTypes: []string{"json_rpc"}},
	})
	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","id":3,"method":"eth_blockNumber"}`)
	req.Header.Set("Target-Service-Id", "nope")
	ctx := newCtx(req)
	ctx.ServiceID = "nope"
	ctx.RPCType = domain.RPCTypeJSONRPC
	// Parse has run by the time Validate does, so the payload exists and the
	// id comes from it.
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":3,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")}
	if err := mw(noNext(t)).HandleRelay(ctx); err == nil {
		t.Fatal("expected an error")
	}
	r := rejectionOf(t, ctx)
	if r.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.status)
	}
	code, msg, data := r.jsonRPCError(t)
	if code != -32600 || !strings.Contains(msg, `"nope"`) {
		t.Errorf("code=%v message=%q", code, msg)
	}
	avail, _ := data["available_services"].([]any)
	if len(avail) != 2 || avail[0] != "eth" || avail[1] != "poly" {
		t.Errorf("available_services = %v, want [eth poly] sorted", avail)
	}
	if r.body["id"] != float64(3) {
		t.Errorf("id = %v, want 3", r.body["id"])
	}
}

func TestValidate_UnsupportedType_SaysWhatIsAllowed(t *testing.T) {
	mw := middleware.Validate([]config.ServiceConfig{{ID: "eth", RPCTypes: []string{"json_rpc", "websocket"}}})
	req := newPOSTRequest("/v1/cosmos/bank", ``)
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeREST
	if err := mw(noNext(t)).HandleRelay(ctx); err == nil {
		t.Fatal("expected an error")
	}
	r := rejectionOf(t, ctx)
	if r.status != http.StatusBadRequest || r.ctype != "application/json" {
		t.Fatalf("status=%d content-type=%q", r.status, r.ctype)
	}
	// A REST request gets the plain form, with the same data attached.
	data, _ := r.body["data"].(map[string]any)
	if data["detected_type"] != "rest" || data["service_id"] != "eth" {
		t.Errorf("data = %v", data)
	}
	if allowed, _ := data["allowed_rpc_types"].([]any); len(allowed) != 2 {
		t.Errorf("allowed_rpc_types = %v", data["allowed_rpc_types"])
	}
}
