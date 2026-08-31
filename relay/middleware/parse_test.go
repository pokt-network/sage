package middleware_test

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func TestParse_MissingServiceHeader(t *testing.T) {
	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
	ctx := newCtx(req)

	var called bool
	handler := mw(relay.HandlerFunc(func(_ *relay.Context) error {
		called = true
		return nil
	}))

	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error for missing service header")
	}
	if called {
		t.Fatal("next handler should not be called when header is missing")
	}
	re, ok := err.(*domain.RelayError)
	if !ok {
		t.Fatalf("expected *domain.RelayError, got %T", err)
	}
	if re.Kind != domain.ErrValidation {
		t.Errorf("expected ErrValidation, got %v", re.Kind)
	}
	w := ctx.Writer.(*mockWriter)
	if w.statusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.statusCode)
	}
}

func TestParse_ValidServiceHeader_NoPlugin(t *testing.T) {
	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)

	var called bool
	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		called = true
		if c.ServiceID != "eth" {
			t.Errorf("expected ServiceID eth, got %s", c.ServiceID)
		}
		if c.RPCType != domain.RPCTypeJSONRPC {
			t.Errorf("expected RPCTypeJSONRPC, got %s", c.RPCType)
		}
		if len(c.Payloads) == 0 {
			t.Error("expected at least one payload")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("next handler was not called")
	}
}

func TestParse_ValidServiceHeader_WithPlugin(t *testing.T) {
	registry := qos.NewRegistry()
	plugin := &mockPlugin{
		parsedPayload: domain.NewPayload([]byte(`{"jsonrpc":"2.0"}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}
	_ = registry.Register("eth", plugin)

	mw := middleware.Parse(registry)

	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if len(c.Payloads) == 0 {
			t.Error("expected payloads from plugin")
		}
		// Plugin should be stored.
		if c.Plugin == nil {
			t.Error("plugin not stored in context")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParse_PluginParseError(t *testing.T) {
	registry := qos.NewRegistry()
	plugin := &mockPlugin{parseErr: domain.NewRelayError(domain.ErrValidation, "bad request", nil, false)}
	_ = registry.Register("eth", plugin)

	mw := middleware.Parse(registry)

	req := newPOSTRequest("/v1", `bad body`)
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)

	var called bool
	handler := mw(relay.HandlerFunc(func(_ *relay.Context) error {
		called = true
		return nil
	}))

	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error from plugin parse failure")
	}
	if called {
		t.Fatal("next handler should not be called on parse error")
	}
}

// --- RPC type detection tests --- //

func TestParse_RPCTypeDetection_JSONRPC(t *testing.T) {
	tests := []struct {
		name    string
		makeReq func() *http.Request
	}{
		{
			name: "POST with jsonrpc object",
			makeReq: func() *http.Request {
				req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
				req.Header.Set("Target-Service-Id", "eth")
				return req
			},
		},
		{
			name: "POST with jsonrpc batch array",
			makeReq: func() *http.Request {
				req := newPOSTRequest("/v1", `[{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}]`)
				req.Header.Set("Target-Service-Id", "eth")
				return req
			},
		},
	}

	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCtx(tc.makeReq())
			handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
				if c.RPCType != domain.RPCTypeJSONRPC {
					t.Errorf("expected RPCTypeJSONRPC, got %s", c.RPCType)
				}
				return nil
			}))
			if err := handler.HandleRelay(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestParse_RPCTypeDetection_REST(t *testing.T) {
	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	req := newGETRequest("/cosmos/bank/v1beta1/balances/addr")
	req.Header.Set("Target-Service-Id", "xrplevm")
	ctx := newCtx(req)

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if c.RPCType != domain.RPCTypeREST {
			t.Errorf("expected RPCTypeREST, got %s", c.RPCType)
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParse_RPCTypeDetection_CometBFT(t *testing.T) {
	paths := []string{"/status", "/health", "/block", "/validators"}

	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := newGETRequest(path)
			req.Header.Set("Target-Service-Id", "xrplevm")
			ctx := newCtx(req)

			handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
				if c.RPCType != domain.RPCTypeCometBFT {
					t.Errorf("path %s: expected RPCTypeCometBFT, got %s", path, c.RPCType)
				}
				return nil
			}))

			if err := handler.HandleRelay(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestParse_RPCTypeDetection_WebSocket(t *testing.T) {
	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	req := newWSRequest()
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if c.RPCType != domain.RPCTypeWebSocket {
			t.Errorf("expected RPCTypeWebSocket, got %s", c.RPCType)
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// gRPC is identified by media type, not by path — a method path like
// /cosmos.bank.v1beta1.Query/Params starts with "/cosmos." and would otherwise
// be read as a Cosmos REST path ("/cosmos/").
func TestParse_RPCTypeDetection_GRPC(t *testing.T) {
	framings := []string{
		"application/grpc",
		"application/grpc+proto",
		"application/grpc-web",
		"application/grpc-web+proto",
		"application/grpc-web-text",
		"application/grpc-web+json; charset=utf-8",
		"APPLICATION/GRPC-WEB",
	}

	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	for _, ct := range framings {
		t.Run(ct, func(t *testing.T) {
			req := newPOSTRequest("/cosmos.bank.v1beta1.Query/Params", "\x00\x00\x00\x00\x00")
			req.Header.Set("Target-Service-Id", "pocket")
			req.Header.Set("Content-Type", ct)
			ctx := newCtx(req)

			handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
				if c.RPCType != domain.RPCTypeGRPC {
					t.Errorf("RPCType = %q, want %q", c.RPCType, domain.RPCTypeGRPC)
				}
				return nil
			}))
			if err := handler.HandleRelay(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// The media-type check must not swallow requests that merely share the prefix,
// nor the Cosmos REST paths served by grpc-gateway.
func TestParse_GRPCDetectionDoesNotCatchOtherTransports(t *testing.T) {
	registry := qos.NewRegistry()
	mw := middleware.Parse(registry)

	tests := []struct {
		name        string
		contentType string
		want        domain.RPCType
	}{
		{"json rpc", "application/json", domain.RPCTypeJSONRPC},
		{"no content type", "", domain.RPCTypeJSONRPC},
		// Shares the first sixteen characters with gRPC and is not gRPC.
		{"different type, same prefix", "application/grpcish+json", domain.RPCTypeJSONRPC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newPOSTRequest("/", `{"jsonrpc":"2.0","id":1,"method":"status"}`)
			req.Header.Set("Target-Service-Id", "pocket")
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			ctx := newCtx(req)

			var got domain.RPCType
			handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
				got = c.RPCType
				return nil
			}))
			if err := handler.HandleRelay(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("RPCType = %q, want %q", got, tt.want)
			}
		})
	}
}

// A chain's native REST namespace (tron /wallet, pocket /poktroll) is REST
// when the service declares rest — without enumerating every chain's paths.
// SAGE used to default these to JSON-RPC and route them to json_rpc suppliers
// that do not serve them (tron 405, pocket 501 on the mainnet canary).
func TestParse_RPCTypeDetection_NativeREST(t *testing.T) {
	rpcTypes := func(svc domain.ServiceID) []string {
		switch svc {
		case "tron":
			return []string{"rest", "json_rpc"}
		case "pocket":
			return []string{"json_rpc", "rest", "comet_bft"}
		case "eth":
			return []string{"json_rpc"}
		}
		return nil
	}
	mw := middleware.ParseWithServices(qos.NewRegistry(), rpcTypes)

	cases := []struct {
		name string
		req  func() *http.Request
		want domain.RPCType
	}{
		{"tron native REST POST", func() *http.Request {
			r := newPOSTRequest("/wallet/getnowblock", `{}`)
			r.Header.Set("Target-Service-Id", "tron")
			return r
		}, domain.RPCTypeREST},
		{"pocket native REST GET", func() *http.Request {
			r := newGETRequest("/poktroll/session/params")
			r.Header.Set("Target-Service-Id", "pocket")
			return r
		}, domain.RPCTypeREST},
		{"tron eth_ JSON-RPC at root stays JSON-RPC", func() *http.Request {
			r := newPOSTRequest("/", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
			r.Header.Set("Target-Service-Id", "tron")
			return r
		}, domain.RPCTypeJSONRPC},
		{"tron JSON-RPC entry path /jsonrpc stays JSON-RPC", func() *http.Request {
			r := newPOSTRequest("/jsonrpc", `{"method":"eth_blockNumber","id":1}`)
			r.Header.Set("Target-Service-Id", "tron")
			return r
		}, domain.RPCTypeJSONRPC},
		{"ambiguous bare POST at root on a REST service defaults JSON-RPC", func() *http.Request {
			r := newPOSTRequest("/", `{}`)
			r.Header.Set("Target-Service-Id", "tron")
			return r
		}, domain.RPCTypeJSONRPC},
		{"EVM path on a json_rpc-only service stays JSON-RPC", func() *http.Request {
			r := newPOSTRequest("/anything", `{"jsonrpc":"2.0","method":"eth_call","id":1}`)
			r.Header.Set("Target-Service-Id", "eth")
			return r
		}, domain.RPCTypeJSONRPC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCtx(tc.req())
			var got domain.RPCType
			h := mw(relay.HandlerFunc(func(c *relay.Context) error { got = c.RPCType; return nil }))
			if err := h.HandleRelay(ctx); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}
