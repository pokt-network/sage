package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// The client-visible contract, layer 1 of docs/path-compat.md: what a client
// meets on /v1 and the health routes, pinned so it cannot drift without a
// test saying so. Each test names the PATH behaviour it matches or the
// register row it diverges under.

// mockEndpointLister adds per-service readiness to mockSessions.
type mockEndpointLister struct {
	mockSessions
	endpoints map[domain.ServiceID]int
}

func (m *mockEndpointLister) AvailableEndpoints(_ context.Context, id domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	n, ok := m.endpoints[id]
	if !ok {
		return nil, errors.New("no session")
	}
	list := make(domain.EndpointAddrList, n)
	return list, nil
}

func okChain() *mockChain {
	return &mockChain{response: &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), HTTPStatusCode: 200}}
}

func serve(t *testing.T, r *Router, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r.mux.ServeHTTP(rec, req)
	return rec
}

func TestContract_RelayResponseHasJSONContentType(t *testing.T) {
	r := newTestRouter(t, okChain(), &mockSessions{ready: true})
	req := httptest.NewRequest(http.MethodPost, "/v1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`))
	req.Header.Set("Target-Service-Id", "eth")
	rec := serve(t, r, req)
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json (PATH sets it on every QoS response)", got)
	}
}

func TestContract_CORSOnEveryV1ResponseAndPreflight(t *testing.T) {
	r := newTestRouter(t, okChain(), &mockSessions{ready: true})

	pre := httptest.NewRequest(http.MethodOptions, "/v1", nil)
	pre.Header.Set("Origin", "https://dapp.example")
	pre.Header.Set("Access-Control-Request-Method", "POST")
	pre.Header.Set("Access-Control-Request-Headers", "content-type,target-service-id")
	rec := serve(t, r, pre)
	if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS /v1 = %d, want 200/204 (was 405: browser dapps failed preflight)", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://dapp.example" {
		t.Errorf("Allow-Origin = %q, want the Origin mirrored", got)
	}
	for _, h := range []string{"Content-Type", "Target-Service-Id", "RPC-Type"} {
		if !strings.Contains(strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers")), strings.ToLower(h)) {
			t.Errorf("Allow-Headers = %q, want %s", rec.Header().Get("Access-Control-Allow-Headers"), h)
		}
	}

	post := httptest.NewRequest(http.MethodPost, "/v1", strings.NewReader(`{}`))
	post.Header.Set("Origin", "https://dapp.example")
	post.Header.Set("Target-Service-Id", "eth")
	rec = serve(t, r, post)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://dapp.example" {
		t.Errorf("relay response Allow-Origin = %q, want the Origin mirrored", got)
	}
	if got := rec.Header().Get("Vary"); !strings.Contains(got, "Origin") {
		t.Errorf("Vary = %q, want Origin so caches do not serve one origin's grant to another", got)
	}

	// No Origin: no grant, nothing to leak to a cache.
	plain := httptest.NewRequest(http.MethodPost, "/v1", strings.NewReader(`{}`))
	plain.Header.Set("Target-Service-Id", "eth")
	rec = serve(t, r, plain)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("Allow-Origin set with no Origin header")
	}
}

func TestContract_AnyVerbReachesTheRelayChain(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead} {
		chain := okChain()
		r := newTestRouter(t, chain, &mockSessions{ready: true})
		req := httptest.NewRequest(method, "/v1/cosmos/bank/v1beta1/params", nil)
		req.Header.Set("Target-Service-Id", "cosmos")
		rec := serve(t, r, req)
		if rec.Code == http.StatusMethodNotAllowed {
			t.Errorf("%s /v1/... = 405; PATH routes every verb to REST services", method)
		}
		if !chain.called {
			t.Errorf("%s did not reach the chain", method)
		}
	}
}

func TestContract_GatewayErrorStatusFollowsTheCause(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"timeout", domain.NewRelayError(domain.ErrTransport, "relay timeout exceeded", context.DeadlineExceeded, true), http.StatusGatewayTimeout},
		{"transport", domain.NewRelayError(domain.ErrTransport, "dial failed", errors.New("x"), true), http.StatusInternalServerError},
		{"protocol", domain.NewRelayError(domain.ErrProtocol, "no endpoints", nil, false), http.StatusInternalServerError},
		{"rate limit", domain.NewRelayError(domain.ErrRateLimit, "slow down", nil, false), http.StatusTooManyRequests},
		{"validation", domain.NewRelayError(domain.ErrValidation, "bad", nil, false), http.StatusBadRequest},
		{"untyped", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse has run by the time a deep failure happens, so the payload
			// (and its id) exists on the context.
			raw := `{"jsonrpc":"2.0","id":9,"method":"eth_blockNumber"}`
			failing := relay.HandlerFunc(func(ctx *relay.Context) error {
				ctx.RPCType = domain.RPCTypeJSONRPC
				ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(raw), domain.RPCTypeJSONRPC, "eth_blockNumber")}
				return tc.err
			})
			r := newTestRouter(t, failing, &mockSessions{ready: true})
			req := httptest.NewRequest(http.MethodPost, "/v1", strings.NewReader(raw))
			req.Header.Set("Target-Service-Id", "eth")
			req.Header.Set("Content-Type", "application/json")
			rec := serve(t, r, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d; a 200 hid this from every load balancer (register: gateway errors)", rec.Code, tc.want)
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body not JSON: %s", rec.Body.String())
			}
			e, _ := body["error"].(map[string]any)
			if e == nil || e["code"] != float64(-32603) {
				t.Errorf("error = %v, want code -32603 kept (the standard code, not PATH's -31001)", body["error"])
			}
			if body["id"] != float64(9) {
				t.Errorf("id = %v, want 9 echoed", body["id"])
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q", got)
			}
		})
	}
}

func TestContract_HealthIsLivenessHealthzIsReadiness(t *testing.T) {
	sessions := &mockSessions{ready: false}
	r := newTestRouter(t, relay.Noop, sessions)
	// PATH: GET /health is unconditional 200. A liveness probe written for
	// PATH must not restart SAGE pods during a full-node outage.
	if rec := serve(t, r, httptest.NewRequest(http.MethodGet, "/health", nil)); rec.Code != http.StatusOK {
		t.Errorf("/health = %d while sessions are down, want 200 (liveness)", rec.Code)
	}
	if rec := serve(t, r, httptest.NewRequest(http.MethodGet, "/livez", nil)); rec.Code != http.StatusOK {
		t.Errorf("/livez = %d, want 200", rec.Code)
	}
	if rec := serve(t, r, httptest.NewRequest(http.MethodGet, "/healthz", nil)); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/healthz = %d while sessions are down, want 503 (readiness, as on PATH)", rec.Code)
	}
	sessions.ready = true
	if rec := serve(t, r, httptest.NewRequest(http.MethodGet, "/healthz", nil)); rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d once ready, want 200", rec.Code)
	}
}

func TestContract_ReadyPerServiceCanFail(t *testing.T) {
	sessions := &mockEndpointLister{
		mockSessions: mockSessions{ready: true, services: map[domain.ServiceID]struct{}{"eth": {}, "poly": {}, "sui": {}}},
		endpoints:    map[domain.ServiceID]int{"eth": 3, "poly": 0},
	}
	r := New(config.RouterConfig{}, relay.Noop, sessions, nil, discardLogger())
	for id, want := range map[string]int{"eth": http.StatusOK, "poly": http.StatusServiceUnavailable, "sui": http.StatusServiceUnavailable, "nope": http.StatusNotFound} {
		rec := serve(t, r, httptest.NewRequest(http.MethodGet, "/ready/"+id, nil))
		if rec.Code != want {
			t.Errorf("/ready/%s = %d, want %d (PATH: 503 when the service has no session or endpoints)", id, rec.Code, want)
		}
	}
	var body map[string]any
	rec := serve(t, r, httptest.NewRequest(http.MethodGet, "/ready/eth", nil))
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["ready"] != true || body["endpoint_count"] != float64(3) {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestContract_WebSocketGateBeforeUpgrade(t *testing.T) {
	ws := &mockWSOpener{}
	r := newTestRouterWithWS(t, relay.Noop, &mockSessions{ready: true}, ws)
	r.SetServiceRPCTypes(func(id domain.ServiceID) ([]string, bool) {
		switch id {
		case "eth":
			return []string{"json_rpc", "websocket"}, true
		case "rest-only":
			return []string{"rest"}, true
		}
		return nil, false
	})
	upgrade := func(svc string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/v1", nil)
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Target-Service-Id", svc)
		return req
	}
	if rec := serve(t, r, upgrade("rest-only")); rec.Code != http.StatusBadRequest || ws.called {
		t.Errorf("rest-only: status=%d opened=%v, want 400 before any upgrade (PATH: 'WebSocket not supported for service')", rec.Code, ws.called)
	}
	if rec := serve(t, r, upgrade("nope")); rec.Code != http.StatusBadRequest || ws.called {
		t.Errorf("unconfigured: status=%d opened=%v, want 400", rec.Code, ws.called)
	}
	serve(t, r, upgrade("eth"))
	if !ws.called {
		t.Error("eth declares websocket and was not opened")
	}
}

func TestContract_ServerDefaultsMatchPATH(t *testing.T) {
	r := newTestRouter(t, relay.Noop, &mockSessions{})
	if r.server.ReadTimeout.Seconds() != 60 || r.server.WriteTimeout.Seconds() != 120 || r.server.IdleTimeout.Seconds() != 180 {
		t.Errorf("timeouts = %v/%v/%v, want 60s/120s/180s (PATH's; 30s write cut slow archival calls)", r.server.ReadTimeout, r.server.WriteTimeout, r.server.IdleTimeout)
	}
	if r.server.MaxHeaderBytes != 2_000_000 {
		t.Errorf("MaxHeaderBytes = %d, want 2 MB (PATH's)", r.server.MaxHeaderBytes)
	}
}
