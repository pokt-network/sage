package router

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// --- Mocks ---

// mockChain is a relay.Handler that records calls and optionally returns an error.
type mockChain struct {
	called   bool
	err      error
	response *domain.Response
	degraded bool
}

func (m *mockChain) HandleRelay(ctx *relay.Context) error {
	m.called = true
	if m.response != nil {
		ctx.Response = m.response
	}
	ctx.Degraded = m.degraded
	return m.err
}

// mockSessions implements protocol.SessionManager.
type mockSessions struct {
	ready    bool
	services map[domain.ServiceID]struct{}
}

func (m *mockSessions) ConfiguredServices() map[domain.ServiceID]struct{} {
	if m.services == nil {
		return map[domain.ServiceID]struct{}{}
	}
	return m.services
}

func (m *mockSessions) IsReady(_ context.Context) bool {
	return m.ready
}

// --- Helpers ---

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestRouter(t *testing.T, chain relay.Handler, sessions *mockSessions) *Router {
	t.Helper()
	return New(
		config.RouterConfig{Port: 0},
		chain,
		sessions,
		nil, // no WS relayer in unit tests
		discardLogger(),
	)
}

// mockWSOpener captures Open calls so tests can assert routing to the WS path.
type mockWSOpener struct {
	called    bool
	serviceID domain.ServiceID
	returnErr error
}

func (m *mockWSOpener) Open(_ context.Context, svcID domain.ServiceID, _ *http.Request, w http.ResponseWriter) error {
	m.called = true
	m.serviceID = svcID
	if m.returnErr != nil {
		return m.returnErr
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func newTestRouterWithWS(t *testing.T, chain relay.Handler, sessions *mockSessions, ws WebSocketOpener) *Router {
	t.Helper()
	return New(
		config.RouterConfig{Port: 0},
		chain,
		sessions,
		ws,
		discardLogger(),
	)
}

// --- Relay endpoint tests ---

func TestHandleRelay_CallsChain(t *testing.T) {
	chain := &mockChain{
		response: &domain.Response{
			Body:           []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`),
			HTTPStatusCode: http.StatusOK,
		},
	}
	sessions := &mockSessions{ready: true, services: map[domain.ServiceID]struct{}{
		"eth": {},
	}}

	r := newTestRouter(t, chain, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	resp, err := http.Post(srv.URL+"/v1", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if !chain.called {
		t.Error("expected chain.HandleRelay to be called")
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHandleRelay_ChainError_WritesJSONRPCError(t *testing.T) {
	chain := &mockChain{err: domain.NewRelayError(domain.ErrTransport, "upstream unavailable", nil, true)}
	sessions := &mockSessions{ready: true}

	r := newTestRouter(t, chain, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":42}`
	resp, err := http.Post(srv.URL+"/v1", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// JSON-RPC errors are delivered with HTTP 200.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("JSON-RPC errors should be 200, got %d", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if _, ok := out["error"]; !ok {
		t.Error("expected 'error' field in JSON-RPC error response")
	}
}

func TestHandleRelay_PathVariant_POST(t *testing.T) {
	chain := &mockChain{
		response: &domain.Response{Body: []byte(`OK`), HTTPStatusCode: http.StatusOK},
	}
	sessions := &mockSessions{ready: true}

	r := newTestRouter(t, chain, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/cosmos/base/tendermint/v1beta1/blocks/latest",
		"application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if !chain.called {
		t.Error("expected chain.HandleRelay to be called for POST /v1/{path...}")
	}
}

func TestHandleRelay_PathVariant_GET(t *testing.T) {
	chain := &mockChain{
		response: &domain.Response{Body: []byte(`OK`), HTTPStatusCode: http.StatusOK},
	}
	sessions := &mockSessions{ready: true}

	r := newTestRouter(t, chain, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if !chain.called {
		t.Error("expected chain.HandleRelay to be called for GET /v1/{path...}")
	}
}

// pathRecorder captures the request URI the chain sees, which is the URI the
// relay ultimately replays against the supplier's backend.
type pathRecorder struct {
	requestURI string
}

func (p *pathRecorder) HandleRelay(ctx *relay.Context) error {
	p.requestURI = ctx.HTTPRequest.URL.RequestURI()
	ctx.Response = &domain.Response{Body: []byte(`OK`), HTTPStatusCode: http.StatusOK}
	return nil
}

// The "/v1" mount point is the gateway's, not the service's. Leaving it on the
// path relays "/v1/status" to the supplier's backend — a 404 — and also breaks
// RPC type detection, which matches on "/status" and "/cosmos/".
func TestHandleRelay_StripsMountPrefixFromServicePath(t *testing.T) {
	tests := []struct {
		name   string
		method string
		url    string
		want   string
	}{
		{"comet path", http.MethodGet, "/v1/status", "/status"},
		{"comet path with query", http.MethodGet, "/v1/block?height=42", "/block?height=42"},
		{"rest path", http.MethodGet, "/v1/cosmos/bank/v1beta1/params", "/cosmos/bank/v1beta1/params"},
		{"bare mount point", http.MethodPost, "/v1", "/"},
		{"trailing slash", http.MethodPost, "/v1/", "/"},
		{"nested path", http.MethodPost, "/v1/a/b/c", "/a/b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &pathRecorder{}
			r := newTestRouter(t, rec, &mockSessions{ready: true})
			srv := httptest.NewServer(r.mux)
			defer srv.Close()

			req, err := http.NewRequest(tt.method, srv.URL+tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if rec.requestURI != tt.want {
				t.Errorf("service sees %q, want %q", rec.requestURI, tt.want)
			}
		})
	}
}

// --- Health endpoint tests ---

func TestHandleHealth_Ready(t *testing.T) {
	sessions := &mockSessions{ready: true}
	r := newTestRouter(t, relay.Noop, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health status = %d, want 200", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["status"] != "ok" {
		t.Errorf("status = %q, want %q", out["status"], "ok")
	}
}

func TestHandleHealth_NotReady(t *testing.T) {
	sessions := &mockSessions{ready: false}
	r := newTestRouter(t, relay.Noop, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("health status = %d, want 503", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["status"] != "unavailable" {
		t.Errorf("status = %q, want %q", out["status"], "unavailable")
	}
}

// --- Ready endpoint tests ---

func TestHandleReadyService_Found(t *testing.T) {
	sessions := &mockSessions{
		ready:    true,
		services: map[domain.ServiceID]struct{}{"eth": {}},
	}
	r := newTestRouter(t, relay.Noop, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ready/eth")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("ready status = %d, want 200", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["ready"] != true {
		t.Errorf("ready = %v, want true", out["ready"])
	}
	if out["service"] != "eth" {
		t.Errorf("service = %v, want eth", out["service"])
	}
}

func TestHandleReadyService_NotFound(t *testing.T) {
	sessions := &mockSessions{ready: true, services: map[domain.ServiceID]struct{}{}}
	r := newTestRouter(t, relay.Noop, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ready/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("ready status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleReadyAll(t *testing.T) {
	sessions := &mockSessions{
		ready:    true,
		services: map[domain.ServiceID]struct{}{"eth": {}, "solana": {}},
	}
	r := newTestRouter(t, relay.Noop, sessions)
	srv := httptest.NewServer(r.mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("ready status = %d, want 200", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	svcs, ok := out["services"].(map[string]any)
	if !ok {
		t.Fatalf("expected 'services' map, got %T", out["services"])
	}
	if _, ok := svcs["eth"]; !ok {
		t.Error("expected 'eth' in services map")
	}
	if _, ok := svcs["solana"]; !ok {
		t.Error("expected 'solana' in services map")
	}
}

// --- Unit tests for helpers ---

func TestExtractJSONRPCID(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		want    string
	}{
		{"numeric id", []byte(`{"id":1}`), "1"},
		{"string id", []byte(`{"id":"req-1"}`), `"req-1"`},
		{"null id", []byte(`{"id":null}`), "null"},
		{"missing id", []byte(`{}`), "null"},
		{"invalid json", []byte(`not json`), "null"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(extractJSONRPCID(tc.payload))
			if got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

// --- WebSocket routing tests ---

func TestHandleMaybeWebSocket_UpgradeRoutesToWSOpener(t *testing.T) {
	chain := &mockChain{}
	ws := &mockWSOpener{}
	r := newTestRouterWithWS(t, chain, &mockSessions{ready: true}, ws)

	req := httptest.NewRequest(http.MethodGet, "/v1", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Target-Service-Id", "eth")
	w := httptest.NewRecorder()

	r.mux.ServeHTTP(w, req)

	if !ws.called {
		t.Error("expected WSOpener.Open to be called for upgrade request")
	}
	if ws.serviceID != "eth" {
		t.Errorf("ws service id = %q, want %q", ws.serviceID, "eth")
	}
	if chain.called {
		t.Error("relay chain should not be invoked for WS upgrade request")
	}
}

func TestHandleMaybeWebSocket_NonUpgradeGETGoesToRelayChain(t *testing.T) {
	chain := &mockChain{response: &domain.Response{Body: []byte(`ok`), HTTPStatusCode: 200}}
	ws := &mockWSOpener{}
	r := newTestRouterWithWS(t, chain, &mockSessions{ready: true}, ws)

	req := httptest.NewRequest(http.MethodGet, "/v1/some/path", nil)
	req.Header.Set("Target-Service-Id", "eth")
	w := httptest.NewRecorder()

	r.mux.ServeHTTP(w, req)

	if ws.called {
		t.Error("WSOpener should NOT be called for non-upgrade GET")
	}
	if !chain.called {
		t.Error("relay chain should be invoked for non-upgrade GET")
	}
}

func TestHandleMaybeWebSocket_NilWSRelayer_Returns503(t *testing.T) {
	r := newTestRouter(t, &mockChain{}, &mockSessions{ready: true}) // no WS

	req := httptest.NewRequest(http.MethodGet, "/v1", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Target-Service-Id", "eth")
	w := httptest.NewRecorder()

	r.mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when wsRelayer is nil", w.Code)
	}
}

func TestHandleMaybeWebSocket_UpgradeWithoutServiceID_Returns400(t *testing.T) {
	ws := &mockWSOpener{}
	r := newTestRouterWithWS(t, &mockChain{}, &mockSessions{ready: true}, ws)

	req := httptest.NewRequest(http.MethodGet, "/v1", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	// No Target-Service-Id.
	w := httptest.NewRecorder()

	r.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
	if ws.called {
		t.Error("WSOpener should not be called without service id")
	}
}

func TestIsWebSocketUpgrade(t *testing.T) {
	cases := []struct {
		name string
		hdr  map[string]string
		want bool
	}{
		{"canonical upgrade", map[string]string{"Connection": "Upgrade", "Upgrade": "websocket"}, true},
		{"mixed case upgrade", map[string]string{"Connection": "keep-alive, Upgrade", "Upgrade": "WebSocket"}, true},
		{"missing upgrade", map[string]string{"Connection": "Upgrade"}, false},
		{"non-ws upgrade", map[string]string{"Connection": "Upgrade", "Upgrade": "h2c"}, false},
		{"plain request", map[string]string{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1", nil)
			for k, v := range tc.hdr {
				req.Header.Set(k, v)
			}
			if got := isWebSocketUpgrade(req); got != tc.want {
				t.Errorf("isWebSocketUpgrade = %v, want %v", got, tc.want)
			}
		})
	}
}

// The X-Degraded header is emitted here rather than by the middleware that
// degrades, because SelectEndpoint runs inside the batch and hedge fan-outs and
// cannot know whether the attempt it degraded is the one being answered with.
// By the time the chain returns, ctx.Degraded has been merged and settled.
func TestHandleRelay_EmitsDegradedHeaderFromContext(t *testing.T) {
	cases := []struct {
		name     string
		degraded bool
		want     string
	}{
		{"degraded relay is marked", true, "true"},
		{"a normal relay is not", false, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chain := &mockChain{
				response: &domain.Response{
					Body:           []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`),
					HTTPStatusCode: http.StatusOK,
				},
				degraded: tc.degraded,
			}
			sessions := &mockSessions{ready: true, services: map[domain.ServiceID]struct{}{"eth": {}}}

			r := newTestRouter(t, chain, sessions)
			srv := httptest.NewServer(r.mux)
			defer srv.Close()

			body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
			resp, err := http.Post(srv.URL+"/v1", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if got := resp.Header.Get(relay.HeaderDegraded); got != tc.want {
				t.Errorf("%s = %q, want %q", relay.HeaderDegraded, got, tc.want)
			}
		})
	}
}

// TestRelayRouter_DoesNotServeAdmin is a security regression guard. The admin
// API has no authentication, and it used to be mounted on this same mux — so it
// answered on whatever port relays arrive on, and only network topology kept it
// off the internet. It gets its own listener now (see cmd/sagegw); if it ever
// reappears here, that protection is silently gone again.
func TestRelayRouter_DoesNotServeAdmin(t *testing.T) {
	chain := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: []byte(`{"ok":true}`), HTTPStatusCode: 200}
		return nil
	})
	r := newTestRouter(t, chain, &mockSessions{})

	// The mutating routes are the ones that matter: flipping shadow_mode alone
	// stops the gateway serving anything.
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/admin/flags"},
		{http.MethodPut, "/admin/flags/shadow_mode"},
		{http.MethodPut, "/admin/flags/shadow_mode/eth"},
		{http.MethodGet, "/admin/config"},
		{http.MethodPost, "/admin/reputation/reset/eth/some-endpoint"},
		{http.MethodPost, "/admin/circuit-breaker/clear/eth"},
		{http.MethodGet, "/admin/reputation/eth"},
		{http.MethodGet, "/admin/timeline/eth"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"enabled":true}`))
			w := httptest.NewRecorder()
			r.mux.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Errorf("relay router answered %s %s with %d, want 404 — the admin API must not be reachable on the relay port",
					tc.method, tc.path, w.Code)
			}
		})
	}
}

// TestHandleRelay_MiddlewareWriteThenErrorIsSingleResponse is the double-write
// regression. A middleware that rejects a request writes a terse error body via
// ctx.Writer and returns the error (parse, validate, batch all do this). The
// router used to write again to the raw ResponseWriter, so the client received
// two JSON objects concatenated — invalid JSON. The router now writes through
// the same guarded writer, so the middleware's response stands alone.
func TestHandleRelay_MiddlewareWriteThenErrorIsSingleResponse(t *testing.T) {
	const clientBody = `{"error":"failed to parse request"}`
	chain := relay.HandlerFunc(func(ctx *relay.Context) error {
		// Reproduce middleware/parse.go: answer the client, then return the error.
		ctx.Writer.SetStatusCode(http.StatusBadRequest)
		_ = ctx.Writer.Write([]byte(clientBody))
		return domain.NewRelayError(domain.ErrValidation, "failed to parse request",
			io.EOF, false)
	})
	r := newTestRouter(t, chain, &mockSessions{})

	req := httptest.NewRequest(http.MethodPost, "/v1", strings.NewReader("not json"))
	req.Header.Set("Target-Service-Id", "eth")
	w := httptest.NewRecorder()
	r.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d (the middleware's, not the router's)", w.Code, http.StatusBadRequest)
	}
	// The whole point: exactly one JSON value, and it parses.
	body := w.Body.Bytes()
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("response body is not a single valid JSON value: %v\nbody: %q", err, body)
	}
	if got := strings.TrimSpace(string(body)); got != clientBody {
		t.Errorf("body = %q, want the middleware's body %q", got, clientBody)
	}
}

// TestHandleRelay_DeepErrorStillWritten is the other half: when nothing in the
// chain answered — a send failure, say — the router is the writer, so a bare
// returned error must still reach the client as one valid JSON-RPC error.
func TestHandleRelay_DeepErrorStillWritten(t *testing.T) {
	chain := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.RPCType = domain.RPCTypeJSONRPC
		return domain.NewRelayError(domain.ErrTransport, "supplier unreachable", nil, true)
	})
	r := newTestRouter(t, chain, &mockSessions{})

	req := httptest.NewRequest(http.MethodPost, "/v1", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`))
	req.Header.Set("Target-Service-Id", "eth")
	w := httptest.NewRecorder()
	r.mux.ServeHTTP(w, req)

	var resp jsonRPCError
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("router did not write a valid JSON-RPC error for an unanswered chain error: %v\nbody: %q", err, w.Body.Bytes())
	}
	if resp.Error.Message == "" {
		t.Error("JSON-RPC error has no message")
	}
}

// A gRPC-Web client reads its status from a final in-body frame and rejects a
// reply without one; a native gRPC client would decode that same frame as a
// second message. The relay carries only a body, so the framing the client
// asked for has to be rebuilt on the way out.
func TestHandleRelay_GRPCResponseFraming(t *testing.T) {
	message := []byte{0, 0, 0, 0, 4, 0x0a, 0x02, 0x10, 0x01}

	tests := []struct {
		name            string
		requestType     string
		wantTrailerLast bool
	}{
		{"grpc-web gets a trailer frame", "application/grpc-web+proto", true},
		{"native grpc does not", "application/grpc", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := relay.HandlerFunc(func(ctx *relay.Context) error {
				ctx.RPCType = domain.RPCTypeGRPC
				ctx.Response = &domain.Response{
					Body:           message,
					HTTPStatusCode: http.StatusOK,
					Headers:        map[string]string{"grpc-status": "0"},
				}
				return nil
			})

			r := newTestRouter(t, chain, &mockSessions{ready: true})
			srv := httptest.NewServer(r.mux)
			defer srv.Close()

			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/cosmos.bank.v1beta1.Query/Params",
				bytes.NewReader(nil))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", tt.requestType)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if got := resp.Header.Get("Content-Type"); got != tt.requestType {
				t.Errorf("Content-Type = %q, want %q", got, tt.requestType)
			}
			if got := resp.Header.Get("Grpc-Status"); got != "0" {
				t.Errorf("Grpc-Status = %q, want 0", got)
			}

			hasTrailer := len(body) > len(message) && body[len(message)] == 0x80
			if hasTrailer != tt.wantTrailerLast {
				t.Errorf("trailer frame present = %v, want %v (body %x)", hasTrailer, tt.wantTrailerLast, body)
			}
			if !bytes.HasPrefix(body, message) {
				t.Errorf("the message frame was not passed through intact: %x", body)
			}
		})
	}
}
