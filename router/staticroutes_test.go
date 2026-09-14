package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// A static route is answered by the gateway on the path the service sees,
// with the configured status, type, headers and body, and never reaches the
// relay chain; any other path still does. Register row: static_routes.
func TestContract_StaticRouteAnsweredWithoutTheRelay(t *testing.T) {
	chain := okChain()
	r := newTestRouter(t, chain, &mockSessions{ready: true})
	r.SetStaticRoutes(func(svc domain.ServiceID, path, method string) (config.StaticRoute, bool) {
		if svc == "eth" && path == "/identity" {
			return config.StaticRoute{Path: path, Body: "0xpayout", Headers: map[string]string{"Cache-Control": "max-age=60", "Content-Type": "ignored"}}, true
		}
		return config.StaticRoute{}, false
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/identity", nil)
	req.Header.Set("Target-Service-Id", "eth")
	req.Header.Set("Origin", "https://dapp.example")
	rec := serve(t, r, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "0xpayout" {
		t.Fatalf("GET /v1/identity = %d %q, want 200 0xpayout", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want the route's resolved type, not the headers map's", got)
	}
	if rec.Header().Get("Cache-Control") != "max-age=60" || rec.Header().Get("Access-Control-Allow-Origin") != "https://dapp.example" {
		t.Errorf("headers = %v, want the route's extra header and CORS", rec.Header())
	}
	if chain.called {
		t.Fatal("a static route must not reach the relay chain")
	}

	req = httptest.NewRequest(http.MethodPost, "/v1", nil)
	req.Header.Set("Target-Service-Id", "eth")
	serve(t, r, req)
	if !chain.called {
		t.Fatal("a path no route matches must still reach the relay chain")
	}
}
