package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

type spyRebinder struct{ asked []domain.ServiceID }

func (s *spyRebinder) RebindService(id domain.ServiceID) int {
	s.asked = append(s.asked, id)
	return 2
}

func TestAdmin_WebSocketRebind(t *testing.T) {
	admin, mux := newTestAdminWithDrain(t, nil, nil, 0)
	spy := &spyRebinder{}
	admin.SetWebSocketRebinder(spy)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/websocket/rebind/eth", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"bridges":2`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(spy.asked) != 1 || spy.asked[0] != "eth" {
		t.Fatalf("asked = %v", spy.asked)
	}
}

func TestAdmin_WebSocketRebind_NotWired(t *testing.T) {
	_, mux := newTestAdminWithDrain(t, nil, nil, 0)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/websocket/rebind/eth", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want 501", rec.Code)
	}
}

func TestAdmin_WebSocketClients(t *testing.T) {
	admin, mux := newTestAdminWithDrain(t, nil, nil, 0)
	var gotService domain.ServiceID
	var gotLimit int
	var gotShopping bool
	admin.SetWebSocketClients(func(s domain.ServiceID, limit int, onlyShopping bool) any {
		gotService, gotLimit, gotShopping = s, limit, onlyShopping
		return map[string]any{"clients": []map[string]any{{"client_ip": "203.0.113.7", "quick_client_closes": 5}}}
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/websocket/clients?service=robinhood&limit=10&shopping=true", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"quick_client_closes":5`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if gotService != "robinhood" || gotLimit != 10 || !gotShopping {
		t.Fatalf("report asked for service=%q limit=%d shopping=%v", gotService, gotLimit, gotShopping)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/websocket/clients?limit=-1", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative limit: status=%d, want 400", rec.Code)
	}
}

func TestAdmin_WebSocketClients_NotWired(t *testing.T) {
	_, mux := newTestAdminWithDrain(t, nil, nil, 0)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/websocket/clients", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want 501", rec.Code)
	}
}

func TestAdmin_WebSocketNotificationSamples(t *testing.T) {
	admin, mux := newTestAdminWithDrain(t, nil, nil, 0)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/websocket/notification-samples", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("not wired: status=%d, want 501", rec.Code)
	}

	var gotService domain.ServiceID
	admin.SetWebSocketNotificationSamples(func(s domain.ServiceID) any {
		gotService = s
		return []map[string]any{{"operator": "op.example", "kind": "tx", "hash": "0xabc"}}
	})
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/websocket/notification-samples?service=gnosis", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"hash":"0xabc"`) || gotService != "gnosis" {
		t.Fatalf("status=%d body=%s service=%q", rec.Code, rec.Body.String(), gotService)
	}
}
