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
