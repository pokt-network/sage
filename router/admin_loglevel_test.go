package router

import (
	"log/slog"
	"net/http"
	"testing"
)

// The route exists so a live problem can be looked at without a restart:
// the pods that needed it ran at error with their config in a sealed secret.
// The mutation must be observable on the LevelVar the logger reads.
func TestAdminLogLevel_SetMovesTheLevel(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()

	lv := new(slog.LevelVar)
	lv.Set(slog.LevelError)
	admin.SetLogLevel(lv)

	status, body := doTuning(t, srv.URL, http.MethodGet, "/admin/log-level", "")
	if status != http.StatusOK || body["level"] != "error" {
		t.Fatalf("GET before: status %d body %v, want 200 and error", status, body)
	}

	status, body = doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"DEBUG"}`)
	if status != http.StatusOK {
		t.Fatalf("PUT debug: status %d body %v", status, body)
	}
	if body["level"] != "debug" || body["previous"] != "error" {
		t.Fatalf("PUT debug: body %v, want level debug previous error", body)
	}
	if lv.Level() != slog.LevelDebug {
		t.Fatalf("LevelVar = %v after PUT debug, want debug", lv.Level())
	}

	status, body = doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"verbose"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("PUT verbose: status %d body %v, want 400", status, body)
	}
	if lv.Level() != slog.LevelDebug {
		t.Fatalf("a rejected level must not move the LevelVar, got %v", lv.Level())
	}

	if status, _ = doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"error"}`); status != http.StatusOK {
		t.Fatalf("PUT error back: status %d", status)
	}
	if lv.Level() != slog.LevelError {
		t.Fatalf("LevelVar = %v after PUT error, want error", lv.Level())
	}
}

// An AdminAPI that was never given the process's LevelVar must say so, not
// answer 200 while changing nothing.
func TestAdminLogLevel_UnavailableWithoutLevelVar(t *testing.T) {
	_, srv := newAdminServer(t)
	defer srv.Close()

	if status, _ := doTuning(t, srv.URL, http.MethodGet, "/admin/log-level", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("GET: status %d, want 503", status)
	}
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"debug"}`); status != http.StatusServiceUnavailable {
		t.Fatalf("PUT: status %d, want 503", status)
	}
}
