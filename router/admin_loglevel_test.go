package router

import (
	"context"
	"log/slog"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/override"
)

// The route exists so a live problem can be looked at without a restart:
// the pods that needed it ran at error with their config in a sealed secret.
// The mutation must be observable on the LevelVar the logger reads, and in
// the override store other replicas and the next restart read.
func TestAdminLogLevel_SetPersistsAndClearRestoresBase(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()

	lv := new(slog.LevelVar)
	lv.Set(slog.LevelError)
	admin.SetLogLevel(lv, slog.LevelError)
	store := override.NewMemoryStore()
	admin.SetOverrides(store)

	status, body := doTuning(t, srv.URL, http.MethodGet, "/admin/log-level", "")
	if status != http.StatusOK || body["level"] != "error" || body["base"] != "error" || body["override"] != nil {
		t.Fatalf("GET before: status %d body %v", status, body)
	}

	status, body = doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"DEBUG"}`)
	if status != http.StatusOK || body["level"] != "debug" || body["previous"] != "error" {
		t.Fatalf("PUT debug: status %d body %v", status, body)
	}
	if lv.Level() != slog.LevelDebug {
		t.Fatalf("LevelVar = %v after PUT debug, want debug", lv.Level())
	}
	if v, ok, _ := store.Get(context.Background(), LogLevelOverrideKey); !ok || v != "debug" {
		t.Fatalf("override store = %q,%v, want debug persisted", v, ok)
	}
	if _, body = doTuning(t, srv.URL, http.MethodGet, "/admin/log-level", ""); body["override"] != "debug" {
		t.Fatalf("GET after PUT: %v, want override debug", body)
	}

	if status, body = doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"verbose"}`); status != http.StatusBadRequest {
		t.Fatalf("PUT verbose: status %d body %v, want 400", status, body)
	}
	if lv.Level() != slog.LevelDebug {
		t.Fatalf("a rejected level must not move the LevelVar, got %v", lv.Level())
	}

	status, body = doTuning(t, srv.URL, http.MethodDelete, "/admin/log-level", "")
	if status != http.StatusOK || body["removed"] != true || body["level"] != "error" {
		t.Fatalf("DELETE: status %d body %v", status, body)
	}
	if lv.Level() != slog.LevelError {
		t.Fatalf("LevelVar = %v after DELETE, want the base error", lv.Level())
	}
	if _, ok, _ := store.Get(context.Background(), LogLevelOverrideKey); ok {
		t.Fatal("DELETE must remove the persisted override")
	}
	if _, body = doTuning(t, srv.URL, http.MethodDelete, "/admin/log-level", ""); body["removed"] != false {
		t.Fatalf("second DELETE: %v, want removed false", body)
	}
}

// Without an override store the routes still work, per process.
func TestAdminLogLevel_WorksWithoutStore(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()
	lv := new(slog.LevelVar)
	lv.Set(slog.LevelInfo)
	admin.SetLogLevel(lv, slog.LevelInfo)
	if status, body := doTuning(t, srv.URL, http.MethodPut, "/admin/log-level", `{"level":"warn"}`); status != http.StatusOK || body["persisted"] != false {
		t.Fatalf("PUT: status %d body %v, want 200 persisted false", status, body)
	}
	if lv.Level() != slog.LevelWarn {
		t.Fatalf("LevelVar = %v, want warn", lv.Level())
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
