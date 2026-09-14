package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reload"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/tuning"
)

// stubReloader answers with whatever the test wants a reload to have done.
type stubReloader struct {
	result reload.Result
	err    error
	calls  int

	applied []byte
	cleared int
}

// Reload records the call and returns the canned answer.
func (s *stubReloader) Reload(_ context.Context) (reload.Result, error) {
	s.calls++
	return s.result, s.err
}

// ApplyConfig records the document and returns the canned answer.
func (s *stubReloader) ApplyConfig(_ context.Context, data []byte) (reload.Result, error) {
	s.applied = append([]byte(nil), data...)
	return s.result, s.err
}

// ClearConfigOverride records the call and returns the canned answer.
func (s *stubReloader) ClearConfigOverride(_ context.Context) (reload.Result, error) {
	s.cleared++
	return s.result, s.err
}

// newReloadAdmin builds an AdminAPI wired to r (which may be nil) and a mux
// serving its routes.
func newReloadAdmin(t *testing.T, r Reloader) *http.ServeMux {
	t.Helper()
	admin := NewAdminAPI(
		newMockFlagStore(), newMockRepService(), reputation.NewTimeline(10),
		circuitbreaker.New(), nil, nil, nil, 0, qos.NewRegistry(),
		tuning.NewStore(), r, nil, discardLogger(),
	)
	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)
	return mux
}

// doReload posts to /admin/reload and returns the recorder.
func doReload(t *testing.T, mux *http.ServeMux) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/reload", nil))
	return rec
}

// TestHandleReload_OK: the route's whole job is to hand the reload's own
// account back verbatim, including the sections that did NOT take effect.
func TestHandleReload_OK(t *testing.T) {
	stub := &stubReloader{result: reload.Result{
		Applied:       []string{"gateway_config.defaults", "feature_flags"},
		NeedsRestart:  []string{"gateway_config.services[eth].chain_id"},
		Ignored:       []string{},
		Inert:         []string{},
		Unimplemented: []string{},
		Warnings:      []string{},
	}}
	rec := doReload(t, newReloadAdmin(t, stub))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if stub.calls != 1 {
		t.Fatalf("reloader called %d times, want 1", stub.calls)
	}

	var got reload.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body %s)", err, rec.Body.String())
	}
	if len(got.Applied) != 2 || got.Applied[0] != "gateway_config.defaults" {
		t.Errorf("applied = %v, want the reloader's own list", got.Applied)
	}
	if len(got.NeedsRestart) != 1 {
		t.Errorf("needs_restart = %v, want the one entry the reloader reported", got.NeedsRestart)
	}
	// The empty lists have to survive as arrays: a client that renders
	// `needs_restart` cannot tell null from "nothing changed" without
	// special-casing it.
	body := rec.Body.String()
	if strings.Contains(body, "null") {
		t.Errorf("body carries a null where an array belongs: %s", body)
	}
}

// TestHandleReload_NoConfigFile: 409, not 400. A gateway started from
// GATEWAY_CONFIG has no file, which is a different thing from a file that did
// not validate — 400 would send an operator to edit something nothing reads.
func TestHandleReload_NoConfigFile(t *testing.T) {
	stub := &stubReloader{result: reload.NewResult(), err: reload.ErrNoConfigFile}
	rec := doReload(t, newReloadAdmin(t, stub))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "GATEWAY_CONFIG") {
		t.Errorf("body = %s, want it to say why there is nothing to reload", rec.Body.String())
	}
}

// TestHandleReload_InvalidConfig: a file that would not boot is the caller's
// problem, and the error text is the only thing that says which line.
func TestHandleReload_InvalidConfig(t *testing.T) {
	stub := &stubReloader{
		result: reload.NewResult(),
		err:    errors.New(`service "eth": chain_id "nope" is not hex`),
	}
	rec := doReload(t, newReloadAdmin(t, stub))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "chain_id") {
		t.Errorf("body = %s, want the validation error itself", rec.Body.String())
	}
}

// TestHandleReload_NoReloader: with nothing wired, the route exists and says
// so rather than 404ing, which would read as a typo in the URL.
func TestHandleReload_NoReloader(t *testing.T) {
	rec := doReload(t, newReloadAdmin(t, nil))

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

// TestHandleReload_MethodIsPOST: reloading is not a read, and a GET that
// happened to work would make it reachable from a browser address bar.
func TestHandleReload_MethodIsPOST(t *testing.T) {
	mux := newReloadAdmin(t, &stubReloader{result: reload.NewResult()})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/reload", nil))

	if rec.Code == http.StatusOK {
		t.Fatal("GET /admin/reload succeeded; the route must be POST-only")
	}
}

// PUT /admin/config hands the body to the reloader whole and reports what it
// applied; an empty body is refused before the reloader sees it; DELETE
// clears through the reloader.
func TestHandleApplyConfig_ForwardsBodyAndClears(t *testing.T) {
	stub := &stubReloader{result: reload.Result{Applied: []string{"gateway_config.defaults.retry_config"}}}
	mux := newReloadAdmin(t, stub)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	yaml := "gateway_config:\n  defaults:\n    retry_config:\n      hedge_delay: 25ms\n"
	status, body := doTuning(t, srv.URL, http.MethodPut, "/admin/config", yaml)
	if status != http.StatusOK {
		t.Fatalf("PUT: status %d body %v", status, body)
	}
	if string(stub.applied) != yaml {
		t.Fatalf("reloader received %q, want the body verbatim", stub.applied)
	}
	if applied, _ := body["applied"].([]any); len(applied) != 1 {
		t.Fatalf("response = %v, want the reload result", body)
	}

	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/config", "   \n"); status != http.StatusBadRequest {
		t.Fatalf("empty body: status %d, want 400", status)
	}

	stub.err = errors.New("validate config: full_node_config.rpc_url is required")
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/config", "nope: 1\n"); status != http.StatusBadRequest {
		t.Fatalf("refused document: status %d, want 400", status)
	}
	stub.err = nil

	if status, _ := doTuning(t, srv.URL, http.MethodDelete, "/admin/config", ""); status != http.StatusOK || stub.cleared != 1 {
		t.Fatalf("DELETE: status %d cleared %d", status, stub.cleared)
	}
	stub.err = reload.ErrNoConfigFile
	if status, _ := doTuning(t, srv.URL, http.MethodDelete, "/admin/config", ""); status != http.StatusConflict {
		t.Fatalf("DELETE without a file: status %d, want 409", status)
	}
}

// While an uploaded config is in force, re-reading the file is refused so it
// cannot silently undo the upload.
func TestHandleReload_RefusedWhileOverridden(t *testing.T) {
	stub := &stubReloader{result: reload.NewResult(), err: reload.ErrConfigOverridden}
	mux := newReloadAdmin(t, stub)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if status, _ := doTuning(t, srv.URL, http.MethodPost, "/admin/reload", ""); status != http.StatusConflict {
		t.Fatalf("reload while overridden: status %d, want 409", status)
	}
}
