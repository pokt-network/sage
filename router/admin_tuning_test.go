package router

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/tuning"
)

// doTuning issues a request against the admin test server and returns the
// status and decoded body.
func doTuning(t *testing.T, srv string, method, path, body string) (int, map[string]any) {
	t.Helper()

	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, srv+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func TestAdminTuning_SetAndClear(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()

	// A global override, then a narrower per-service one.
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/tuning/"+tuning.KnobRetryMaxRetries, `{"value":"5"}`); status != http.StatusOK {
		t.Fatalf("set global: status %d", status)
	}
	if status, _ := doTuning(t, srv.URL, http.MethodPut, "/admin/tuning/"+tuning.KnobRetryMaxRetries+"/eth", `{"value":"1"}`); status != http.StatusOK {
		t.Fatalf("set per-service: status %d", status)
	}

	// Assert through the store the middlewares read, not through the listing:
	// a handler that answered 200 and stored nothing would pass the listing.
	if got := admin.tuning.Int(tuning.KnobRetryMaxRetries, "eth", 2); got != 1 {
		t.Fatalf("eth resolves to %d, want the per-service 1", got)
	}
	if got := admin.tuning.Int(tuning.KnobRetryMaxRetries, "poly", 2); got != 5 {
		t.Fatalf("poly resolves to %d, want the global 5", got)
	}

	// Clearing the global must leave the narrower statement standing.
	status, body := doTuning(t, srv.URL, http.MethodDelete, "/admin/tuning/"+tuning.KnobRetryMaxRetries, "")
	if status != http.StatusOK {
		t.Fatalf("clear global: status %d", status)
	}
	if cleared, _ := body["cleared"].(bool); !cleared {
		t.Fatal("clearing an override that existed should report cleared=true")
	}
	if got := admin.tuning.Int(tuning.KnobRetryMaxRetries, "eth", 2); got != 1 {
		t.Fatalf("eth resolves to %d after the global was cleared, want 1", got)
	}
	if got := admin.tuning.Int(tuning.KnobRetryMaxRetries, "poly", 2); got != 2 {
		t.Fatalf("poly resolves to %d, want the config base 2 back", got)
	}
}

func TestAdminTuning_RejectsBadValues(t *testing.T) {
	admin, srv := newAdminServer(t)
	defer srv.Close()

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "unknown knob",
			path: "/admin/tuning/retry.max_retrys",
			body: `{"value":"3"}`,
		},
		{
			name: "wrong type",
			path: "/admin/tuning/" + tuning.KnobRetryMaxRetries,
			body: `{"value":"250ms"}`,
		},
		{
			name: "out of range",
			path: "/admin/tuning/" + tuning.KnobRelayTimeout,
			body: `{"value":"900s"}`,
		},
		{
			name: "malformed body",
			path: "/admin/tuning/" + tuning.KnobRelayTimeout,
			body: `{"value":`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := doTuning(t, srv.URL, http.MethodPut, tt.path, tt.body)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %v)", status, body)
			}
		})
	}

	// Nothing rejected may have been stored: a refusal that still takes effect
	// is worse than either outcome on its own.
	if got := admin.tuning.Int(tuning.KnobRetryMaxRetries, "eth", 2); got != 2 {
		t.Fatalf("a rejected value was stored anyway: got %d, want the base 2", got)
	}
	if got := admin.tuning.Duration(tuning.KnobRelayTimeout, "eth", 0); got != 0 {
		t.Fatalf("a rejected duration was stored anyway: got %s", got)
	}
}

func TestAdminTuning_ListsEveryKnob(t *testing.T) {
	_, srv := newAdminServer(t)
	defer srv.Close()

	status, body := doTuning(t, srv.URL, http.MethodGet, "/admin/tuning", "")
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}

	knobs, ok := body["knobs"].(map[string]any)
	if !ok {
		t.Fatalf("response carries no knobs object: %v", body)
	}
	if len(knobs) != len(tuning.Knobs) {
		t.Fatalf("listed %d knobs, want all %d", len(knobs), len(tuning.Knobs))
	}
	if body["note"] == nil {
		t.Fatal("the listing must say overrides do not survive a restart")
	}
}
