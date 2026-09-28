package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// The debug routes map the probe's refusals to statuses, pass the request
// through, and answer 501 when no probe is wired.
func TestAdmin_DebugRoutes(t *testing.T) {
	admin := newTestAdmin(newMockFlagStore(), newMockRepService(), reputation.NewTimeline(10), circuitbreaker.New(), qos.NewRegistry())
	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)
	post := func(path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
		return rec
	}
	if rec := post("/admin/debug/relay", `{}`); rec.Code != http.StatusNotImplemented {
		t.Fatalf("unwired: %d", rec.Code)
	}

	var gotType domain.RPCType
	var gotDuration time.Duration
	admin.SetDebugProbe(
		func(_ context.Context, _ domain.ServiceID, target string, rpcType domain.RPCType, _ []byte) (any, error) {
			gotType = rpcType
			switch target {
			case "gone":
				return nil, fmt.Errorf("%w: x", protocol.ErrDebugTargetNotFound)
			case "busy":
				return nil, fmt.Errorf("%w: x", protocol.ErrDebugBusy)
			}
			return map[string]string{"ok": target}, nil
		},
		func(_ context.Context, _ domain.ServiceID, _ string, _ []byte, d time.Duration, _ int, _, _ bool) (any, error) {
			gotDuration = d
			return map[string]string{"stop": "duration"}, nil
		})

	for _, tc := range []struct {
		path, body string
		want       int
	}{
		{"/admin/debug/relay", `{"service_id":"eth","target":"op.example","payload":{"method":"eth_blockNumber"}}`, http.StatusOK},
		{"/admin/debug/relay", `{"service_id":"eth","target":"gone","payload":{}}`, http.StatusNotFound},
		{"/admin/debug/relay", `{"service_id":"eth","target":"busy","payload":{}}`, http.StatusTooManyRequests},
		{"/admin/debug/relay", `{"service_id":"eth"}`, http.StatusBadRequest},
		{"/admin/debug/relay", `not json`, http.StatusBadRequest},
		{"/admin/debug/ws-subscribe", `{"service_id":"eth","target":"op.example","payload":{},"duration_s":30}`, http.StatusOK},
	} {
		if rec := post(tc.path, tc.body); rec.Code != tc.want {
			t.Errorf("%s %s: %d %s, want %d", tc.path, tc.body, rec.Code, rec.Body.String(), tc.want)
		}
	}
	if gotType != domain.RPCTypeJSONRPC || gotDuration != 30*time.Second {
		t.Fatalf("rpc_type=%q duration=%v", gotType, gotDuration)
	}
}
