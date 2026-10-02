package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// The route forgets the operator's counters: its rate is there before and gone
// after, and an operator with nothing recorded is a 404.
func TestAdmin_ResetOperatorForgetsTheOperatorRate(t *testing.T) {
	rep := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	ep := domain.EndpointAddr("s1-https://rm01.op.example")
	for i := 0; i < 300; i++ {
		_ = rep.RecordSignal(context.Background(), "eth", ep, domain.RPCTypeJSONRPC, reputation.NewSignal(reputation.SignalCriticalError, "down", 0))
	}
	if _, ok := rep.OperatorRate("eth", domain.RPCTypeJSONRPC, "op.example"); !ok {
		t.Fatal("setup: no operator rate recorded")
	}
	admin := newTestAdmin(newMockFlagStore(), rep, reputation.NewTimeline(10), circuitbreaker.New(), qos.NewRegistry())
	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/reputation/operator-reset/eth/op.example", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"reset":1`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, ok := rep.OperatorRate("eth", domain.RPCTypeJSONRPC, "op.example"); ok {
		t.Fatal("the operator rate survived the reset")
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/reputation/operator-reset/eth/nobody.example", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown operator: status=%d, want 404", rec.Code)
	}
}
