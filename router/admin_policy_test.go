package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/tuning"
)

// PUT sets a party's policy penalty, GET lists it, DELETE lifts it; each
// mutation is observed through the list. Bad input is refused.
func TestAdmin_PolicyPenalty(t *testing.T) {
	repSvc := reputation.NewService(reputation.NewMemoryStorage(), nil, reputation.ServiceConfig{})
	admin := NewAdminAPI(newMockFlagStore(), repSvc, reputation.NewTimeline(10), circuitbreaker.New(), nil, nil, nil, 0, qos.NewRegistry(), tuning.NewStore(), nil, nil, discardLogger())
	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)
	do := func(method, path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
		return rec
	}
	list := func() []reputation.PolicyPenalty {
		t.Helper()
		var out struct {
			PolicyPenalties []reputation.PolicyPenalty `json:"policy_penalties"`
		}
		rec := do(http.MethodGet, "/admin/reputation/policy", "")
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
			t.Fatalf("list: %d %s", rec.Code, rec.Body)
		}
		return out.PolicyPenalties
	}

	for _, bad := range []string{
		`{"penalty": 0, "reason": "r"}`,
		`{"penalty": 10, "reason": "r"}`,
		`{"penalty": -101, "reason": "r"}`,
		`{"penalty": -30}`,
		`{"penalty": -30, "reason": "r", "until": "2020-01-01T00:00:00Z"}`,
		`not json`,
	} {
		if rec := do(http.MethodPut, "/admin/reputation/policy/pokt1owner", bad); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", bad, rec.Code)
		}
	}
	if got := list(); len(got) != 0 {
		t.Fatalf("a refused request was stored: %+v", got)
	}

	rec := do(http.MethodPut, "/admin/reputation/policy/POKT1Owner", `{"penalty": -30, "reason": "resells a public RPC"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("set: %d %s", rec.Code, rec.Body)
	}
	got := list()
	if len(got) != 1 || got[0].Party != "pokt1owner" || got[0].Penalty != -30 || got[0].Reason != "resells a public RPC" || !got[0].Until.IsZero() {
		t.Fatalf("listed %+v, want pokt1owner -30 with its reason and no expiry", got)
	}

	if rec := do(http.MethodDelete, "/admin/reputation/policy/pokt1owner", ""); rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	if got := list(); len(got) != 0 {
		t.Fatalf("still listed after delete: %+v", got)
	}
	if rec := do(http.MethodDelete, "/admin/reputation/policy/pokt1owner", ""); rec.Code != http.StatusNotFound {
		t.Errorf("second delete: %d, want 404", rec.Code)
	}
}
