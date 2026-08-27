package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/traffic"
	"github.com/pokt-network/sage/tuning"
)

// newTestAdminWithSampler builds an AdminAPI wired to sampler (which may be
// nil, to exercise the 503 path) and a mux serving its routes.
func newTestAdminWithSampler(t *testing.T, sampler *traffic.Sampler) *http.ServeMux {
	t.Helper()
	flags := newMockFlagStore()
	repSvc := newMockRepService()
	tl := reputation.NewTimeline(10)
	breaker := circuitbreaker.New()
	qosReg := qos.NewRegistry()
	admin := NewAdminAPI(flags, repSvc, tl, breaker, nil, nil, nil, 0, qosReg, tuning.NewStore(), nil, sampler, discardLogger())
	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)
	return mux
}

func jsonRPCPayload(method string) domain.Payload {
	body := []byte(`{"jsonrpc":"2.0","method":"` + method + `","params":[],"id":1}`)
	return domain.NewPayload(body, domain.RPCTypeJSONRPC, method)
}

// jsonRPCPayloadWithParam is jsonRPCPayload but with a distinct params member,
// so each call fingerprints differently.
func jsonRPCPayloadWithParam(method, param string) domain.Payload {
	body := []byte(`{"jsonrpc":"2.0","method":"` + method + `","params":["` + param + `"],"id":1}`)
	return domain.NewPayload(body, domain.RPCTypeJSONRPC, method)
}

func TestAdminRequestSample_ServiceUnavailable(t *testing.T) {
	mux := newTestAdminWithSampler(t, nil)

	for _, path := range []string{"/admin/request-sample", "/admin/request-sample/eth"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want 503", path, rec.Code)
		}
	}
}

func TestAdminRequestSample_UnknownService404(t *testing.T) {
	sampler := traffic.New(traffic.WithRate(1))
	mux := newTestAdminWithSampler(t, sampler)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample/eth", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body)
	}
}

func TestAdminRequestSample_ListAndGet(t *testing.T) {
	sampler := traffic.New(traffic.WithRate(1))
	sampler.Observe("eth", []domain.Payload{jsonRPCPayload("eth_blockNumber")})
	sampler.Observe("eth", []domain.Payload{jsonRPCPayload("eth_blockNumber")})
	sampler.Observe("eth", []domain.Payload{jsonRPCPayload("eth_chainId")})

	mux := newTestAdminWithSampler(t, sampler)

	// List: the service has only a current window (no roll yet), so the
	// entry must fall back to it and say so.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d: %s", rec.Code, rec.Body)
	}
	var list map[string]requestSampleEntry
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	entry, ok := list["eth"]
	if !ok {
		t.Fatal("expected an \"eth\" entry in the list response")
	}
	if entry.Window != "current" {
		t.Errorf("window = %q, want current (no window has rolled yet)", entry.Window)
	}
	if entry.Summary.Sampled != 3 {
		t.Errorf("sampled = %d, want 3", entry.Summary.Sampled)
	}

	// Get: explicit window=current, top=1.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample/eth?window=current&top=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", rec.Code, rec.Body)
	}
	var got struct {
		Summary traffic.Summary       `json:"summary"`
		Top     []traffic.Fingerprint `json:"top"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if got.Summary.Sampled != 3 {
		t.Errorf("sampled = %d, want 3", got.Summary.Sampled)
	}
	if got.Summary.Distinct != 2 {
		t.Errorf("distinct = %d, want 2", got.Summary.Distinct)
	}
	if len(got.Top) != 1 {
		t.Fatalf("len(top) = %d, want 1 (capped by ?top=1)", len(got.Top))
	}
}

func TestAdminRequestSample_KnownServiceNoPreviousWindow(t *testing.T) {
	sampler := traffic.New(traffic.WithRate(1))
	sampler.Observe("eth", []domain.Payload{jsonRPCPayload("eth_blockNumber")})

	mux := newTestAdminWithSampler(t, sampler)

	// Default window is "previous", which has not rolled yet: this must be a
	// 200 with an empty summary, not a 404 — the service is real.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample/eth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body)
	}
	var got struct {
		Summary traffic.Summary       `json:"summary"`
		Top     []traffic.Fingerprint `json:"top"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Summary.Sampled != 0 {
		t.Errorf("sampled = %d, want 0 (no previous window yet)", got.Summary.Sampled)
	}
	if len(got.Top) != 0 {
		t.Errorf("len(top) = %d, want 0", len(got.Top))
	}
}

func TestAdminRequestSample_InvalidWindowAndTop(t *testing.T) {
	sampler := traffic.New(traffic.WithRate(1))
	sampler.Observe("eth", []domain.Payload{jsonRPCPayload("eth_blockNumber")})
	mux := newTestAdminWithSampler(t, sampler)

	for _, q := range []string{"?window=sideways", "?top=-1", "?top=0", "?top=notanumber"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample/eth"+q, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, rec.Code)
		}
	}
}

func TestAdminRequestSample_TopCappedAt100(t *testing.T) {
	sampler := traffic.New(traffic.WithRate(1))
	for i := 0; i < 150; i++ {
		sampler.Observe("eth", []domain.Payload{jsonRPCPayloadWithParam("eth_getBalance", strconv.Itoa(i))})
	}
	mux := newTestAdminWithSampler(t, sampler)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample/eth?window=current&top=1000", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var got struct {
		Top []traffic.Fingerprint `json:"top"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Top) != 100 {
		t.Errorf("len(top) = %d, want 100 (150 distinct fingerprints, ?top=1000 must still cap at 100)", len(got.Top))
	}
}

// A ?top above the cap is clamped down to it, not merely accepted: this pins
// that 0 (the boundary Sampler.Top would otherwise treat as "unlimited") does
// not leak through as a way to bypass the 100-entry ceiling.
func TestAdminRequestSample_TopAboveCapIsClamped(t *testing.T) {
	sampler := traffic.New(traffic.WithRate(1))
	for i := 0; i < 150; i++ {
		sampler.Observe("eth", []domain.Payload{jsonRPCPayloadWithParam("eth_getBalance", strconv.Itoa(i))})
	}
	mux := newTestAdminWithSampler(t, sampler)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/request-sample/eth?window=current&top=500", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var got struct {
		Top []traffic.Fingerprint `json:"top"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Top) != 100 {
		t.Errorf("len(top) = %d, want 100 (?top=500 must clamp to the 100-entry cap)", len(got.Top))
	}
}
