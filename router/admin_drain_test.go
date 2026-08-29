package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/tuning"
)

// fakeEndpointProvider implements protocol.EndpointProvider with a fixed list
// of endpoints, regardless of serviceID or rpcType, for tests that only care
// about which operators those endpoints belong to.
type fakeEndpointProvider struct {
	endpoints domain.EndpointAddrList
}

func (f *fakeEndpointProvider) AvailableEndpoints(_ context.Context, _ domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	return f.endpoints, nil
}

var _ protocol.EndpointProvider = (*fakeEndpointProvider)(nil)

// perTypeEndpointProvider implements protocol.EndpointProvider with a
// different endpoint set per RPC type, for tests where the last-operator
// check must not see a union across types.
type perTypeEndpointProvider struct {
	byType map[domain.RPCType]domain.EndpointAddrList
}

func (f *perTypeEndpointProvider) AvailableEndpoints(_ context.Context, _ domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error) {
	return f.byType[rpcType], nil
}

var _ protocol.EndpointProvider = (*perTypeEndpointProvider)(nil)

// stubErrStore wraps a drain.Store and makes Set fail outright, or Release
// fail after applying locally, for testing the propagation_error paths without
// a real Redis. Release delegates first because that is what RedisStore does:
// the local map is updated and only the Redis write fails.
type stubErrStore struct {
	drain.Store
	setErr     error
	releaseErr error
}

func (s *stubErrStore) Set(_ context.Context, _ drain.Entry) error {
	return s.setErr
}

func (s *stubErrStore) Release(ctx context.Context, k drain.Key) error {
	if err := s.Store.Release(ctx, k); err != nil {
		return err
	}
	return s.releaseErr
}

// twoOperatorEndpoints has one endpoint each at two distinct operators.
var twoOperatorEndpoints = domain.EndpointAddrList{
	"supplier1-https://rpc.example.com",
	"supplier2-https://rpc.example2.com",
}

// oneOperatorEndpoints has two endpoints that both resolve to the same
// operator (same registrable domain, different subdomains).
var oneOperatorEndpoints = domain.EndpointAddrList{
	"supplier1-https://a.only-example.com",
	"supplier2-https://b.only-example.com",
}

func newTestAdminWithDrain(t *testing.T, store drain.Store, endpoints protocol.EndpointProvider, maxDrain time.Duration) (*AdminAPI, *http.ServeMux) {
	t.Helper()
	flags := newMockFlagStore()
	repSvc := newMockRepService()
	tl := reputation.NewTimeline(100)
	breaker := circuitbreaker.New()
	qosReg := qos.NewRegistry()
	admin := NewAdminAPI(flags, repSvc, tl, breaker, nil, store, endpoints, maxDrain, qosReg, tuning.NewStore(), nil, nil, discardLogger())
	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)
	return admin, mux
}

type drainTestResponse struct {
	status int
	body   []byte
}

func doRequest(t *testing.T, mux *http.ServeMux, method, path, body string) drainTestResponse {
	t.Helper()
	req, err := http.NewRequest(method, path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return drainTestResponse{status: rec.Code, body: rec.Body.Bytes()}
}

func TestAdminDrain_SetAppliesAndReportsMatchedEndpoints(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","reason":"noisy"}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}

	var out struct {
		ServiceID        string        `json:"service_id"`
		Domain           string        `json:"domain"`
		Applied          bool          `json:"applied"`
		Released         bool          `json:"released"`
		DryRun           bool          `json:"dry_run"`
		MatchedEndpoints int           `json:"matched_endpoints"`
		DrainedUntil     *time.Time    `json:"drained_until"`
		PropagationError string        `json:"propagation_error"`
		ActiveDrains     []drain.Entry `json:"active_drains"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatalf("decode: %v, body=%s", err, res.body)
	}
	if out.ServiceID != "eth" || out.Domain != "example.com" {
		t.Errorf("service_id/domain = %q/%q", out.ServiceID, out.Domain)
	}
	if !out.Applied || out.Released || out.DryRun {
		t.Errorf("applied/released/dry_run = %v/%v/%v, want true/false/false", out.Applied, out.Released, out.DryRun)
	}
	if out.MatchedEndpoints != 1 {
		t.Errorf("matched_endpoints = %d, want 1", out.MatchedEndpoints)
	}
	if out.DrainedUntil == nil {
		t.Fatal("drained_until is nil")
	}
	wantUntil := time.Now().Add(30 * time.Minute)
	if diff := out.DrainedUntil.Sub(wantUntil); diff < -5*time.Second || diff > 5*time.Second {
		t.Errorf("drained_until = %v, want ~%v", out.DrainedUntil, wantUntil)
	}
	if out.PropagationError != "" {
		t.Errorf("propagation_error = %q, want empty", out.PropagationError)
	}
	if len(out.ActiveDrains) != 1 {
		t.Fatalf("active_drains = %+v, want 1 entry", out.ActiveDrains)
	}

	// The store itself now has the drain.
	active := store.Active(context.Background(), "eth")
	if len(active) != 1 || active[0].Operator != "example.com" {
		t.Errorf("store.Active = %+v", active)
	}
}

func TestAdminDrain_GetListsActiveDrains(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	// Empty case: never null.
	res := doRequest(t, mux, http.MethodGet, "/admin/reputation/drain/eth", "")
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.status)
	}
	if strings.TrimSpace(string(res.body)) != "[]" {
		t.Errorf("empty GET body = %s, want []", res.body)
	}

	doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`)

	res = doRequest(t, mux, http.MethodGet, "/admin/reputation/drain/eth", "")
	var entries []drain.Entry
	if err := json.Unmarshal(res.body, &entries); err != nil {
		t.Fatalf("decode: %v body=%s", err, res.body)
	}
	if len(entries) != 1 || entries[0].Operator != "example.com" {
		t.Errorf("entries = %+v", entries)
	}
}

func TestAdminDrain_OverCeilingRefused(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"72h"}`)
	if res.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", res.status, res.body)
	}
	if !strings.Contains(strings.ToLower(string(res.body)), "1h0m0s") && !strings.Contains(string(res.body), "1h") {
		t.Errorf("400 body should name the ceiling, got %s", res.body)
	}

	// Refused, not clamped: nothing should have been stored.
	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after a refused drain", active)
	}
}

func TestAdminDrain_LastOperatorRefused(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: oneOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"only-example.com","duration":"30m"}`)
	if res.status != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body=%s", res.status, res.body)
	}

	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after a refused drain", active)
	}
}

func TestAdminDrain_DurationZeroReleases(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`)
	if active := store.Active(context.Background(), "eth"); len(active) != 1 {
		t.Fatalf("setup: store.Active = %+v, want 1 entry", active)
	}

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"0s"}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		Released bool `json:"released"`
		Applied  bool `json:"applied"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Released || out.Applied {
		t.Errorf("released/applied = %v/%v, want true/false", out.Released, out.Applied)
	}
	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after release", active)
	}
}

func TestAdminDrain_DryRunLeavesStoreEmpty(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","dry_run":true}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		DryRun           bool `json:"dry_run"`
		MatchedEndpoints int  `json:"matched_endpoints"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if !out.DryRun {
		t.Error("dry_run = false, want true")
	}
	if out.MatchedEndpoints != 1 {
		t.Errorf("matched_endpoints = %d, want 1", out.MatchedEndpoints)
	}
	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after a dry run", active)
	}
}

func TestAdminDrain_DeleteReleasesEveryRPCType(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","rpc_type":"websocket"}`)
	doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","rpc_type":"json_rpc"}`)
	if active := store.Active(context.Background(), "eth"); len(active) != 2 {
		t.Fatalf("setup: store.Active = %+v, want 2 entries", active)
	}

	res := doRequest(t, mux, http.MethodDelete, "/admin/reputation/drain/eth/example.com", "")
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after DELETE", active)
	}
}

func TestAdminDrain_InvalidRPCTypeRejected(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","rpc_type":"carrier_pigeon"}`)
	if res.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", res.status, res.body)
	}
}

func TestAdminDrain_MissingDomainRejected(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"duration":"30m"}`)
	if res.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", res.status, res.body)
	}
}

func TestAdminDrain_UnscopedRefusedWhenAnyRPCTypePoolWouldCollapse(t *testing.T) {
	store := drain.NewMemoryStore()
	provider := &perTypeEndpointProvider{byType: map[domain.RPCType]domain.EndpointAddrList{
		domain.RPCTypeWebSocket: {"supplier1-https://only.example.com"},
		domain.RPCTypeJSONRPC: {
			"supplier1-https://only.example.com",
			"supplier2-https://rpc.example2.com",
		},
	}}
	_, mux := newTestAdminWithDrain(t, store, provider, time.Hour)

	// Unscoped: websocket has no endpoint outside the target operator, so the
	// drain must be refused even though json_rpc has a second operator.
	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`)
	if res.status != http.StatusConflict {
		t.Fatalf("unscoped: status = %d, want 409, body=%s", res.status, res.body)
	}
	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after a refused drain", active)
	}

	// Scoped to json_rpc: two operators there, so the drain is allowed.
	res = doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","rpc_type":"json_rpc"}`)
	if res.status != http.StatusOK {
		t.Fatalf("scoped json_rpc: status = %d, want 200, body=%s", res.status, res.body)
	}
}

func TestAdminDrain_PropagationErrorStillAppliesLocally(t *testing.T) {
	inner := drain.NewMemoryStore()
	store := &stubErrStore{Store: inner, setErr: fmt.Errorf("wrap: %w", drain.ErrPropagation)}
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		PropagationError string `json:"propagation_error"`
		Applied          bool   `json:"applied"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Applied {
		t.Error("applied = false, want true: the drain still applies on this instance")
	}
	if out.PropagationError == "" {
		t.Error("propagation_error is empty, want the wrapped error's message")
	}
}

// TestAdminDrain_ReleasePropagationErrorStillAnswers200 covers the release
// side of the same honesty rule the set side has: the drain is gone from this
// instance, so a 500 would describe local state that does not exist and would
// invite an operator to retry a release that already happened.
func TestAdminDrain_ReleasePropagationErrorStillAnswers200(t *testing.T) {
	inner := drain.NewMemoryStore()
	store := &stubErrStore{Store: inner, releaseErr: fmt.Errorf("wrap: %w", drain.ErrPropagation)}
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	if err := inner.Set(context.Background(), drain.Entry{
		Key:   drain.Key{ServiceID: "eth", Operator: "example.com"},
		Until: time.Now().Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"0s"}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		Released         bool   `json:"released"`
		PropagationError string `json:"propagation_error"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Released {
		t.Error("released = false, want true: the release applied on this instance")
	}
	if !strings.Contains(out.PropagationError, drain.ErrPropagation.Error()) {
		t.Errorf("propagation_error = %q, want the wrapped propagation error", out.PropagationError)
	}
	if active := inner.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty: the local release did apply", active)
	}
}

// TestAdminDrain_ReleaseNonPropagationErrorAnswers500 keeps the split honest
// in the other direction: an error that is not a propagation failure means the
// release itself did not happen, and that is a 500.
func TestAdminDrain_ReleaseNonPropagationErrorAnswers500(t *testing.T) {
	inner := drain.NewMemoryStore()
	store := &stubErrStore{Store: inner, releaseErr: errors.New("store is broken")}
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"0s"}`)
	if res.status != http.StatusInternalServerError {
		t.Fatalf("POST status = %d, want 500, body=%s", res.status, res.body)
	}

	if err := inner.Set(context.Background(), drain.Entry{
		Key:   drain.Key{ServiceID: "eth", Operator: "example.com"},
		Until: time.Now().Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	res = doRequest(t, mux, http.MethodDelete, "/admin/reputation/drain/eth/example.com", "")
	if res.status != http.StatusInternalServerError {
		t.Fatalf("DELETE status = %d, want 500, body=%s", res.status, res.body)
	}
}

// TestAdminDrain_DeletePropagationErrorReleasesEveryKey pins both halves of
// the DELETE fix: a key that only failed to propagate does not abort the loop,
// and every such failure is reported in one propagation_error rather than the
// first one deciding the whole response.
func TestAdminDrain_DeletePropagationErrorReleasesEveryKey(t *testing.T) {
	inner := drain.NewMemoryStore()
	store := &stubErrStore{Store: inner, releaseErr: fmt.Errorf("wrap: %w", drain.ErrPropagation)}
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	ctx := context.Background()
	for _, rt := range []domain.RPCType{domain.RPCTypeWebSocket, domain.RPCTypeJSONRPC} {
		if err := inner.Set(ctx, drain.Entry{
			Key:   drain.Key{ServiceID: "eth", Operator: "example.com", RPCType: rt},
			Until: time.Now().Add(30 * time.Minute),
		}); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	res := doRequest(t, mux, http.MethodDelete, "/admin/reputation/drain/eth/example.com", "")
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		Released         int    `json:"released"`
		PropagationError string `json:"propagation_error"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Released != 2 {
		t.Errorf("released = %d, want 2: a failed propagation must not stop the loop", out.Released)
	}
	if n := strings.Count(out.PropagationError, drain.ErrPropagation.Error()); n != 2 {
		t.Errorf("propagation_error = %q, want both keys' errors accumulated", out.PropagationError)
	}
	if active := inner.Active(ctx, "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty: every key was released locally", active)
	}
}

// TestAdminDrain_UppercaseHostIsMatched pins the case fold on the endpoint
// side. shannon's chokepoint lowercases the operator it derives from the URL,
// so a mixed-case host IS drained there; counting it with a case-sensitive
// comparison here reported matched_endpoints: 0 for a drain that applies, and
// let the last-operator guard wave through a drain that empties the pool.
func TestAdminDrain_UppercaseHostIsMatched(t *testing.T) {
	store := drain.NewMemoryStore()
	mixedCase := domain.EndpointAddrList{
		"supplier1-https://RPC.Example.COM",
		"supplier2-https://rpc.example2.com",
	}
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: mixedCase}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		MatchedEndpoints int `json:"matched_endpoints"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if out.MatchedEndpoints != 1 {
		t.Errorf("matched_endpoints = %d, want 1: an uppercase host belongs to the same operator", out.MatchedEndpoints)
	}
}

// TestAdminDrain_UppercaseHostTriggersLastOperator409 is the consequence that
// matters: with a case-sensitive comparison every endpoint looks like someone
// else's, so the pool-collapse guard sees a diverse pool and allows a drain
// that would leave selection with nothing.
func TestAdminDrain_UppercaseHostTriggersLastOperator409(t *testing.T) {
	store := drain.NewMemoryStore()
	mixedCase := domain.EndpointAddrList{
		"supplier1-https://A.Only-Example.COM",
		"supplier2-https://B.Only-Example.COM",
	}
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: mixedCase}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"only-example.com","duration":"30m"}`)
	if res.status != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body=%s", res.status, res.body)
	}
	if active := store.Active(context.Background(), "eth"); len(active) != 0 {
		t.Errorf("store.Active = %+v, want empty after a refused drain", active)
	}
}

func TestAdminDrain_NegativeDurationRejected(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"-5m"}`)
	if res.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", res.status, res.body)
	}
}

func TestAdminDrain_DomainLowercasedInActiveDrains(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"EXAMPLE.COM","duration":"30m"}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		Domain       string        `json:"domain"`
		ActiveDrains []drain.Entry `json:"active_drains"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Domain != "example.com" {
		t.Errorf("response domain = %q, want lowercased example.com", out.Domain)
	}
	if len(out.ActiveDrains) != 1 || out.ActiveDrains[0].Operator != "example.com" {
		t.Errorf("active_drains = %+v, want lowercased operator example.com", out.ActiveDrains)
	}
}

func TestAdminDrain_NilStoreAnswers503(t *testing.T) {
	_, mux := newTestAdminWithDrain(t, nil, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	post := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`)
	if post.status != http.StatusServiceUnavailable {
		t.Errorf("POST status = %d, want 503, body=%s", post.status, post.body)
	}

	get := doRequest(t, mux, http.MethodGet, "/admin/reputation/drain/eth", "")
	if get.status != http.StatusServiceUnavailable {
		t.Errorf("GET status = %d, want 503, body=%s", get.status, get.body)
	}

	del := doRequest(t, mux, http.MethodDelete, "/admin/reputation/drain/eth/example.com", "")
	if del.status != http.StatusServiceUnavailable {
		t.Errorf("DELETE status = %d, want 503, body=%s", del.status, del.body)
	}
}

// TestAdminDrain_JSONFieldNamesAreSnakeCase pins drain.Entry's wire shape:
// {service_id, domain, rpc_type?, until, reason?} — not the Go field names.
func TestAdminDrain_JSONFieldNamesAreSnakeCase(t *testing.T) {
	store := drain.NewMemoryStore()
	_, mux := newTestAdminWithDrain(t, store, &fakeEndpointProvider{endpoints: twoOperatorEndpoints}, time.Hour)

	doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","reason":"noisy"}`)
	res := doRequest(t, mux, http.MethodGet, "/admin/reputation/drain/eth", "")

	for _, want := range []string{`"service_id":"eth"`, `"domain":"example.com"`, `"until":`, `"reason":"noisy"`} {
		if !strings.Contains(string(res.body), want) {
			t.Errorf("body missing %s, got %s", want, res.body)
		}
	}
	for _, unwanted := range []string{`"ServiceID"`, `"Operator"`, `"RPCType"`, `"Until"`, `"Reason"`} {
		if strings.Contains(string(res.body), unwanted) {
			t.Errorf("body contains PascalCase field %s, got %s", unwanted, res.body)
		}
	}
}

// registeredEndpointProvider hands out fewer endpoints than it has registered,
// the way the Shannon protocol does once a ban, a drain or the blacklist has
// removed some.
type registeredEndpointProvider struct {
	available  domain.EndpointAddrList
	registered domain.EndpointAddrList
}

func (p *registeredEndpointProvider) AvailableEndpoints(_ context.Context, _ domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	return p.available, nil
}

func (p *registeredEndpointProvider) RegisteredEndpoints(_ context.Context, _ domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	return p.registered, nil
}

// matched_endpoints counts an operator's registrations whether or not they
// are currently excluded for another reason. A dry run against an operator
// that is already drained must say how many endpoints the drain covers, not
// zero — zero reads as "no such operator".
func TestAdminDrain_MatchedEndpointsCountsRegistrationsNotSurvivors(t *testing.T) {
	store := drain.NewMemoryStore()
	provider := &registeredEndpointProvider{
		// Already excluded: nothing of example.com is being handed out.
		available: domain.EndpointAddrList{"pokt1c-https://rpc.other.net"},
		registered: domain.EndpointAddrList{
			"pokt1a-https://rpc-1.example.com",
			"pokt1b-https://rpc-2.example.com",
			"pokt1c-https://rpc.other.net",
		},
	}
	_, mux := newTestAdminWithDrain(t, store, provider, time.Hour)

	res := doRequest(t, mux, http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m","dry_run":true}`)
	if res.status != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", res.status, res.body)
	}
	var out struct {
		MatchedEndpoints int `json:"matched_endpoints"`
	}
	if err := json.Unmarshal(res.body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.MatchedEndpoints != 2 {
		t.Fatalf("matched_endpoints = %d, want 2 registrations of example.com", out.MatchedEndpoints)
	}
}
