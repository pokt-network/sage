package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/tuning"
)

// --- Mocks ---

// mockFlagStore implements featureflag.FlagStore in memory.
type mockFlagStore struct {
	flags map[string]featureflag.FlagState
}

func newMockFlagStore() *mockFlagStore {
	return &mockFlagStore{flags: make(map[string]featureflag.FlagState)}
}

func (m *mockFlagStore) IsEnabled(_ context.Context, flag string, _ domain.ServiceID) bool {
	if s, ok := m.flags[flag]; ok {
		return s.Enabled
	}
	return false
}

func (m *mockFlagStore) Set(_ context.Context, flag string, enabled bool) error {
	s := m.flags[flag]
	s.Enabled = enabled
	m.flags[flag] = s
	return nil
}

func (m *mockFlagStore) SetForService(_ context.Context, flag string, serviceID domain.ServiceID, enabled bool) error {
	s := m.flags[flag]
	if s.ServiceOverrides == nil {
		s.ServiceOverrides = make(map[domain.ServiceID]bool)
	}
	s.ServiceOverrides[serviceID] = enabled
	m.flags[flag] = s
	return nil
}

func (m *mockFlagStore) GetAll(_ context.Context) (map[string]featureflag.FlagState, error) {
	out := make(map[string]featureflag.FlagState, len(m.flags))
	for k, v := range m.flags {
		out[k] = v
	}
	return out, nil
}

func (m *mockFlagStore) Delete(_ context.Context, flag string, serviceID domain.ServiceID) error {
	if serviceID == "" {
		delete(m.flags, flag)
		return nil
	}
	s := m.flags[flag]
	delete(s.ServiceOverrides, serviceID)
	m.flags[flag] = s
	return nil
}

// mockRepService implements reputation.Service in memory.
type mockRepService struct {
	scores map[string]float64
}

func newMockRepService() *mockRepService {
	return &mockRepService{scores: make(map[string]float64)}
}

func (m *mockRepService) RecordSignal(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, _ domain.RPCType, _ reputation.Signal) error {
	return nil
}

func (m *mockRepService) GetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, _ domain.RPCType) (float64, error) {
	key := string(serviceID) + ":" + string(endpoint)
	return m.scores[key], nil
}

func (m *mockRepService) GetScores(_ context.Context, serviceID domain.ServiceID) (map[string]float64, error) {
	prefix := string(serviceID) + ":"
	out := make(map[string]float64)
	for k, v := range m.scores {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			out[k[len(prefix):]] = v
		}
	}
	return out, nil
}

func (m *mockRepService) SelectBest(_ context.Context, _ domain.ServiceID, endpoints domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	if len(endpoints) == 0 {
		return ""
	}
	return endpoints[0]
}

func (m *mockRepService) SelectSpread(_ context.Context, _ domain.ServiceID, endpoints domain.EndpointAddrList, _ domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(endpoints) == 0 {
		return ""
	}
	return endpoints[0]
}

func (m *mockRepService) ResetScore(_ context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr) error {
	key := string(serviceID) + ":" + string(endpoint)
	m.scores[key] = 100
	return nil
}

// --- Helper ---

func newTestAdmin(
	flags featureflag.FlagStore,
	repSvc reputation.Service,
	tl *reputation.Timeline,
	breaker *circuitbreaker.Breaker,
	qosReg *qos.Registry,
) *AdminAPI {
	return NewAdminAPI(flags, repSvc, tl, breaker, nil, qosReg, tuning.NewStore(), discardLogger())
}

// newTestAdminAPIWithBlocks is the same as newTestAdmin, plus a method-block
// store, for tests that exercise the method-blocks admin routes.
func newTestAdminAPIWithBlocks(t *testing.T, store *methodblock.Store) *AdminAPI {
	t.Helper()
	flags := newMockFlagStore()
	repSvc := newMockRepService()
	tl := reputation.NewTimeline(100)
	breaker := circuitbreaker.New()
	qosReg := qos.NewRegistry()
	return NewAdminAPI(flags, repSvc, tl, breaker, store, qosReg, tuning.NewStore(), discardLogger())
}

func newAdminServer(t *testing.T) (*AdminAPI, *httptest.Server) {
	t.Helper()

	flags := newMockFlagStore()
	_ = flags.Set(context.Background(), "hedge", true)
	_ = flags.Set(context.Background(), "retry", false)

	repSvc := newMockRepService()
	repSvc.scores["eth:supplier1-example.com"] = 82.5
	repSvc.scores["eth:supplier2-example2.com"] = 55.0

	tl := reputation.NewTimeline(100)
	tl.Record("eth:supplier1-example.com", reputation.TimelineEvent{
		Timestamp: time.Now(),
		Event:     "signal",
		Detail:    "test event",
		Score:     82.5,
	})

	breaker := circuitbreaker.New()

	qosReg := qos.NewRegistry()

	admin := newTestAdmin(flags, repSvc, tl, breaker, qosReg)

	mux := http.NewServeMux()
	admin.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return admin, srv
}

// --- Feature flag tests ---

func TestAdminListFlags(t *testing.T) {
	_, srv := newAdminServer(t)

	resp, err := http.Get(srv.URL + "/admin/flags")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]featureflag.FlagState
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if _, ok := out["hedge"]; !ok {
		t.Error("expected 'hedge' flag in response")
	}
	if out["hedge"].Enabled != true {
		t.Error("expected hedge to be enabled")
	}
}

func TestAdminSetFlag(t *testing.T) {
	_, srv := newAdminServer(t)

	body := `{"enabled": false}`
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/admin/flags/hedge", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["flag"] != "hedge" {
		t.Errorf("flag = %v, want hedge", out["flag"])
	}
	if out["enabled"] != false {
		t.Errorf("enabled = %v, want false", out["enabled"])
	}
}

func TestAdminSetFlagForService(t *testing.T) {
	_, srv := newAdminServer(t)

	body := `{"enabled": true}`
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/admin/flags/retry/eth", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["service_id"] != "eth" {
		t.Errorf("service_id = %v, want eth", out["service_id"])
	}
}

// --- Reputation tests ---

func TestAdminGetReputation(t *testing.T) {
	_, srv := newAdminServer(t)

	resp, err := http.Get(srv.URL + "/admin/reputation/eth")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Errorf("got %d endpoints, want 2", len(out))
	}
	score, ok := out["supplier1-example.com"]
	if !ok {
		t.Error("expected supplier1 in reputation response")
	}
	if score != 82.5 {
		t.Errorf("score = %f, want 82.5", score)
	}
}

func TestAdminResetReputation(t *testing.T) {
	_, srv := newAdminServer(t)

	endpoint := "supplier1-example.com"
	req, err := http.NewRequest(http.MethodPost,
		srv.URL+"/admin/reputation/reset/eth/"+endpoint,
		nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["status"] != "reset" {
		t.Errorf("status = %q, want reset", out["status"])
	}
}

// --- Circuit breaker tests ---

func TestAdminClearCircuitBreaker(t *testing.T) {
	_, srv := newAdminServer(t)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/circuit-breaker/clear/eth", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["service_id"] != "eth" {
		t.Errorf("service_id = %v, want eth", out["service_id"])
	}
	if _, ok := out["cleared_domains"]; !ok {
		t.Error("expected cleared_domains in response")
	}
}

func TestAdminGetCircuitBreaker(t *testing.T) {
	_, srv := newAdminServer(t)

	resp, err := http.Get(srv.URL + "/admin/circuit-breaker/eth")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	// The result is a map (possibly empty if nothing is broken).
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
}

// --- Timeline tests ---

func TestAdminGetTimeline(t *testing.T) {
	_, srv := newAdminServer(t)

	resp, err := http.Get(srv.URL + "/admin/timeline/eth")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out []any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	// We recorded one event in newAdminServer.
	if len(out) != 1 {
		t.Errorf("expected 1 timeline event, got %d", len(out))
	}
}

func TestAdminGetTimelineEndpoint(t *testing.T) {
	_, srv := newAdminServer(t)

	endpoint := "supplier1-example.com"
	resp, err := http.Get(srv.URL + "/admin/timeline/eth/" + endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out []any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Errorf("expected 1 timeline event, got %d", len(out))
	}
}

// --- Config dump test ---

func TestAdminGetConfig(t *testing.T) {
	_, srv := newAdminServer(t)

	resp, err := http.Get(srv.URL + "/admin/config")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if _, ok := out["services"]; !ok {
		t.Error("expected 'services' in config dump")
	}
	if _, ok := out["flags"]; !ok {
		t.Error("expected 'flags' in config dump")
	}
	if _, ok := out["qos"]; !ok {
		t.Error("expected 'qos' in config dump")
	}
}
