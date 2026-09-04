package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/sage/circuitbreaker"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/tuning"
)

// fakeQoSPlugin is a minimal qos.Plugin for router tests that need a
// registered plugin without any chain-specific behaviour.
type fakeQoSPlugin struct{}

func (fakeQoSPlugin) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (fakeQoSPlugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return endpoints, nil
}

// fakeResettablePlugin additionally implements qos.StateResetter, tracking
// whether ResetState was called.
type fakeResettablePlugin struct {
	fakeQoSPlugin
	resetCalls int
}

func (p *fakeResettablePlugin) ResetState() {
	p.resetCalls++
}

func newChainStateAdminAPI(t *testing.T, reg *qos.Registry) *AdminAPI {
	t.Helper()
	flags := newMockFlagStore()
	repSvc := newMockRepService()
	tl := reputation.NewTimeline(100)
	breaker := circuitbreaker.New()
	return NewAdminAPI(flags, repSvc, tl, breaker, nil, nil, nil, 0, reg, tuning.NewStore(), nil, nil, discardLogger())
}

func TestAdmin_ClearChainState_Resettable(t *testing.T) {
	reg := qos.NewRegistry()
	plugin := &fakeResettablePlugin{}
	if err := reg.Register("eth", plugin); err != nil {
		t.Fatal(err)
	}
	api := newChainStateAdminAPI(t, reg)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/chain-state/clear/eth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}

	var body struct {
		ServiceID string `json:"service_id"`
		Reset     bool   `json:"reset"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	if body.ServiceID != "eth" || !body.Reset {
		t.Fatalf("unexpected body: %+v", body)
	}
	if plugin.resetCalls != 1 {
		t.Fatalf("ResetState called %d times, want 1", plugin.resetCalls)
	}
}

func TestAdmin_ClearChainState_NoOpPlugin(t *testing.T) {
	reg := qos.NewRegistry()
	if err := reg.Register("noop", fakeQoSPlugin{}); err != nil {
		t.Fatal(err)
	}
	api := newChainStateAdminAPI(t, reg)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/chain-state/clear/noop", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}

	var body struct {
		ServiceID string `json:"service_id"`
		Reset     bool   `json:"reset"`
		Message   string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	if body.ServiceID != "noop" || body.Reset || body.Message == "" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestAdmin_ClearChainState_UnknownService(t *testing.T) {
	reg := qos.NewRegistry()
	api := newChainStateAdminAPI(t, reg)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/chain-state/clear/ghost", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404: %s", rec.Code, rec.Body)
	}
}

// fakeViewingPlugin implements the read half: a chain view and per-endpoint
// heights.
type fakeViewingPlugin struct{ fakeQoSPlugin }

func (fakeViewingPlugin) ChainView() qos.ChainView {
	return qos.ChainView{Perceived: 318664809, Highest: 318664809, Lowest: 3, Endpoints: 2}
}

func (fakeViewingPlugin) EndpointHeights() []qos.EndpointHeight {
	return []qos.EndpointHeight{
		{Endpoint: "good-https://n1.example", Height: 318664809},
		{Endpoint: "bad-https://n2.example", Height: 3},
	}
}

func TestAdmin_GetChainState_NamesTheEndpointBehindTheSpread(t *testing.T) {
	reg := qos.NewRegistry()
	if err := reg.Register("sui", fakeViewingPlugin{}); err != nil {
		t.Fatal(err)
	}
	api := newChainStateAdminAPI(t, reg)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/chain-state/sui", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var body struct {
		Perceived uint64 `json:"perceived"`
		Heights   []struct {
			Endpoint string `json:"endpoint"`
			Height   uint64 `json:"height"`
		} `json:"heights"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	if body.Perceived != 318664809 || len(body.Heights) != 2 || body.Heights[1].Height != 3 {
		t.Fatalf("body = %s; the point of the route is naming the endpoint that reported 3", rec.Body)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/chain-state/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown service = %d, want 404", rec.Code)
	}
}
