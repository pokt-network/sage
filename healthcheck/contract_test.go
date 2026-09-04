package healthcheck

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// Controls that must control (the 2026-09-04 audit): the health_checks flag,
// configured checks on a passthrough service, the warm denominator, and an
// Essential probe that answers nothing.

type passthroughPlugin struct{}

func (passthroughPlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (passthroughPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}

func probeCount(r *stubRelayer) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func settle() { time.Sleep(50 * time.Millisecond) }

func TestHealthChecksFlag_OffStopsProbing(t *testing.T) {
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"result":"0x1"}`)}}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"a-https://n1.example"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}, "poly": {}}}
	exec := newTestExecutor(relayer, eps, sessions, probeableRegistry(t, "eth", "poly"), &stubRepService{})

	flags := featureflag.NewMemoryStore(map[string]bool{featureflag.FlagHealthChecks: false})
	exec.SetFlags(flags)
	exec.runOnce(context.Background())
	settle()
	if n := probeCount(relayer); n != 0 {
		t.Fatalf("sent %d probes with the flag off; PUT /admin/flags/health_checks must stop them", n)
	}

	// Per service: on globally, off for poly.
	flags = featureflag.NewMemoryStore(map[string]bool{featureflag.FlagHealthChecks: true})
	_ = flags.SetForService(context.Background(), featureflag.FlagHealthChecks, "poly", false)
	exec.SetFlags(flags)
	exec.runOnce(context.Background())
	settle()
	relayer.mu.Lock()
	defer relayer.mu.Unlock()
	if len(relayer.calls) != 1 || relayer.calls[0].serviceID != "eth" {
		t.Fatalf("calls = %+v, want exactly one probe, for eth", relayer.calls)
	}
}

func TestConfiguredChecks_RunOnPassthroughService(t *testing.T) {
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"ok":true}`)}}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"a-https://n1.example"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"radix": {}}}
	reg := qos.NewRegistry()
	if err := reg.Register("radix", passthroughPlugin{}); err != nil {
		t.Fatal(err)
	}
	exec := newTestExecutor(relayer, eps, sessions, reg, &stubRepService{})
	checks, warnings := BuildConfiguredChecks(config.HealthCheckConfig{Local: []config.ServiceHealthChecks{{
		ServiceID: "radix", Enabled: true,
		Checks: []config.HealthCheck{{Name: "status", Type: "rest", Method: "GET", Path: "/status"}},
	}}})
	if len(warnings) != 0 {
		t.Fatal(warnings)
	}
	exec.SetConfiguredChecks(checks)
	exec.runOnce(context.Background())
	settle()
	if n := probeCount(relayer); n != 1 {
		t.Fatalf("sent %d probes, want 1: a YAML check for a passthrough service was built and never sent", n)
	}
}

func TestWarm_IgnoresServicesNothingCanProbe(t *testing.T) {
	// Three passthrough services and one real one: the real one alone is
	// 100% of what can be waited for. Counting all four held readiness at
	// 503 forever, since 75% of 4 is 3 and only 1 can ever report.
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}, "a": {}, "b": {}, "c": {}}}
	reg := probeableRegistry(t, "eth")
	for _, id := range []domain.ServiceID{"a", "b", "c"} {
		if err := reg.Register(id, passthroughPlugin{}); err != nil {
			t.Fatal(err)
		}
	}
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{}, sessions, reg, &stubRepService{}, nil, defaultInterval, 4, slog.Default())
	if exec.Warm() {
		t.Fatal("not warm before eth reports")
	}
	exec.markCovered("eth")
	if !exec.Warm() {
		t.Fatal("eth is every service that can report, so its result must make the pod warm")
	}
}

// essentialPlugin declares one Essential check and extracts a height only
// when the body has one — the shape of a REST chain such as eth-beacon.
type essentialPlugin struct{ checkOnlyPlugin }

func (essentialPlugin) ExtractData(_ domain.EndpointAddr, _, response []byte) (*qos.ExtractedData, error) {
	if strings.Contains(string(response), "slot") {
		h := uint64(7)
		return &qos.ExtractedData{BlockHeight: &h}, nil
	}
	return &qos.ExtractedData{}, nil
}

func TestEssentialProbe_A2xxWithoutTheFactIsAFailure(t *testing.T) {
	payload := domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/eth/v1/beacon/headers/head", "GET")
	plugin := &essentialPlugin{checkOnlyPlugin{checks: []qos.HealthCheck{{Name: "beacon_head_header", Payload: payload, Essential: true}}}}
	reg := qos.NewRegistry()
	if err := reg.Register("eth-beacon", plugin); err != nil {
		t.Fatal(err)
	}
	rep := &stubRepService{}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth-beacon": {}}}
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{}, sessions, reg, rep, nil, defaultInterval, 4, slog.Default())

	exec.applyResult(context.Background(), ProbeResult{
		ServiceID: "eth-beacon", Endpoint: "a-https://n1.example", Check: "beacon_head_header",
		RPCType: domain.RPCTypeREST, StatusCode: 200, Body: []byte("<html>gateway</html>"),
	})
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) == 0 {
		t.Fatal("no signal recorded")
	}
	sig := rep.signals[len(rep.signals)-1].signal
	if sig.Type == reputation.SignalSuccess {
		t.Fatalf("an HTML 200 on the head-header probe graded success: %+v", sig)
	}
	if !strings.Contains(sig.Reason, "without the fact") {
		t.Errorf("reason = %q", sig.Reason)
	}
}

func TestRPCTypeCoverageGaps(t *testing.T) {
	reg := probeableRegistry(t, "tron")
	checks, _ := BuildConfiguredChecks(config.HealthCheckConfig{})
	gaps := RPCTypeCoverageGaps([]config.ServiceConfig{
		{ID: "tron", RPCTypes: []string{"json_rpc", "rest", "websocket"}},
		{ID: "eth", RPCTypes: []string{"json_rpc"}},
	}, reg, checks)
	joined := strings.Join(gaps, "\n")
	if !strings.Contains(joined, `"tron" declares rest`) {
		t.Errorf("tron's unprobed REST surface not named: %v", gaps)
	}
	if !strings.Contains(joined, `"tron" declares websocket`) {
		t.Errorf("websocket should be named as unprobeable: %v", gaps)
	}
	if strings.Contains(joined, `"eth"`) {
		t.Errorf("eth has no gap (no plugin at all is the QoS-coverage report's business): %v", gaps)
	}
}
