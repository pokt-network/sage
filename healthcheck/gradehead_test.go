package healthcheck

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// headCheckPlugin declares one check that grades the head and one that does
// not, and reports the time it was asked to grade at.
type headCheckPlugin struct{ gradedAt *time.Time }

func (headCheckPlugin) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (headCheckPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}
func (headCheckPlugin) HealthChecks() []qos.HealthCheck {
	return []qos.HealthCheck{
		{Name: "canary", Payload: domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_call"), GradesHead: true},
		{Name: "plain", Payload: domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_chainId")},
	}
}
func (p headCheckPlugin) HeadLag(_ domain.Payload, _ []byte, at time.Time) (uint64, bool, bool) {
	*p.gradedAt = at
	return 7, true, true
}

func TestGradeHead_RecordsOnlyGradingChecksAtProbeTime(t *testing.T) {
	var at time.Time
	var got []string
	e := &Executor{}
	e.SetHeadLagRecorder(func(_ domain.ServiceID, _, method string, lag uint64, stale bool) {
		if lag != 7 || !stale {
			t.Errorf("lag %d stale %v", lag, stale)
		}
		got = append(got, method)
	})
	plugin := headCheckPlugin{gradedAt: &at}
	probed := time.Now().Add(-30 * time.Second)
	if !e.gradeHead(plugin, ProbeResult{ServiceID: "eth", Endpoint: "a1-https://x.a.net", Check: "canary", ProbedAt: probed}) {
		t.Error("the canary is a head check")
	}
	if e.gradeHead(plugin, ProbeResult{ServiceID: "eth", Endpoint: "a1-https://x.a.net", Check: "plain", ProbedAt: probed}) {
		t.Error("a plain check is not")
	}
	if len(got) != 1 || got[0] != "canary" {
		t.Fatalf("recorded %v, want only the grading check", got)
	}
	if !at.Equal(probed) {
		t.Fatalf("graded at %v, want the probe's time %v", at, probed)
	}
}

// A head check records no reputation signal, refused or not: a gateway
// refusing a multicall (-32602, or a 4xx) is policy, not a fault.
func TestGradeHead_CheckRecordsNoReputationSignal(t *testing.T) {
	var at time.Time
	reg := qos.NewRegistry()
	if err := reg.Register("eth", headCheckPlugin{gradedAt: &at}); err != nil {
		t.Fatal(err)
	}
	rep := &stubRepService{}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{}, sessions, reg, rep, nil, defaultInterval, 4, slog.Default())
	for _, status := range []int{200, 400} {
		exec.applyResult(context.Background(), ProbeResult{
			ServiceID: "eth", Endpoint: "a-https://n1.example", Check: "canary", RPCType: domain.RPCTypeJSONRPC,
			StatusCode: status, Body: []byte(`{"error":{"code":-32602,"message":"request is too complex"}}`),
		})
	}
	exec.applyResult(context.Background(), ProbeResult{
		ServiceID: "eth", Endpoint: "a-https://n1.example", Check: "canary", RPCType: domain.RPCTypeJSONRPC,
		TransportError: "timeout", TransportReason: "timeout", TransportSeverity: "major",
	})
	exec.applyResult(context.Background(), ProbeResult{
		ServiceID: "eth", Endpoint: "a-https://n1.example", Check: "plain", RPCType: domain.RPCTypeJSONRPC,
		StatusCode: 400,
	})
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 1 {
		t.Fatalf("%d signals, want only the plain check's", len(rep.signals))
	}
}

// restHeadPlugin's one head check is a body-less GET, recognised by its path.
type restHeadPlugin struct{ graded *bool }

func (restHeadPlugin) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (restHeadPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}
func (restHeadPlugin) HealthChecks() []qos.HealthCheck {
	return []qos.HealthCheck{{Name: "rest_canary", Payload: domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/latest", http.MethodGet), GradesHead: true}}
}
func (p restHeadPlugin) HeadLag(payload domain.Payload, _ []byte, _ time.Time) (uint64, bool, bool) {
	*p.graded = payload.Path() == "/latest" && payload.HTTPMethod() == http.MethodGet
	return 1, false, *p.graded
}

// A REST canary has no body: the grading must carry the check's path, or the
// plugin cannot tell what was asked.
func TestGradeHead_RESTCheckKeepsItsPath(t *testing.T) {
	graded := false
	e := &Executor{}
	e.SetHeadLagRecorder(func(domain.ServiceID, string, string, uint64, bool) {})
	e.gradeHead(restHeadPlugin{graded: &graded}, ProbeResult{ServiceID: "osmosis", Endpoint: "a-https://n1.example", Check: "rest_canary", RPCType: domain.RPCTypeREST, ProbedAt: time.Now()})
	if !graded {
		t.Fatal("REST canary graded without its path")
	}
}
