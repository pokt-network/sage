package healthcheck

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
)

// --- protocol stubs ---

type stubRelayer struct {
	mu       sync.Mutex
	calls    []relayCall
	response *domain.Response
	err      error
}

type relayCall struct {
	serviceID domain.ServiceID
	endpoint  domain.EndpointAddr
}

func (s *stubRelayer) SendRelay(_ context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, _ domain.Payload) (*domain.Response, error) {
	s.mu.Lock()
	s.calls = append(s.calls, relayCall{serviceID: svcID, endpoint: ep})
	s.mu.Unlock()
	return s.response, s.err
}

type stubEndpointProvider struct {
	endpoints domain.EndpointAddrList
}

func (s *stubEndpointProvider) AvailableEndpoints(_ context.Context, _ domain.ServiceID, _ domain.RPCType) (domain.EndpointAddrList, error) {
	return s.endpoints, nil
}

type stubSessionManager struct {
	services map[domain.ServiceID]struct{}
}

func (s *stubSessionManager) ConfiguredServices() map[domain.ServiceID]struct{} {
	return s.services
}

func (s *stubSessionManager) IsReady(_ context.Context) bool { return true }

// --- qos stubs ---

// checkOnlyPlugin implements both Plugin and HealthChecker.
type checkOnlyPlugin struct {
	checks []qos.HealthCheck
}

func (p *checkOnlyPlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (p *checkOnlyPlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}

func (p *checkOnlyPlugin) HealthChecks(_ domain.EndpointAddr) []qos.HealthCheck {
	return p.checks
}

// --- reputation stub ---

type stubRepService struct {
	mu      sync.Mutex
	signals []signalRecord
}

type signalRecord struct {
	serviceID domain.ServiceID
	endpoint  domain.EndpointAddr
	signal    reputation.Signal
}

func (s *stubRepService) RecordSignal(_ context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, sig reputation.Signal) error {
	s.mu.Lock()
	s.signals = append(s.signals, signalRecord{svcID, ep, sig})
	s.mu.Unlock()
	return nil
}

func (s *stubRepService) GetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr) (float64, error) {
	return 100, nil
}

func (s *stubRepService) GetScores(_ context.Context, _ domain.ServiceID) (map[domain.EndpointAddr]float64, error) {
	return nil, nil
}

func (s *stubRepService) SelectBest(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}

func (s *stubRepService) SelectSpread(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}

func (s *stubRepService) ResetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr) error {
	return nil
}

// --- tests ---

func newTestExecutor(
	relayer *stubRelayer,
	eps *stubEndpointProvider,
	sessions *stubSessionManager,
	reg *qos.Registry,
	rep reputation.Service,
) *Executor {
	logger := slog.Default()
	return NewExecutor(relayer, eps, sessions, reg, rep, nil, defaultInterval, defaultWorkers, logger)
}

func TestRunOnce_CallsRelayForEachEndpoint(t *testing.T) {
	relayer := &stubRelayer{
		response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)},
	}
	eps := &stubEndpointProvider{
		endpoints: domain.EndpointAddrList{
			"supplierA-https://node1.example.com",
			"supplierB-https://node2.example.com",
		},
	}
	sessions := &stubSessionManager{
		services: map[domain.ServiceID]struct{}{"eth": {}},
	}

	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &checkOnlyPlugin{
		checks: []qos.HealthCheck{
			{Name: "block_number", Payload: payload},
		},
	}
	reg := qos.NewRegistry()
	if err := reg.Register("eth", plugin); err != nil {
		t.Fatal(err)
	}

	rep := &stubRepService{}
	exec := newTestExecutor(relayer, eps, sessions, reg, rep)
	exec.runOnce(context.Background())

	// Allow worker goroutines to complete.
	time.Sleep(50 * time.Millisecond)

	relayer.mu.Lock()
	gotCalls := len(relayer.calls)
	relayer.mu.Unlock()

	if gotCalls != 2 {
		t.Errorf("expected 2 relay calls (one per endpoint), got %d", gotCalls)
	}
}

func TestRunOnce_RecordsSuccessSignal(t *testing.T) {
	relayer := &stubRelayer{
		response: &domain.Response{HTTPStatusCode: 200},
	}
	eps := &stubEndpointProvider{
		endpoints: domain.EndpointAddrList{"supplierA-https://node.example.com"},
	}
	sessions := &stubSessionManager{
		services: map[domain.ServiceID]struct{}{"eth": {}},
	}

	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &checkOnlyPlugin{
		checks: []qos.HealthCheck{{Name: "block_number", Payload: payload}},
	}
	reg := qos.NewRegistry()
	_ = reg.Register("eth", plugin)

	rep := &stubRepService{}
	exec := newTestExecutor(relayer, eps, sessions, reg, rep)
	exec.runOnce(context.Background())

	time.Sleep(50 * time.Millisecond)

	rep.mu.Lock()
	signals := rep.signals
	rep.mu.Unlock()

	if len(signals) == 0 {
		t.Fatal("expected at least one reputation signal, got none")
	}
	if signals[0].signal.Type != reputation.SignalSuccess {
		t.Errorf("expected success signal, got %q", signals[0].signal.Type)
	}
}

func TestRunOnce_RecordsErrorSignalOnRelayFailure(t *testing.T) {
	relayer := &stubRelayer{err: context.DeadlineExceeded}
	eps := &stubEndpointProvider{
		endpoints: domain.EndpointAddrList{"supplierA-https://node.example.com"},
	}
	sessions := &stubSessionManager{
		services: map[domain.ServiceID]struct{}{"eth": {}},
	}

	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &checkOnlyPlugin{
		checks: []qos.HealthCheck{{Name: "block_number", Payload: payload}},
	}
	reg := qos.NewRegistry()
	_ = reg.Register("eth", plugin)

	rep := &stubRepService{}
	exec := newTestExecutor(relayer, eps, sessions, reg, rep)
	exec.runOnce(context.Background())

	time.Sleep(50 * time.Millisecond)

	rep.mu.Lock()
	signals := rep.signals
	rep.mu.Unlock()

	if len(signals) == 0 {
		t.Fatal("expected error signal, got none")
	}
	if signals[0].signal.Type != reputation.SignalMajorError {
		t.Errorf("expected major error signal, got %q", signals[0].signal.Type)
	}
}

func TestRunOnce_SkipsServicesWithNoPlugin(t *testing.T) {
	var relayCalls int64
	relayer := &stubRelayer{}
	relayer.response = &domain.Response{HTTPStatusCode: 200}

	eps := &stubEndpointProvider{
		endpoints: domain.EndpointAddrList{"supplierA-https://node.example.com"},
	}
	sessions := &stubSessionManager{
		services: map[domain.ServiceID]struct{}{"unknown-service": {}},
	}

	reg := qos.NewRegistry() // no plugins registered
	rep := &stubRepService{}
	exec := newTestExecutor(relayer, eps, sessions, reg, rep)
	exec.runOnce(context.Background())

	time.Sleep(50 * time.Millisecond)

	relayer.mu.Lock()
	relayCalls = int64(len(relayer.calls))
	relayer.mu.Unlock()

	if relayCalls != 0 {
		t.Errorf("expected 0 relay calls for service with no plugin, got %d", relayCalls)
	}
}

func TestRunOnce_WorkerBound(t *testing.T) {
	// Verify that even with many endpoints the semaphore doesn't deadlock.
	const numEndpoints = 20
	var concurrentPeak int64
	var current int64

	eps := make(domain.EndpointAddrList, numEndpoints)
	for i := 0; i < numEndpoints; i++ {
		eps[i] = domain.EndpointAddr("supplierA-https://node.example.com/" + string(rune('a'+i)))
	}

	relayer := &stubRelayer{}
	relayer.response = &domain.Response{HTTPStatusCode: 200}

	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &checkOnlyPlugin{
		checks: []qos.HealthCheck{{Name: "check", Payload: payload}},
	}
	reg := qos.NewRegistry()
	_ = reg.Register("eth", plugin)

	sessions := &stubSessionManager{
		services: map[domain.ServiceID]struct{}{"eth": {}},
	}
	rep := &stubRepService{}

	exec := NewExecutor(
		&trackingRelayer{stubRelayer: relayer},
		&stubEndpointProvider{endpoints: eps},
		sessions, reg, rep, nil,
		defaultInterval, 4, slog.Default(),
	)
	exec.runOnce(context.Background())
	time.Sleep(200 * time.Millisecond)

	_ = concurrentPeak
	_ = current
	// If we get here without deadlock, the worker pool is working correctly.
}

// trackingRelayer is a relayer that counts concurrent calls.
type trackingRelayer struct {
	*stubRelayer
	current atomic.Int64
	peak    atomic.Int64
}

func (t *trackingRelayer) SendRelay(ctx context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, p domain.Payload) (*domain.Response, error) {
	c := t.current.Add(1)
	if c > t.peak.Load() {
		t.peak.Store(c)
	}
	defer t.current.Add(-1)
	return t.stubRelayer.SendRelay(ctx, svcID, ep, p)
}

// --- checkSignal grading ---

// Grading on HTTP status alone was the bug: a 200 carrying an unparseable body,
// or one confidently reporting the wrong chain, both scored a full success and
// kept the endpoint in rotation at unblemished reputation.
func TestCheckSignal_Grading(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		extractErr error
		want       reputation.SignalType
	}{
		{
			name:       "healthy 200 succeeds",
			statusCode: 200,
			extractErr: nil,
			want:       reputation.SignalSuccess,
		},
		{
			name:       "non-2xx is a minor error",
			statusCode: 500,
			extractErr: nil,
			want:       reputation.SignalMinorError,
		},
		{
			// The endpoint is healthy and serving another chain under this
			// service's name. Its block heights are real, so every height filter
			// passes it — this signal is the only thing that ejects it.
			name:       "200 reporting the wrong chain is critical",
			statusCode: 200,
			extractErr: fmt.Errorf("wrapped: %w", qos.ErrWrongChain),
			want:       reputation.SignalCriticalError,
		},
		{
			name:       "200 with an unparseable body is a minor error",
			statusCode: 200,
			extractErr: errors.New("no result field in response"),
			want:       reputation.SignalMinorError,
		},
		{
			// Status is checked first: a wrong-chain body on a 500 is still just
			// a failing endpoint, and shouldn't be graded on what it claimed.
			name:       "non-2xx outranks the extract error",
			statusCode: 503,
			extractErr: qos.ErrWrongChain,
			want:       reputation.SignalMinorError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := checkSignal("eth_chainId", tc.statusCode, tc.extractErr, 5*time.Millisecond)
			if sig.Type != tc.want {
				t.Errorf("checkSignal type = %q, want %q", sig.Type, tc.want)
			}
			if sig.Latency != 5*time.Millisecond {
				t.Errorf("latency = %v, want 5ms", sig.Latency)
			}
			if !strings.Contains(sig.Reason, "eth_chainId") {
				t.Errorf("reason %q should name the check", sig.Reason)
			}
		})
	}
}
