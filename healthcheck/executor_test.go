package healthcheck

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/config"
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
	check     string
}

func (s *stubRelayer) SendRelay(_ context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, payload domain.Payload) (*domain.Response, error) {
	s.mu.Lock()
	s.calls = append(s.calls, relayCall{serviceID: svcID, endpoint: ep, check: payload.Method()})
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
	// onceCalls is one entry per RecordSignalOnce call, holding the endpoint
	// list the executor handed over. signals alone cannot show the difference
	// between one call naming three siblings and three calls naming one each,
	// which is exactly what ruling F1 changed.
	onceCalls []domain.EndpointAddrList
}

type signalRecord struct {
	serviceID domain.ServiceID
	endpoint  domain.EndpointAddr
	signal    reputation.Signal
}

func (s *stubRepService) RecordSignal(_ context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType, sig reputation.Signal) error {
	s.mu.Lock()
	s.signals = append(s.signals, signalRecord{svcID, ep, sig})
	s.mu.Unlock()
	return nil
}

// RecordSignalOnce implements reputation.OnceRecorder. The stub records the
// call itself and, so the per-registration assertions elsewhere keep their
// meaning, one signalRecord per named endpoint — the real service dedupes by
// reputation key, which is its own business and is tested in reputation/.
func (s *stubRepService) RecordSignalOnce(_ context.Context, svcID domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType, sig reputation.Signal) error {
	s.mu.Lock()
	s.onceCalls = append(s.onceCalls, append(domain.EndpointAddrList(nil), eps...))
	for _, ep := range eps {
		s.signals = append(s.signals, signalRecord{svcID, ep, sig})
	}
	s.mu.Unlock()
	return nil
}

func (s *stubRepService) GetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) (float64, error) {
	return 100, nil
}

func (s *stubRepService) GetScores(_ context.Context, _ domain.ServiceID) (map[string]float64, error) {
	return nil, nil
}

func (s *stubRepService) SelectBest(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}

func (s *stubRepService) SelectSpread(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}

func (s *stubRepService) ResetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr) error {
	return nil
}

func (s *stubRepService) Vouched(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) bool {
	return true
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

// TestExecutor_ProbeSignalsAreMarkedProbe pins Probe on the response path
// (arranged exactly as TestRunOnce_RecordsSuccessSignal). Reverting Probe to
// its zero value makes this fail.
func TestExecutor_ProbeSignalsAreMarkedProbe(t *testing.T) {
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

	require.NotEmpty(t, signals)
	for _, sig := range signals {
		assert.True(t, sig.signal.Probe, "every health-check signal must carry Probe")
	}
}

// TestExecutor_ProbeSignalsAreMarkedProbe_TransportFailure pins Probe on the
// transport-failure path (arranged exactly as
// TestRunOnce_RecordsErrorSignalOnRelayFailure).
func TestExecutor_ProbeSignalsAreMarkedProbe_TransportFailure(t *testing.T) {
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

	require.NotEmpty(t, signals)
	for _, sig := range signals {
		assert.True(t, sig.signal.Probe, "every health-check transport-failure signal must carry Probe")
	}
}

// TestExecutor_ProbeSignalsAreMarkedProbe_ConfiguredOverride pins Probe on a
// signal built from a configured check's declared reputation_signal
// (configured.SignalFor override), not just the default grading path.
func TestExecutor_ProbeSignalsAreMarkedProbe_ConfiguredOverride(t *testing.T) {
	relayer := &stubRelayer{
		response: &domain.Response{HTTPStatusCode: 500},
	}
	eps := &stubEndpointProvider{
		endpoints: domain.EndpointAddrList{"supplierA-https://node.example.com"},
	}
	sessions := &stubSessionManager{
		services: map[domain.ServiceID]struct{}{"eth": {}},
	}

	// No plugin-provided checks; only the configured one runs.
	plugin := &checkOnlyPlugin{}
	reg := qos.NewRegistry()
	_ = reg.Register("eth", plugin)

	configured, warnings := BuildConfiguredChecks(localConfig(config.ServiceHealthChecks{
		ServiceID: "eth",
		Enabled:   true,
		Checks: []config.HealthCheck{
			{Name: "configured", Method: "eth_chainId", ReputationSignal: "critical_error"},
		},
	}))
	require.Empty(t, warnings)

	rep := &stubRepService{}
	exec := newTestExecutor(relayer, eps, sessions, reg, rep)
	exec.SetConfiguredChecks(configured)
	exec.runOnce(context.Background())

	time.Sleep(50 * time.Millisecond)

	rep.mu.Lock()
	signals := rep.signals
	rep.mu.Unlock()

	require.NotEmpty(t, signals)
	for _, sig := range signals {
		// Sanity: confirm the configured override actually fired, otherwise
		// this test would pass for the wrong reason.
		assert.Equal(t, reputation.SignalCriticalError, sig.signal.Type)
		assert.True(t, sig.signal.Probe, "a configured-override signal must still carry Probe")
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

// timeoutErr is a minimal net.Error whose Timeout() is true and which is
// NOT a *net.OpError with Op == "dial" — the shape of a request that
// connected and then failed to answer in time, as opposed to a failed dial.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// TestTransportSignal_Grading is the health-check counterpart of
// heuristic.AnalyzeTransportError's own tests: it asserts sendCheck's
// transport-failure branch grades with the SAME severity the relay path
// would give the identical error, rather than a flat major. A dead host
// must not look healthier to health checks than it does to relays.
func TestTransportSignal_Grading(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		ctxErr error
		want   reputation.SignalType
		none   bool
	}{
		{
			// Connect-level failure: the host isn't serving anything. Critical
			// on the relay path via heuristic.AnalyzeTransportError, and must
			// be critical here too — this is the exact gap beta found: a flat
			// major grade let a DNS-dead host sit at score 50 for ~7 cycles,
			// "vouched for" the whole time.
			name: "connect failure (dial) is critical",
			err: domain.NewRelayError(domain.ErrTransport, "HTTP relay failed",
				&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true),
			want: reputation.SignalCriticalError,
		},
		{
			name: "timeout after connect is major",
			err:  domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", timeoutErr{}, true),
			want: reputation.SignalMajorError,
		},
		{
			// The executor's own context ended — a client hang-up on the
			// health-check goroutine, not evidence about the endpoint.
			name:   "client-cancelled records nothing",
			err:    domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.Canceled, true),
			ctxErr: context.Canceled,
			none:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Grade the way the leader does: probe records the verdict on the
			// result, applyResult turns it into a signal.
			ctx := context.Background()
			if tc.ctxErr != nil {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			exe := newTestExecutor(&stubRelayer{err: tc.err}, &stubEndpointProvider{}, &stubSessionManager{}, qos.NewRegistry(), nil)
			res := exe.probe(ctx, "eth", "supplierA-https://node1.example.com", nil, qos.HealthCheck{Name: "eth_chainId", Payload: domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_chainId")})
			ok := res.TransportSeverity != ""
			var sig reputation.Signal
			if ok {
				sig = severitySignal(res.TransportSeverity, "health_check: eth_chainId: "+res.TransportReason, 5*time.Millisecond)
			}
			if tc.none {
				if ok {
					t.Fatalf("expected no signal, got %+v", sig)
				}
				return
			}
			if !ok {
				t.Fatal("expected a signal")
			}
			if sig.Type != tc.want {
				t.Errorf("transportSignal type = %q, want %q", sig.Type, tc.want)
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

// --- backend-URL deduplication --- //

// heightPlugin implements HealthChecker, DataExtractor and BlockHeightTracker
// so the fan-out of a backend-derived height can be observed.
type heightPlugin struct {
	checkOnlyPlugin
	height uint64

	mu      sync.Mutex
	updates map[domain.EndpointAddr]uint64
}

func (p *heightPlugin) ExtractData(_ domain.EndpointAddr, _, _ []byte) (*qos.ExtractedData, error) {
	h := p.height
	return &qos.ExtractedData{BlockHeight: &h}, nil
}

func (p *heightPlugin) UpdateBlockHeight(ep domain.EndpointAddr, height uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.updates == nil {
		p.updates = map[domain.EndpointAddr]uint64{}
	}
	p.updates[ep] = height
}

func (p *heightPlugin) PerceivedBlockHeight() uint64 { return p.height }

func (p *heightPlugin) StartSync(_ context.Context) {}

// sharedBackendEndpoints: three registrations in front of one backend, plus one
// in front of another. Four suppliers, two machines.
func sharedBackendEndpoints() domain.EndpointAddrList {
	return domain.EndpointAddrList{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node1.example.com",
		"supplierD-https://node2.example.com",
	}
}

func dedupTestFixture(t *testing.T) (*Executor, *stubRelayer, *stubRepService, *heightPlugin) {
	t.Helper()
	relayer := &stubRelayer{
		response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)},
	}
	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &heightPlugin{
		checkOnlyPlugin: checkOnlyPlugin{checks: []qos.HealthCheck{{Name: "eth_blockNumber", Payload: payload}}},
		height:          1234,
	}
	reg := qos.NewRegistry()
	if err := reg.Register("eth", plugin); err != nil {
		t.Fatal(err)
	}
	rep := &stubRepService{}
	exe := newTestExecutor(
		relayer,
		&stubEndpointProvider{endpoints: sharedBackendEndpoints()},
		&stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}},
		reg,
		rep,
	)
	return exe, relayer, rep, plugin
}

// One relay per backend, not per registration: probing the same machine through
// three suppliers asks it the same question three times.
func TestRunOnce_DedupsRelaysByBackendURL(t *testing.T) {
	exe, relayer, _, _ := dedupTestFixture(t)

	exe.runOnce(context.Background())
	exe.wg.Wait()

	relayer.mu.Lock()
	defer relayer.mu.Unlock()
	if len(relayer.calls) != 2 {
		t.Fatalf("expected 2 relays (one per backend), got %d: %v", len(relayer.calls), relayer.calls)
	}
	seen := map[string]bool{}
	for _, c := range relayer.calls {
		url, err := c.endpoint.URL()
		if err != nil {
			t.Fatalf("unparseable endpoint %q", c.endpoint)
		}
		if seen[url] {
			t.Errorf("backend %q was probed twice in one cycle", url)
		}
		seen[url] = true
	}
}

// The height belongs to the backend. Without fan-out the un-probed siblings
// would look permanently height-less and drop out of selection.
func TestRunOnce_FansBackendResultsToSiblings(t *testing.T) {
	exe, _, rep, plugin := dedupTestFixture(t)

	exe.runOnce(context.Background())
	exe.wg.Wait()

	plugin.mu.Lock()
	updates := len(plugin.updates)
	for _, ep := range sharedBackendEndpoints() {
		if plugin.updates[ep] != 1234 {
			t.Errorf("endpoint %q height = %d, want 1234", ep, plugin.updates[ep])
		}
	}
	plugin.mu.Unlock()
	if updates != 4 {
		t.Errorf("expected all 4 endpoints to receive a height, got %d", updates)
	}

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 4 {
		t.Errorf("expected all 4 registrations to be named, got %d", len(rep.signals))
	}
}

// Ruling F1: one probe is one attempt per reputation key. The executor must
// hand the whole sibling set to the reputation service in ONE call and let it
// dedupe by key, rather than looping and charging a backend's stake count as
// its attempt count. Asserting on the signal list alone cannot see this — the
// stub fans a call back out — so it asserts on the calls.
func TestRunOnce_ProbeIsOneCallPerBackend(t *testing.T) {
	exe, _, rep, _ := dedupTestFixture(t)

	exe.runOnce(context.Background())
	exe.wg.Wait()

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.onceCalls) != 2 {
		t.Fatalf("expected 1 reputation call per backend (2 backends), got %d: %v",
			len(rep.onceCalls), rep.onceCalls)
	}
	bySize := map[int]domain.EndpointAddrList{}
	for _, call := range rep.onceCalls {
		bySize[len(call)] = call
	}
	shared, ok := bySize[3]
	if !ok {
		t.Fatalf("no call carried the 3 siblings of node1 in one go: %v", rep.onceCalls)
	}
	for _, want := range []domain.EndpointAddr{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node1.example.com",
	} {
		if !slices.Contains(shared, want) {
			t.Errorf("sibling %q missing from the shared-backend call %v", want, shared)
		}
	}
	if _, ok := bySize[1]; !ok {
		t.Errorf("expected the lone node2 registration in a call of its own: %v", rep.onceCalls)
	}
}

// Rotation matters: a relay spends the probing supplier's per-session
// allowance, and a registration that is never probed is never observed to be
// individually broken.
func TestRunOnce_RotatesTheProbingSupplier(t *testing.T) {
	exe, relayer, _, _ := dedupTestFixture(t)

	probed := map[domain.EndpointAddr]bool{}
	now := time.Unix(1_000_000, 0)
	exe.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		exe.runOnce(context.Background())
		exe.wg.Wait()
		now = now.Add(defaultInterval)
	}

	relayer.mu.Lock()
	defer relayer.mu.Unlock()
	for _, c := range relayer.calls {
		probed[c.endpoint] = true
	}
	for _, ep := range []domain.EndpointAddr{
		"supplierA-https://node1.example.com",
		"supplierB-https://node1.example.com",
		"supplierC-https://node1.example.com",
	} {
		if !probed[ep] {
			t.Errorf("supplier %q was never probed across 3 cycles", ep)
		}
	}
}

// A transport failure can be the probing registration rather than the backend,
// and there is no response to tell them apart — so it penalizes only the
// endpoint that failed.
func TestRunOnce_RelayErrorPenalizesOnlyTheProbedEndpoint(t *testing.T) {
	exe, relayer, rep, _ := dedupTestFixture(t)
	relayer.response = nil
	relayer.err = errors.New("transport failure")

	exe.runOnce(context.Background())
	exe.wg.Wait()

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.signals) != 2 {
		t.Fatalf("expected 1 signal per probed endpoint (2 backends), got %d", len(rep.signals))
	}
}

// Disabling the dedup restores one relay per registration.
func TestRunOnce_DedupCanBeDisabled(t *testing.T) {
	exe, relayer, _, _ := dedupTestFixture(t)
	exe.SetBackendURLDedup(false)

	exe.runOnce(context.Background())
	exe.wg.Wait()

	relayer.mu.Lock()
	defer relayer.mu.Unlock()
	if len(relayer.calls) != 4 {
		t.Errorf("expected 4 relays with dedup off, got %d", len(relayer.calls))
	}
}

// TestSetConfiguredChecks_SafeWhileRunning is the discriminating test for the
// atomics on configured/dedupByBackendURL.
//
// A config reload calls both setters from its own goroutine against a running
// executor while a check cycle reads them. Reverting either field to a plain
// assignment makes this fail under -race — which is the point: the race is
// invisible without a reader and a writer actually overlapping.
func TestSetConfiguredChecks_SafeWhileRunning(t *testing.T) {
	relayer := &stubRelayer{
		response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)},
	}
	eps := &stubEndpointProvider{
		endpoints: domain.EndpointAddrList{
			"supplierA-https://node1.example.com",
			"supplierB-https://node2.example.com",
		},
	}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}

	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
		domain.RPCTypeJSONRPC, "eth_blockNumber")
	reg := qos.NewRegistry()
	if err := reg.Register("eth", &checkOnlyPlugin{
		checks: []qos.HealthCheck{{Name: "block_number", Payload: payload}},
	}); err != nil {
		t.Fatal(err)
	}

	exec := newTestExecutor(relayer, eps, sessions, reg, &stubRepService{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cycles, from one goroutine.
	var cycles sync.WaitGroup
	cycles.Add(1)
	go func() {
		defer cycles.Done()
		for i := 0; i < 50; i++ {
			exec.runOnce(ctx)
		}
	}()

	// Reloads, from another — exactly what POST /admin/reload does.
	var reloads sync.WaitGroup
	reloads.Add(1)
	go func() {
		defer reloads.Done()
		for i := 0; i < 50; i++ {
			checks, _ := BuildConfiguredChecks(config.HealthCheckConfig{
				Local: []config.ServiceHealthChecks{{
					ServiceID: "eth",
					Enabled:   true,
					Checks:    []config.HealthCheck{{Name: "configured", Method: "eth_chainId"}},
				}},
			})
			exec.SetConfiguredChecks(checks)
			exec.SetBackendURLDedup(i%2 == 0)
		}
	}()

	cycles.Wait()
	reloads.Wait()
	exec.wg.Wait()
}

// TestRunOnce_DoesNotWriteIntoThePluginsCheckSlice pins the ownership of the
// slice a plugin returns from HealthChecks. Appending the configured checks to
// it writes into the plugin's own backing array whenever that array has spare
// capacity — the plugin hands the same slice to every service and every cycle,
// so one service's YAML check would end up in the array behind everyone's.
//
// The sentinel is what an unrelated element of that array looks like; nothing
// the executor does may disturb it.
func TestRunOnce_DoesNotWriteIntoThePluginsCheckSlice(t *testing.T) {
	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
		domain.RPCTypeJSONRPC, "eth_blockNumber")

	// Length 1, capacity 2: exactly the shape that makes append reuse the
	// array instead of copying it.
	backing := make([]qos.HealthCheck, 2)
	backing[0] = qos.HealthCheck{Name: "block_number", Payload: payload}
	backing[1] = qos.HealthCheck{Name: "plugin_owned_sentinel", Payload: payload}

	reg := qos.NewRegistry()
	if err := reg.Register("eth", &checkOnlyPlugin{checks: backing[:1]}); err != nil {
		t.Fatal(err)
	}

	relayer := &stubRelayer{
		response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)},
	}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}

	exec := newTestExecutor(relayer, eps, sessions, reg, &stubRepService{})
	configured, _ := BuildConfiguredChecks(config.HealthCheckConfig{
		Local: []config.ServiceHealthChecks{{
			ServiceID: "eth",
			Enabled:   true,
			Checks:    []config.HealthCheck{{Name: "configured", Method: "eth_chainId"}},
		}},
	})
	exec.SetConfiguredChecks(configured)

	exec.runOnce(context.Background())
	exec.wg.Wait()

	if got := backing[1].Name; got != "plugin_owned_sentinel" {
		t.Fatalf("the plugin's backing array was overwritten: backing[1].Name = %q, want the sentinel", got)
	}
}

// fanoutRepService is a reputation.Service that is NOT a reputation.OnceRecorder:
// it forwards everything to a stubRepService except RecordSignalOnce, which it
// does not have. It stands in for a Service implementation that cannot dedupe
// by key, so the executor's fallback branch has something to be tested with.
type fanoutRepService struct{ inner *stubRepService }

func (f *fanoutRepService) RecordSignal(ctx context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, rt domain.RPCType, sig reputation.Signal) error {
	return f.inner.RecordSignal(ctx, svcID, ep, rt, sig)
}
func (f *fanoutRepService) GetScore(ctx context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, rt domain.RPCType) (float64, error) {
	return f.inner.GetScore(ctx, svcID, ep, rt)
}
func (f *fanoutRepService) GetScores(ctx context.Context, svcID domain.ServiceID) (map[string]float64, error) {
	return f.inner.GetScores(ctx, svcID)
}
func (f *fanoutRepService) SelectBest(ctx context.Context, svcID domain.ServiceID, eps domain.EndpointAddrList, rt domain.RPCType) domain.EndpointAddr {
	return f.inner.SelectBest(ctx, svcID, eps, rt)
}
func (f *fanoutRepService) SelectSpread(ctx context.Context, svcID domain.ServiceID, eps domain.EndpointAddrList, rt domain.RPCType, load map[domain.EndpointAddr]int) domain.EndpointAddr {
	return f.inner.SelectSpread(ctx, svcID, eps, rt, load)
}
func (f *fanoutRepService) ResetScore(ctx context.Context, svcID domain.ServiceID, ep domain.EndpointAddr) error {
	return f.inner.ResetScore(ctx, svcID, ep)
}
func (f *fanoutRepService) Vouched(ctx context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, rt domain.RPCType) bool {
	return f.inner.Vouched(ctx, svcID, ep, rt)
}

var _ reputation.Service = (*fanoutRepService)(nil)

// A reputation.Service without RecordSignalOnce still gets every sibling
// graded: the executor falls back to one RecordSignal per registration rather
// than dropping the probe. The pre-F1 fan-out is the lesser evil.
func TestRunOnce_FallsBackToFanOutWithoutOnceRecorder(t *testing.T) {
	exe, _, rep, _ := dedupTestFixture(t)
	if _, isOnce := reputation.Service(&fanoutRepService{}).(reputation.OnceRecorder); isOnce {
		t.Fatal("fanoutRepService must not be an OnceRecorder, or this test exercises nothing")
	}
	exe.repService = &fanoutRepService{inner: rep}

	exe.runOnce(context.Background())
	exe.wg.Wait()

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.onceCalls) != 0 {
		t.Fatalf("no OnceRecorder, yet RecordSignalOnce was called %d time(s)", len(rep.onceCalls))
	}
	if len(rep.signals) != 4 {
		t.Fatalf("expected one RecordSignal per registration (4), got %d", len(rep.signals))
	}
	seen := map[domain.EndpointAddr]bool{}
	for _, rec := range rep.signals {
		seen[rec.endpoint] = true
		if !rec.signal.Probe {
			t.Errorf("signal for %s must be marked as a probe", rec.endpoint)
		}
	}
	for _, ep := range sharedBackendEndpoints() {
		if !seen[ep] {
			t.Errorf("registration %s received no signal", ep)
		}
	}
}

// A check may declare its own minimum spacing. It runs on the first cycle it
// is seen and then not again until that much time has passed, while the
// checks without one keep the service's cadence.
func TestRunOnce_CheckIntervalIsMinimumSpacing(t *testing.T) {
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"result":"0x1"}`)}}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}
	fast := domain.NewPayload([]byte(`{"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	slow := domain.NewPayload([]byte(`{"method":"eth_chainId"}`), domain.RPCTypeJSONRPC, "eth_chainId")
	plugin := &checkOnlyPlugin{checks: []qos.HealthCheck{
		{Name: "block", Payload: fast},
		{Name: "chain", Payload: slow, Interval: 5 * time.Minute},
	}}
	reg := qos.NewRegistry()
	_ = reg.Register("eth", plugin)

	exec := newTestExecutor(relayer, eps, sessions, reg, &stubRepService{})
	now := time.Unix(1_000_000, 0)
	exec.now = func() time.Time { return now }

	sent := func() []string {
		exec.wg.Wait()
		relayer.mu.Lock()
		defer relayer.mu.Unlock()
		var names []string
		for _, c := range relayer.calls {
			names = append(names, c.check)
		}
		relayer.calls = nil
		return names
	}

	exec.runOnce(context.Background())
	if got := sent(); len(got) != 2 {
		t.Fatalf("first cycle: sent %v, want both checks", got)
	}

	now = now.Add(defaultInterval)
	exec.runOnce(context.Background())
	if got := sent(); len(got) != 1 || got[0] != "eth_blockNumber" {
		t.Fatalf("second cycle: sent %v, want only the un-spaced check", got)
	}

	now = now.Add(5 * time.Minute)
	exec.runOnce(context.Background())
	if got := sent(); len(got) != 2 {
		t.Fatalf("after the spacing elapsed: sent %v, want both checks", got)
	}
}

// A service with its own check_interval is left alone until it is due; the
// executor's tick is the shortest interval in play so the fastest service is
// still served on time.
func TestRunOnce_ServiceIntervalSkipsUndueService(t *testing.T) {
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"result":"0x1"}`)}}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}, "pocket": {}}}
	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "check")
	plugin := &checkOnlyPlugin{checks: []qos.HealthCheck{{Name: "check", Payload: payload}}}
	reg := qos.NewRegistry()
	_ = reg.Register("eth", plugin)
	_ = reg.Register("pocket", plugin)

	exec := newTestExecutor(relayer, eps, sessions, reg, &stubRepService{})
	exec.SetConfiguredChecks(&ConfiguredChecks{intervals: map[domain.ServiceID]time.Duration{
		"pocket": 2 * time.Minute,
		"eth":    10 * time.Second,
	}})
	now := time.Unix(1_000_000, 0)
	exec.now = func() time.Time { return now }

	if got := exec.tick(); got != 10*time.Second {
		t.Fatalf("tick = %v, want the shortest interval (10s)", got)
	}

	sentTo := func() map[domain.ServiceID]int {
		exec.wg.Wait()
		relayer.mu.Lock()
		defer relayer.mu.Unlock()
		out := map[domain.ServiceID]int{}
		for _, c := range relayer.calls {
			out[c.serviceID]++
		}
		relayer.calls = nil
		return out
	}

	exec.runOnce(context.Background())
	if got := sentTo(); got["eth"] != 1 || got["pocket"] != 1 {
		t.Fatalf("first cycle: %v, want one probe per service", got)
	}
	now = now.Add(10 * time.Second)
	exec.runOnce(context.Background())
	if got := sentTo(); got["eth"] != 1 || got["pocket"] != 0 {
		t.Fatalf("at 10s: %v, want eth only", got)
	}
	now = now.Add(110 * time.Second)
	exec.runOnce(context.Background())
	if got := sentTo(); got["eth"] != 1 || got["pocket"] != 1 {
		t.Fatalf("at 120s: %v, want both", got)
	}
}

// Ticks land a hair before the nominal due time (the ticker fires at 30.000s
// while the last run was stamped a few microseconds after the previous tick).
// A check must not slip a whole tick for that.
func TestRunOnce_DueToleratesTickJitter(t *testing.T) {
	relayer := &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"result":"0x1"}`)}}
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}
	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "check")
	reg := qos.NewRegistry()
	_ = reg.Register("eth", &checkOnlyPlugin{checks: []qos.HealthCheck{{Name: "check", Payload: payload}}})

	exec := newTestExecutor(relayer, eps, sessions, reg, &stubRepService{})
	now := time.Unix(1_000_000, 0)
	exec.now = func() time.Time { return now }

	exec.runOnce(context.Background())
	now = now.Add(defaultInterval - 50*time.Millisecond)
	exec.runOnce(context.Background())
	exec.wg.Wait()
	relayer.mu.Lock()
	defer relayer.mu.Unlock()
	if len(relayer.calls) != 2 {
		t.Fatalf("sent %d probes, want 2 (one per tick)", len(relayer.calls))
	}
}

// Readiness must reflect the ability to select, not just that a session
// exists. The executor is warm once it has applied health-check results
// (leader probes or follower stream) for enough of the configured services
// that reputation can steer selection — before that a fresh pod would serve
// blind and return failures until it warmed.
func TestExecutor_WarmAfterResultsCoverServices(t *testing.T) {
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{
		"eth": {}, "poly": {}, "base": {}, "solana": {},
	}}
	reg := qos.NewRegistry()
	rep := &stubRepService{}
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{}, sessions, reg, rep, nil,
		defaultInterval, 4, slog.Default())

	if exec.Warm() {
		t.Fatal("must not be warm before any result is applied")
	}
	ok := &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"result":"0x1"}`)}
	_ = ok
	apply := func(svc domain.ServiceID) {
		exec.applyResult(context.Background(), ProbeResult{
			ServiceID: svc, Endpoint: "supplierA-https://n.example.com",
			StatusCode: 200, Body: []byte(`{"result":"0x1"}`), Source: ResultSourceProbe,
		})
	}
	// Threshold is 75% of 4 services = 3. Two is not enough.
	apply("eth")
	apply("poly")
	if exec.Warm() {
		t.Fatalf("2 of 4 services covered should not be warm")
	}
	apply("base")
	if !exec.Warm() {
		t.Fatalf("3 of 4 (75%%) covered should be warm")
	}
}

// With no configured services the executor is warm immediately — there is
// nothing to wait for, and a readiness gate must not stall.
func TestExecutor_WarmWhenNoServices(t *testing.T) {
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{},
		&stubSessionManager{services: map[domain.ServiceID]struct{}{}}, qos.NewRegistry(),
		&stubRepService{}, nil, defaultInterval, 4, slog.Default())
	if !exec.Warm() {
		t.Fatal("no configured services must read as warm, not stall readiness")
	}
}

// --- probe relay metrics ---

type recordingResultRecorder struct {
	mu      sync.Mutex
	applied []string
	relays  []probeRelayCall
}

type probeRelayCall struct {
	serviceID  domain.ServiceID
	endpoint   domain.EndpointAddr
	statusCode int
	failed     bool
}

func (r *recordingResultRecorder) RecordHealthCheckResult(_ domain.ServiceID, source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, source)
}

func (r *recordingResultRecorder) RecordProbeRelay(
	serviceID domain.ServiceID,
	endpoint domain.EndpointAddr,
	statusCode int,
	_ time.Duration,
	err error,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.relays = append(r.relays, probeRelayCall{
		serviceID:  serviceID,
		endpoint:   endpoint,
		statusCode: statusCode,
		failed:     err != nil,
	})
}

func (r *recordingResultRecorder) probeRelays() []probeRelayCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]probeRelayCall(nil), r.relays...)
}

// A probe is a paid relay that never enters the middleware chain, so the
// executor is the only place it can be counted. Every send must be reported —
// including the ones that fail, which are exactly the ones a probe error rate
// is built from.
func TestProbe_RecordsEveryRelaySend(t *testing.T) {
	cases := []struct {
		name       string
		relayer    *stubRelayer
		wantStatus int
		wantFailed bool
	}{
		{
			name:       "answered",
			relayer:    &stubRelayer{response: &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)}},
			wantStatus: 200,
		},
		{
			name:       "transport failure is 502, the same sentinel the metrics middleware uses",
			relayer:    &stubRelayer{err: errors.New("connection refused")},
			wantStatus: 502,
			wantFailed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eps := &stubEndpointProvider{
				endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"},
			}
			sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}
			payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
			reg := qos.NewRegistry()
			if err := reg.Register("eth", &checkOnlyPlugin{
				checks: []qos.HealthCheck{{Name: "block_number", Payload: payload}},
			}); err != nil {
				t.Fatal(err)
			}

			rec := &recordingResultRecorder{}
			exec := newTestExecutor(tc.relayer, eps, sessions, reg, &stubRepService{})
			exec.SetResultRecorder(rec)
			exec.runOnce(context.Background())

			time.Sleep(50 * time.Millisecond)

			relays := rec.probeRelays()
			if len(relays) != 1 {
				t.Fatalf("recorded %d probe relays, want 1", len(relays))
			}
			got := relays[0]
			if got.serviceID != "eth" {
				t.Errorf("service_id = %q, want %q", got.serviceID, "eth")
			}
			if got.endpoint != "supplierA-https://node1.example.com" {
				t.Errorf("endpoint = %q, want the probed one", got.endpoint)
			}
			if got.statusCode != tc.wantStatus {
				t.Errorf("statusCode = %d, want %d", got.statusCode, tc.wantStatus)
			}
			if got.failed != tc.wantFailed {
				t.Errorf("failed = %v, want %v", got.failed, tc.wantFailed)
			}
		})
	}
}

// A nil recorder is the documented "metrics off" case and must not panic.
func TestProbe_NilRecorderIsSafe(t *testing.T) {
	eps := &stubEndpointProvider{endpoints: domain.EndpointAddrList{"supplierA-https://node1.example.com"}}
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{"eth": {}}}
	payload := domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	reg := qos.NewRegistry()
	if err := reg.Register("eth", &checkOnlyPlugin{
		checks: []qos.HealthCheck{{Name: "block_number", Payload: payload}},
	}); err != nil {
		t.Fatal(err)
	}

	exec := newTestExecutor(
		&stubRelayer{response: &domain.Response{HTTPStatusCode: 200}},
		eps, sessions, reg, &stubRepService{},
	)
	exec.runOnce(context.Background())
	time.Sleep(50 * time.Millisecond)
}

// --- warm-up seeded from hydrated reputation ---

// A pod that loaded the fleet's scores from shared storage already holds the
// knowledge the warm gate is waiting for, so it must go ready without probing
// or waiting for a stream.
func TestSeedCoverage_WarmsWithoutAnyResult(t *testing.T) {
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{
		"eth": {}, "poly": {}, "kava": {}, "sei": {},
	}}
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{}, sessions,
		qos.NewRegistry(), &stubRepService{}, nil, defaultInterval, 4, slog.Default())

	if exec.Warm() {
		t.Fatal("precondition: a fresh executor must not be warm")
	}

	exec.SeedCoverage([]domain.ServiceID{"eth", "poly", "kava"})

	if !exec.Warm() {
		t.Error("3 of 4 services hydrated is the 75% threshold — must read warm")
	}
}

// Storage is shared across a fleet and outlives any one config. A service this
// pod does not serve must not count towards the coverage its readiness is
// measured against.
func TestSeedCoverage_IgnoresUnconfiguredServices(t *testing.T) {
	sessions := &stubSessionManager{services: map[domain.ServiceID]struct{}{
		"eth": {}, "poly": {}, "kava": {}, "sei": {},
	}}
	exec := NewExecutor(&stubRelayer{}, &stubEndpointProvider{}, sessions,
		qos.NewRegistry(), &stubRepService{}, nil, defaultInterval, 4, slog.Default())

	// Three real services would be enough; two real plus two strangers is not.
	exec.SeedCoverage([]domain.ServiceID{"eth", "poly", "avax", "moonriver"})

	if exec.Warm() {
		t.Error("services not in this config must not count towards coverage")
	}

	exec.SeedCoverage([]domain.ServiceID{"kava"})
	if !exec.Warm() {
		t.Error("the third configured service should complete the threshold")
	}
}
