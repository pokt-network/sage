package shannon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/websockets"
)

// spyRepSvc records reputation signals for assertion.
type spyRepSvc struct {
	calls []struct {
		serviceID domain.ServiceID
		endpoint  domain.EndpointAddr
		signal    reputation.Signal
	}
}

func (s *spyRepSvc) RecordSignal(_ context.Context, svcID domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType, sig reputation.Signal) error {
	s.calls = append(s.calls, struct {
		serviceID domain.ServiceID
		endpoint  domain.EndpointAddr
		signal    reputation.Signal
	}{svcID, ep, sig})
	return nil
}
func (s *spyRepSvc) GetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) (float64, error) {
	return 100, nil
}
func (s *spyRepSvc) GetScores(_ context.Context, _ domain.ServiceID) (map[string]float64, error) {
	return nil, nil
}
func (s *spyRepSvc) SelectBest(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}
func (s *spyRepSvc) SelectSpread(_ context.Context, _ domain.ServiceID, eps domain.EndpointAddrList, _ domain.RPCType, _ map[domain.EndpointAddr]int) domain.EndpointAddr {
	if len(eps) == 0 {
		return ""
	}
	return eps[0]
}
func (s *spyRepSvc) ResetScore(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr) error {
	return nil
}
func (s *spyRepSvc) Vouched(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.RPCType) bool {
	return true
}

// newDisabledQueue returns an observe.Queue with Enabled=false so Submit is a
// no-op. WS relayer tests that only want to exercise reputation paths use this.
func newDisabledQueue() *observe.Queue {
	return observe.NewQueue(observe.QueueConfig{Enabled: false}, nil, newTestLogger())
}

func TestNewWSRelayer_PanicsOnMissingDeps(t *testing.T) {
	cases := []struct {
		name string
		deps WSRelayerDeps
	}{
		{"no protocol", WSRelayerDeps{
			Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
			Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
		}},
		{"no reputation", WSRelayerDeps{
			Protocol: &Protocol{}, Observe: newDisabledQueue(),
			Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
		}},
		{"no observe", WSRelayerDeps{
			Protocol: &Protocol{}, Reputation: &spyRepSvc{},
			Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
		}},
		{"no flags", WSRelayerDeps{
			Protocol: &Protocol{}, Reputation: &spyRepSvc{},
			Observe: newDisabledQueue(), Logger: newTestLogger(),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("expected panic for %s", tc.name)
				}
			}()
			_ = NewWSRelayer(tc.deps)
		})
	}
}

func TestFrameSeverityToSignal(t *testing.T) {
	cases := []struct {
		name     string
		res      heuristic.AnalysisResult
		wantType reputation.SignalType
	}{
		{
			"success",
			heuristic.AnalysisResult{ShouldPenalize: false},
			reputation.SignalSuccess,
		},
		{
			"minor",
			heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMinor, Reason: "x"},
			reputation.SignalMinorError,
		},
		{
			"major→minor",
			heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMajor, Reason: "x"},
			reputation.SignalMinorError,
		},
		{
			"critical→major",
			heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityCritical, Reason: "x"},
			reputation.SignalMajorError,
		},
		{
			"quota exceeded stays major",
			heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMajor, Reason: heuristic.ReasonQuotaExceeded},
			reputation.SignalMajorError,
		},
		{
			"fatal→critical (never fatal from one frame)",
			heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityFatal, Reason: "x"},
			reputation.SignalCriticalError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := frameSeverityToSignal(tc.res, 100*time.Millisecond)
			if sig.Type != tc.wantType {
				t.Errorf("got signal type %q, want %q", sig.Type, tc.wantType)
			}
		})
	}
}

func TestWSRelayer_HandleEndpointFrame_Success(t *testing.T) {
	rep := &spyRepSvc{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: rep, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	r.handleEndpointFrame("eth", "ep1", []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil, 10*time.Millisecond, nil)
	if len(rep.calls) != 1 {
		t.Fatalf("want 1 signal recorded, got %d", len(rep.calls))
	}
	if rep.calls[0].signal.Type != reputation.SignalSuccess {
		t.Errorf("want success signal, got %q", rep.calls[0].signal.Type)
	}
}

func TestWSRelayer_HandleEndpointFrame_HeuristicPenalty(t *testing.T) {
	rep := &spyRepSvc{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: rep, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	// HTML error page → critical heuristic → downgraded to major for per-frame.
	r.handleEndpointFrame("eth", "ep1", []byte(`<!DOCTYPE html><body>error</body></html>`), nil, 0, nil)
	if len(rep.calls) != 1 {
		t.Fatalf("want 1 signal, got %d", len(rep.calls))
	}
	if rep.calls[0].signal.Type != reputation.SignalMajorError {
		t.Errorf("want major error (critical→major downgrade), got %q", rep.calls[0].signal.Type)
	}
}

func TestWSRelayer_LoadCounters(t *testing.T) {
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	r.incLoad("epA")
	r.incLoad("epA")
	r.incLoad("epB")

	load := r.snapshotLoad()
	if load["epA"] != 2 {
		t.Errorf("epA load = %d, want 2", load["epA"])
	}
	if load["epB"] != 1 {
		t.Errorf("epB load = %d, want 1", load["epB"])
	}

	r.decLoad("epA")
	r.decLoad("epA")
	r.decLoad("epB")

	load = r.snapshotLoad()
	if len(load) != 0 {
		t.Errorf("expected empty load snapshot after all decrements, got %v", load)
	}
}

// newEchoSupplier is a stand-in supplier that echoes frames back. Bridges need a
// live endpoint to connect to; the expiry tests never send traffic over it.
func newEchoSupplier(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(mt, msg); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startExpiryBridges stands up a gateway that opens one bridge per incoming
// client and watches it with watchSessionExpiry, mirroring WSRelayer.Open's
// wiring minus session/app/endpoint resolution. Returns the client-facing URL.
func startExpiryBridges(t *testing.T, r *WSRelayer, endHeight int64, procs *sync.Map) string {
	t.Helper()
	supplier := newEchoSupplier(t)
	supplierURL := "ws" + strings.TrimPrefix(supplier.URL, "http")

	var seq atomic.Int64
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		proc := &wsMessageProcessor{}
		proc.sessionActive.Store(true)

		bridge, err := websockets.StartBridge(
			req.Context(), newTestLogger(), req, w, supplierURL, http.Header{}, proc,
		)
		if err != nil {
			return
		}
		procs.Store(seq.Add(1)-1, proc)

		var end atomic.Int64
		end.Store(endHeight)
		var cur atomic.Pointer[wsMessageProcessor]
		cur.Store(proc)
		go r.watchSessionExpiry("eth", &end, &cur, bridge, newTestLogger(), nil)
		<-bridge.Done()
	}))
	t.Cleanup(gw.Close)
	return "ws" + strings.TrimPrefix(gw.URL, "http")
}

// THE fan-out case, and the reason the shared expiry channel had to go: with one
// broadcast channel, bridges consumed each other's events and — because sessions
// are keyed by serviceID+appAddr — N bridges on one session were emitted a single
// event between them. Every bridge here reads the height itself, so ALL must close.
func TestWSRelayer_EveryConcurrentBridgeSeesExpiry(t *testing.T) {
	var height atomic.Int64
	height.Store(100)

	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	r.chainHeight = height.Load
	r.expiryCheck = 10 * time.Millisecond

	var procs sync.Map
	url := startExpiryBridges(t, r, 200, &procs)

	const bridges = 8
	clients := make([]*websocket.Conn, 0, bridges)
	for i := 0; i < bridges; i++ {
		c, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}

	// Cross the boundary. Every bridge's own watcher must notice independently.
	height.Store(200)

	for i, c := range clients {
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _, err := c.ReadMessage()
		var closeErr *websocket.CloseError
		if !errors.As(err, &closeErr) {
			t.Errorf("bridge %d never learned its session ended (err: %v)", i, err)
			continue
		}
		if closeErr.Code != websocket.CloseServiceRestart {
			t.Errorf("bridge %d close code = %d, want CloseServiceRestart", i, closeErr.Code)
		}
	}

	// Each bridge must also stop signing client frames against the dead session.
	procs.Range(func(_, v any) bool {
		if proc, ok := v.(*wsMessageProcessor); ok && proc.sessionActive.Load() {
			t.Error("bridge shut down but its processor still signs against the retired session")
		}
		return true
	})
}

// A height we don't trust must not tear down live bridges: 0 means the poller has
// not reported, and guessing "expired" there is worse than letting the miner
// reject frames signed against a session that may well still be live.
func TestWSRelayer_ZeroHeightNeverExpires(t *testing.T) {
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	r.chainHeight = func() int64 { return 0 }
	r.expiryCheck = 5 * time.Millisecond

	var procs sync.Map
	url := startExpiryBridges(t, r, 200, &procs)

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Read past many watcher ticks. Nothing should arrive: a live bridge sends
	// no unsolicited frames, so the read must die of its own deadline rather
	// than of a close frame.
	_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	_, _, err = c.ReadMessage()

	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		t.Fatalf("bridge closed on an unreported height (close code %d)", closeErr.Code)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("want read deadline timeout on a still-open bridge, got: %v", err)
	}
}

// The watcher must not outlive its bridge.
func TestWSRelayer_ExpiryWatcherStopsWhenBridgeCloses(t *testing.T) {
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	r.chainHeight = func() int64 { return 100 }
	r.expiryCheck = 5 * time.Millisecond

	var procs sync.Map
	url := startExpiryBridges(t, r, 200, &procs)

	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		c, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = c.Close()
	}

	// Goroutine counts settle asynchronously; allow them to drain.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("goroutines grew from %d to %d after 10 closed bridges — watchers are leaking",
		before, runtime.NumGoroutine())
}

// --- connection cap ---

// newCappedRelayer builds a relayer with the WS flag ON and the given cap.
// Protocol is a bare &Protocol{}: a rejected Open must not reach it, which is
// half of what these tests assert.
func newCappedRelayer(t *testing.T, max int) *WSRelayer {
	t.Helper()
	// The logger matters: AvailableEndpoints logs before returning its error, so
	// a bare &Protocol{} panics on the nil logger instead of failing cleanly.
	return NewWSRelayer(WSRelayerDeps{
		Protocol:                 &Protocol{logger: newTestLogger()},
		Reputation:               &spyRepSvc{},
		Observe:                  newDisabledQueue(),
		Flags:                    featureflag.NewMemoryStore(map[string]bool{featureflag.FlagWebsocketRelays: true}),
		Logger:                   newTestLogger(),
		MaxConcurrentConnections: max,
	})
}

// At capacity, Open must refuse before touching the protocol. A bare
// &Protocol{} has nil sessions, so if the reservation were checked later — after
// endpoint selection or session lookup — this would panic instead of returning
// a 503. That is the point: refusing a flood has to be cheap, or the rejection
// path is itself the DoS.
func TestWSRelayer_OpenRejectsAtCapacity(t *testing.T) {
	r := newCappedRelayer(t, 1)

	// Occupy the only slot.
	if !r.connLimiter.Acquire() {
		t.Fatal("could not take the first slot")
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
	w := httptest.NewRecorder()

	err := r.Open(context.Background(), "eth", req, w)
	if err == nil {
		t.Fatal("Open must fail when the connection cap is reached")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(w.Body.String(), "too many concurrent") {
		t.Errorf("body = %q, want it to say why", w.Body.String())
	}
}

// A rejected connection must not consume a slot — otherwise the limiter leaks
// capacity on every rejection and the cap ratchets down to zero.
func TestWSRelayer_RejectedOpenDoesNotConsumeASlot(t *testing.T) {
	r := newCappedRelayer(t, 1)
	r.connLimiter.Acquire()

	for i := 0; i < 5; i++ {
		_ = r.Open(context.Background(), "eth", httptest.NewRequest(http.MethodGet, "/v1/ws", nil), httptest.NewRecorder())
	}

	if got := r.connLimiter.Active(); got != 1 {
		t.Errorf("Active() = %d after 5 rejections, want 1 — rejections must not acquire", got)
	}

	// The held slot is still the only one, and releasing it restores capacity.
	r.connLimiter.Release()
	if !r.connLimiter.Acquire() {
		t.Error("capacity was not restored after release")
	}
}

// Open takes the slot before selection and releases it on every early return,
// so a failure after the reservation — here, no endpoints — must not leak it.
func TestWSRelayer_FailedOpenReleasesItsSlot(t *testing.T) {
	r := newCappedRelayer(t, 4)

	// The protocol has no apps wired, so Open fails during endpoint lookup —
	// after the slot is taken. The deferred Release is what must cover this.
	_ = r.Open(context.Background(), "eth", httptest.NewRequest(http.MethodGet, "/v1/ws", nil), httptest.NewRecorder())

	if got := r.connLimiter.Active(); got != 0 {
		t.Errorf("Active() = %d after a failed Open, want 0 — the slot leaked", got)
	}
}

// The flag gate runs first: a service with WS disabled must not spend a slot.
func TestWSRelayer_FlagOffDoesNotConsumeASlot(t *testing.T) {
	r := NewWSRelayer(WSRelayerDeps{
		Protocol:                 &Protocol{},
		Reputation:               &spyRepSvc{},
		Observe:                  newDisabledQueue(),
		Flags:                    featureflag.NewMemoryStore(map[string]bool{featureflag.FlagWebsocketRelays: false}),
		Logger:                   newTestLogger(),
		MaxConcurrentConnections: 1,
	})

	_ = r.Open(context.Background(), "eth", httptest.NewRequest(http.MethodGet, "/v1/ws", nil), httptest.NewRecorder())

	if got := r.connLimiter.Active(); got != 0 {
		t.Errorf("Active() = %d, want 0 — a disabled service must not hold capacity", got)
	}
}

// Zero or negative means no cap.
func TestWSRelayer_NoCapConfigured(t *testing.T) {
	for _, max := range []int{0, -1} {
		r := newCappedRelayer(t, max)
		r.connLimiter.Acquire() // would fill a cap of 1, if there were one
		// Still reaches (and fails at) endpoint lookup rather than being refused.
		err := r.Open(context.Background(), "eth", httptest.NewRequest(http.MethodGet, "/v1/ws", nil), httptest.NewRecorder())
		if err != nil && strings.Contains(err.Error(), "connection limit") {
			t.Errorf("max=%d must not enforce a cap, got %v", max, err)
		}
	}
}

type spyWSMetrics struct {
	mu             sync.Mutex
	rejected       []string
	bound          []string // operator|owner
	released       []string
	frames         map[string]int // operator|owner|direction
	notifications  map[string]int // operator|owner|topic|kind
	probes         []string
	shareCaps      []string
	heads          []string // operator|lag|delayKnown
	mismatches     []string // operator
	sessionEnds    []string // action|blocksPast
	reissued       []string // operator
	reissueReasons []string
	retyped        []string // operator
	answers        []string // operator
	phases         []string
}

func (s *spyWSMetrics) SupplierAnswer(_ domain.ServiceID, operator string, _ time.Duration) {
	s.mu.Lock()
	s.answers = append(s.answers, operator)
	s.mu.Unlock()
}

func (s *spyWSMetrics) OpenPhase(_ domain.ServiceID, phase string, _ time.Duration) {
	s.mu.Lock()
	s.phases = append(s.phases, phase)
	s.mu.Unlock()
}

func (s *spyWSMetrics) SupplierRetyped(_ domain.ServiceID, operator, _ string) {
	s.mu.Lock()
	s.retyped = append(s.retyped, operator)
	s.mu.Unlock()
}

func (s *spyWSMetrics) SupplierReissued(_ domain.ServiceID, operator, _, reason string, n int) {
	s.mu.Lock()
	for range n {
		s.reissued = append(s.reissued, operator)
		s.reissueReasons = append(s.reissueReasons, reason)
	}
	s.mu.Unlock()
}

func (s *spyWSMetrics) SessionEndAction(_ domain.ServiceID, action string, blocksPast int64) {
	s.mu.Lock()
	s.sessionEnds = append(s.sessionEnds, fmt.Sprintf("%s|%d", action, blocksPast))
	s.mu.Unlock()
}

func (s *spyWSMetrics) SupplierHead(_ domain.ServiceID, operator, _ string, lag uint64, _ time.Duration, delayKnown bool) {
	s.mu.Lock()
	s.heads = append(s.heads, fmt.Sprintf("%s|%d|%v", operator, lag, delayKnown))
	s.mu.Unlock()
}

func (s *spyWSMetrics) SupplierHeadMismatch(_ domain.ServiceID, operator, _ string) {
	s.mu.Lock()
	s.mismatches = append(s.mismatches, operator)
	s.mu.Unlock()
}

func (s *spyWSMetrics) ForService(domain.ServiceID) websockets.Observer { return nil }
func (s *spyWSMetrics) ShareCap(_ domain.ServiceID, outcome string) {
	s.mu.Lock()
	s.shareCaps = append(s.shareCaps, outcome)
	s.mu.Unlock()
}
func (s *spyWSMetrics) Probed(_ domain.ServiceID, result string) {
	s.mu.Lock()
	s.probes = append(s.probes, result)
	s.mu.Unlock()
}
func (s *spyWSMetrics) SupplierBound(_ domain.ServiceID, operator, owner string) {
	s.mu.Lock()
	s.bound = append(s.bound, operator+"|"+owner)
	s.mu.Unlock()
}
func (s *spyWSMetrics) SupplierReleased(_ domain.ServiceID, operator, owner string, _ time.Duration) {
	s.mu.Lock()
	s.released = append(s.released, operator+"|"+owner)
	s.mu.Unlock()
}
func (s *spyWSMetrics) SupplierFrame(_ domain.ServiceID, operator, owner string, src websockets.MessageSource) {
	s.mu.Lock()
	if s.frames == nil {
		s.frames = map[string]int{}
	}
	s.frames[operator+"|"+owner+"|"+src.String()]++
	s.mu.Unlock()
}
func (s *spyWSMetrics) SupplierNotification(_ domain.ServiceID, operator, owner string, n qos.Notification) {
	s.mu.Lock()
	if s.notifications == nil {
		s.notifications = map[string]int{}
	}
	s.notifications[fmt.Sprintf("%s|%s|%s|%d", operator, owner, n.Topic, n.Kind)]++
	s.mu.Unlock()
}
func (s *spyWSMetrics) Rejected(sid domain.ServiceID, reason string) {
	s.mu.Lock()
	s.rejected = append(s.rejected, string(sid)+":"+reason)
	s.mu.Unlock()
}

// A refusal at the cap must be counted: it is the one WS failure that leaves
// no bridge, no close code and no log line a dashboard can see.
func TestWSRelayer_OpenRejectsAtCapacity_IsCounted(t *testing.T) {
	spy := &spyWSMetrics{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol:                 &Protocol{logger: newTestLogger()},
		Reputation:               &spyRepSvc{},
		Observe:                  newDisabledQueue(),
		Flags:                    featureflag.NewMemoryStore(map[string]bool{featureflag.FlagWebsocketRelays: true}),
		Logger:                   newTestLogger(),
		MaxConcurrentConnections: 1,
		Metrics:                  spy,
	})
	if !r.connLimiter.Acquire() {
		t.Fatal("could not take the first slot")
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
	if err := r.Open(context.Background(), "eth", req, httptest.NewRecorder()); err == nil {
		t.Fatal("Open must fail at capacity")
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.rejected) != 1 || spy.rejected[0] != "eth:capacity" {
		t.Fatalf("rejected = %v, want [eth:capacity]", spy.rejected)
	}
}

// untriedFirst is the rebind's selection narrowing: never the endpoint that
// just died, preferably not its operator either, unless that leaves nothing.
func TestUntriedFirst(t *testing.T) {
	a1 := domain.EndpointAddr("s1-https://a.example.com")
	a2 := domain.EndpointAddr("s2-https://b.example.com")
	b1 := domain.EndpointAddr("s3-https://a.other.com")
	all := domain.EndpointAddrList{a1, a2, b1}

	if got := untriedFirst(all, nil, true); len(got) != 3 {
		t.Fatalf("nothing tried: want all, got %v", got)
	}
	got := untriedFirst(all, map[domain.EndpointAddr]bool{a1: true}, true)
	if len(got) != 1 || got[0] != b1 {
		t.Fatalf("operator-aware: want only the other operator, got %v", got)
	}
	got = untriedFirst(all, map[domain.EndpointAddr]bool{a1: true}, false)
	if len(got) != 2 {
		t.Fatalf("not operator-aware: want the two untried, got %v", got)
	}
	got = untriedFirst(all, map[domain.EndpointAddr]bool{a1: true, a2: true, b1: true}, true)
	if len(got) != 3 {
		t.Fatalf("everything tried: want the full list back, got %v", got)
	}
	got = untriedFirst(domain.EndpointAddrList{a1, a2}, map[domain.EndpointAddr]bool{a1: true}, true)
	if len(got) != 1 || got[0] != a2 {
		t.Fatalf("only the same operator left: want it anyway, got %v", got)
	}
}

// Only a periodic feed gone silent is a stall. A logs filter that has matched
// nothing for minutes is honest; judging it cost the supplier a major penalty
// and the client a rebind every minute.
func TestStalled_OnlyPeriodicFeedsAreJudged(t *testing.T) {
	quiet := qos.NewSubscriptionRegistry(&evm.Plugin{})
	quiet.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["logs",{"address":"0x0"}]}`))
	quiet.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xlogs"}`))
	time.Sleep(5 * time.Millisecond)
	if stalled(quiet, time.Millisecond) {
		t.Error("a silent logs subscription must not be judged a stall")
	}

	heads := qos.NewSubscriptionRegistry(&evm.Plugin{})
	heads.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`))
	heads.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xheads"}`))
	if stalled(heads, time.Minute) {
		t.Error("a newHeads feed acked just now is not stalled")
	}
	time.Sleep(5 * time.Millisecond)
	if !stalled(heads, time.Millisecond) {
		t.Error("a newHeads feed silent past the timeout is a stall")
	}
}

// A chatty feed must not grade its supplier once per frame: one success per
// interval, while a penalty is recorded every time, and a new supplier after
// a rebind is graded at once.
func TestWSRelayer_SuccessSignalsAreGated(t *testing.T) {
	rep := &spyRepSvc{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: rep, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(),
	})
	var gate wsSuccessGate
	ok := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":{}}}`)
	for i := 0; i < 50; i++ {
		r.handleEndpointFrame("eth", "ep1", ok, nil, time.Millisecond, &gate)
	}
	r.handleEndpointFrame("eth", "ep1", []byte(`<!DOCTYPE html><body>error</body></html>`), nil, 0, &gate)
	r.handleEndpointFrame("eth", "ep2", ok, nil, time.Millisecond, &gate)

	var successes, penalties int
	for _, c := range rep.calls {
		if c.signal.Type == reputation.SignalSuccess {
			successes++
		} else {
			penalties++
		}
	}
	if successes != 2 || penalties != 1 {
		t.Fatalf("successes=%d penalties=%d, want 2 (one per supplier) and 1", successes, penalties)
	}
}

// A WebSocket bind must not go to a node behind the chain head: a newHeads
// subscriber stays for hours and would stream stale heads the whole time.
// The filter is the plugin's own, and never narrows to nothing.
func TestWSRelayer_FreshestDropsLaggingNodes(t *testing.T) {
	plugin := evm.NewPlugin(nil, evm.Config{SyncAllowance: 5})
	reg := qos.NewRegistry()
	if err := reg.Register("eth", plugin); err != nil {
		t.Fatal(err)
	}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(), QoS: reg,
	})
	fresh1, fresh2, stale := domain.EndpointAddr("s1-wss://a.example"), domain.EndpointAddr("s2-wss://b.example"), domain.EndpointAddr("s3-wss://c.example")
	plugin.UpdateBlockHeight(fresh1, 1000)
	plugin.UpdateBlockHeight(fresh2, 999)
	plugin.UpdateBlockHeight(stale, 900)

	got := r.freshest("eth", domain.EndpointAddrList{fresh1, fresh2, stale})
	if got.Contains(stale) || len(got) != 2 {
		t.Fatalf("freshest = %v, want the two endpoints near the head", got)
	}

	// Only lagging nodes left: the plugin degrades rather than empty the pool.
	if got := r.freshest("eth", domain.EndpointAddrList{stale}); len(got) != 1 {
		t.Fatalf("freshest of one = %v, want it kept", got)
	}
	// No plugin for the service: unchanged.
	if got := r.freshest("unknown", domain.EndpointAddrList{fresh1, stale}); len(got) != 2 {
		t.Fatalf("no plugin: freshest = %v, want both", got)
	}
}

// At a session boundary the manager still serves the ended session until the
// next is fetched; a rebind taken then lands on it and closes the bridge. The
// watcher waits for the next session while the ended one is honoured.
func TestSessionEndAction(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		height, actedOn, grace int64
		rebindable, ready      bool
		want                   sessionEndActionKind
	}{
		{"before the end", 99, 0, 101, true, false, sessionWait},
		{"at the end, next session not fetched", 100, 0, 101, true, false, sessionWait},
		{"in grace, next session not fetched", 101, 0, 101, true, false, sessionWait},
		{"next session fetched", 100, 0, 101, true, true, sessionRebind},
		{"past grace", 102, 0, 101, true, false, sessionRebind},
		{"already rebound for this end", 101, 100, 101, true, true, sessionClose},
		{"no rebind handler", 100, 0, 101, false, true, sessionClose},
	} {
		if got := sessionEndAction(tc.height, 100, tc.actedOn, tc.grace, tc.rebindable, tc.ready); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A rebind for a session that ended or an operator's request is not the
// supplier's failure; a dead socket or a stalled feed is.
func TestLossIsSuppliers(t *testing.T) {
	for _, tc := range []struct {
		cause error
		want  bool
	}{
		{websockets.ErrBridgeSessionExpired, false},
		{websockets.ErrBridgeReplaceRequested, false},
		{websockets.ErrBridgeStalled, true},
		{errors.New("read tcp: connection reset"), true},
		{fmt.Errorf("%w: read from endpoint: %w", websockets.ErrBridgeConnectionFailed, &websocket.CloseError{Code: 4002, Text: "stake limit exceeded"}), false},
		{fmt.Errorf("%w: read from endpoint: %w", websockets.ErrBridgeConnectionFailed, &websocket.CloseError{Code: 4000, Text: "session expired"}), false},
		{fmt.Errorf("%w: read from endpoint: %w", websockets.ErrBridgeConnectionFailed, &websocket.CloseError{Code: 1011, Text: "internal"}), true},
		// A frame that failed verification was graded when it arrived.
		{fmt.Errorf("%w: %w", websockets.ErrBridgeMessageProcessing, errors.New("signature verification failed")), false},
		// A miner's refusal frame was not, and the table decides.
		{fmt.Errorf("%w: %w", websockets.ErrBridgeMessageProcessing, &domain.MinerError{Codespace: "relayer_proxy", Code: 9, Message: "supplier(s) not reachable"}), true},
		{fmt.Errorf("%w: %w", websockets.ErrBridgeMessageProcessing, &domain.MinerError{Codespace: "relayer_proxy", Code: 7, Message: "offchain rate limit hit by relayer proxy"}), false},
	} {
		if got := lossIsSuppliers(tc.cause); got != tc.want {
			t.Errorf("%v: %v, want %v", tc.cause, got, tc.want)
		}
	}
}

// The session-end metric tells a rebind onto a session the manager already
// holds from one taken because grace ran out without it, and says how many
// blocks past the end each was taken: the second kind, past grace, is a bridge
// signing against a session the chain no longer honours.
func TestWatchSessionExpiry_CountsSessionEndActions(t *testing.T) {
	failRebind := websockets.WithEndpointLost(func(context.Context, error) (*websocket.Conn, websockets.MessageProcessor, [][]byte, error) {
		return nil, nil, nil, errors.New("no supplier")
	})
	for _, tc := range []struct {
		name   string
		height int64
		opts   []websockets.BridgeOption
		next   func(int64) (bool, int64)
		want   string
	}{
		{"next session held", 101, []websockets.BridgeOption{failRebind},
			func(end int64) (bool, int64) { return true, end + 10 }, "rebind_next_ready|1"},
		{"grace elapsed without it", 111, []websockets.BridgeOption{failRebind},
			func(end int64) (bool, int64) { return false, end + 10 }, "rebind_grace_elapsed|11"},
		{"past grace, lookup reported ready", 111, []websockets.BridgeOption{failRebind},
			func(end int64) (bool, int64) { return true, end + 10 }, "rebind_grace_elapsed|11"},
		{"no rebind possible", 100, nil, nil, "close|0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &spyWSMetrics{}
			r := NewWSRelayer(WSRelayerDeps{
				Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
				Flags: featureflag.NewMemoryStore(nil), Logger: newTestLogger(), Metrics: spy,
			})
			height := tc.height
			r.chainHeight = func() int64 { return height }
			r.expiryCheck = 10 * time.Millisecond

			supplier := newEchoSupplier(t)
			supplierURL := "ws" + strings.TrimPrefix(supplier.URL, "http")
			watched := make(chan struct{})
			gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(watched)
				proc := &wsMessageProcessor{}
				proc.sessionActive.Store(true)
				bridge, err := websockets.StartBridge(req.Context(), newTestLogger(), req, w, supplierURL, http.Header{}, proc, tc.opts...)
				if err != nil {
					return
				}
				var end atomic.Int64
				end.Store(100)
				var cur atomic.Pointer[wsMessageProcessor]
				cur.Store(proc)
				r.watchSessionExpiry("eth", &end, &cur, bridge, newTestLogger(), tc.next)
			}))
			defer gw.Close()

			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(gw.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			select {
			case <-watched:
			case <-time.After(3 * time.Second):
				t.Fatal("watcher never acted on the ended session")
			}
			spy.mu.Lock()
			defer spy.mu.Unlock()
			if len(spy.sessionEnds) != 1 || spy.sessionEnds[0] != tc.want {
				t.Fatalf("session end actions = %v, want [%s]", spy.sessionEnds, tc.want)
			}
		})
	}
}
