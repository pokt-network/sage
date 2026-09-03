package shannon

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"

	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/websockets"
)

const (
	// wsFeatureFlag gates WS relays per-service. Default off.
	wsFeatureFlag = "websocket_relays"

	// wsStallTimeout is how long a connection with established subscriptions
	// may receive nothing before its supplier is replaced. A minute: longer
	// than any chain's block time by a wide margin, so a quiet-but-honest
	// subscription (a logs filter that rarely matches) is not what fires it
	// — a supplier whose feed silently stopped is. wsStallCheckInterval is
	// the poll.
	wsStallTimeout       = 60 * time.Second
	wsStallCheckInterval = 5 * time.Second

	// wsExpiryCheckInterval is how often a bridge asks whether its own session
	// has ended. The block poller only refreshes the height every
	// blockPollInterval (10s), so checking faster than that just re-reads the
	// same value; worst-case lag to notice a boundary is one poll plus one tick.
	wsExpiryCheckInterval = 5 * time.Second
)

// WSRelayerDeps bundles the collaborators a WSRelayer needs.
//
// Every dependency is required in production; constructor panics on any nil.
// This is the structural mitigation for PATH's "WS bypasses reputation" bug:
// a WSRelayer cannot be built without reputation + observation + flags, so
// there is no "raw" path that silently skips them.
type WSRelayerDeps struct {
	Protocol   *Protocol
	Reputation reputation.Service
	Observe    *observe.Queue
	Flags      featureflag.FlagStore
	Logger     *slog.Logger

	// FrameObservationSampleRate is the fraction of routine supplier frames
	// submitted to the observation pipeline (in addition to 100% of frames
	// that trip heuristic penalties). Range 0.0–1.0.
	FrameObservationSampleRate float64

	// CloseObservationSampleRate is the fraction of bridge-close events
	// submitted to the observation pipeline. Typically 1.0 — close events
	// are low volume and load-bearing for debugging.
	CloseObservationSampleRate float64

	// MaxConcurrentConnections caps live bridges across all services. Zero or
	// negative disables the cap; callers should pass the already-resolved value
	// (see config.WebSocketConfig.EffectiveMaxConcurrentConnections).
	MaxConcurrentConnections int

	// Metrics receives bridge lifecycle events. Optional: nil records
	// nothing, which is what the tests want and what production must never
	// wire (see wire.go).
	Metrics WSMetrics

	// QoS resolves the service's plugin. A plugin that implements
	// qos.SubscriptionClassifier gives the bridge a subscription registry —
	// the knowledge a rebind and a stall watchdog need. Optional: nil, or a
	// plugin without the interface, means no tracking.
	QoS *qos.Registry
}

// WSMetrics is what the relayer needs from the metrics package: a per-service
// observer for each bridge, and a counter for the upgrades it refuses before
// a bridge exists. metrics.WebSocketMetrics satisfies it.
type WSMetrics interface {
	ForService(serviceID domain.ServiceID) websockets.Observer
	Rejected(serviceID domain.ServiceID, reason string)
}

// WSRelayer is the only public entry point for opening WebSocket bridges in
// SAGE. It is responsible for selecting a supplier with load-aware spread,
// constructing the Shannon MessageProcessor that signs every outbound frame,
// and hooking each supplier frame into reputation + heuristic + observation.
type WSRelayer struct {
	deps WSRelayerDeps

	// activeLoad tracks the number of open bridges per endpoint, feeding
	// into reputation.SelectSpread to bias away from hot endpoints.
	//
	// An entry is deleted when its count reaches zero rather than left at 0
	// forever. Endpoint addresses carry a staked supplier that rotates every
	// session, so a counter per address ever bound is a map that grows for the
	// life of the process — small per entry, unbounded in count. Recorded as a
	// residual by the ever-seen-maps audit on 2026-09-01; the reputation
	// timeline was OOMKilled for the same shape.
	//
	// A mutex and a plain map, not a sync.Map of atomics. Delete-at-zero is
	// where that combination stops being safe: the delete and the decrement
	// cannot be made one step, so a concurrent open either increments a counter
	// already removed from the map or races the entry back in, and every repair
	// for that either loses a bridge's load or counts it twice. This is called
	// once per bridge opening and closing — not per frame — so the lock costs
	// nothing worth the subtlety.
	loadMu     sync.Mutex
	activeLoad map[domain.EndpointAddr]int

	// chainHeight reads the current chain head. A field so tests can drive
	// the height without a live block poller.
	chainHeight func() int64

	// expiryCheck is each bridge's expiry tick. A field so tests need not
	// wait seconds.
	expiryCheck time.Duration

	// stallTimeout is how long a bridge with live subscriptions may go
	// without a notification before it is rebound; stallCheck is how often
	// that is polled. Fields so tests need not wait a minute.
	stallTimeout time.Duration
	stallCheck   time.Duration

	// live tracks every open bridge by service, for RebindService.
	live sync.Map // *websockets.Bridge → domain.ServiceID

	// connLimiter caps concurrent live bridges. Nil means no cap; every method
	// on it is nil-safe.
	//
	// Deliberately global rather than per-service or per-endpoint: goroutines
	// and file descriptors are process-wide, so that is the level the ceiling
	// has to sit at. activeLoad is the per-endpoint counter, and it exists for
	// a different purpose — biasing selection away from hot endpoints, not
	// refusing work.
	connLimiter *websockets.ConnectionLimiter
}

// NewWSRelayer validates deps and returns a WSRelayer. Panics on missing
// required collaborators — this is intentional: a partially-wired WS path
// is exactly the bug we're trying to prevent, and catching it at startup
// is strictly better than shipping it.
func NewWSRelayer(deps WSRelayerDeps) *WSRelayer {
	if deps.Protocol == nil {
		panic("shannon.NewWSRelayer: Protocol is required")
	}
	if deps.Reputation == nil {
		panic("shannon.NewWSRelayer: Reputation is required")
	}
	if deps.Observe == nil {
		panic("shannon.NewWSRelayer: Observe is required")
	}
	if deps.Flags == nil {
		panic("shannon.NewWSRelayer: Flags is required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.FrameObservationSampleRate < 0 || deps.FrameObservationSampleRate > 1 {
		deps.FrameObservationSampleRate = 0.01
	}
	if deps.CloseObservationSampleRate < 0 || deps.CloseObservationSampleRate > 1 {
		deps.CloseObservationSampleRate = 1.0
	}
	return &WSRelayer{
		deps:         deps,
		chainHeight:  deps.Protocol.LatestBlockHeight,
		expiryCheck:  wsExpiryCheckInterval,
		stallTimeout: wsStallTimeout,
		stallCheck:   wsStallCheckInterval,
		connLimiter:  websockets.NewConnectionLimiter(deps.MaxConcurrentConnections),
	}
}

// Open upgrades the incoming HTTP request to a WebSocket, selects a supplier
// endpoint using tier-cascade + load-aware weighting, opens a Shannon-signed
// bridge to the supplier, and blocks until the bridge shuts down.
//
// Returns a non-nil error only for pre-upgrade failures (flag off, no
// endpoints, endpoint resolution failures). After the upgrade succeeds, all
// further errors surface via bridge close codes to the client.
func (r *WSRelayer) Open(ctx context.Context, serviceID domain.ServiceID, req *http.Request, w http.ResponseWriter) error {
	logger := r.deps.Logger.With("component", "ws_relayer", "service_id", serviceID)

	// Feature-flag gate.
	if !r.deps.Flags.IsEnabled(ctx, wsFeatureFlag, serviceID) {
		logger.Info("ws open: feature flag off")
		http.Error(w, "websocket relays disabled for this service", http.StatusServiceUnavailable)
		return fmt.Errorf("ws open: %s flag disabled for %q", wsFeatureFlag, serviceID)
	}

	// Reserve a connection slot before doing any work for this client.
	//
	// Placed here, ahead of endpoint selection and session/app resolution, so a
	// flood arriving at capacity is refused for the price of one atomic load
	// rather than a session lookup each. Open blocks until the bridge closes,
	// so the slot covers the connection's whole life and a plain defer releases
	// it on every path — including the early returns below, where the
	// connection never went live.
	if !r.connLimiter.Acquire() {
		logger.Warn("ws open: at connection capacity, rejecting",
			"active_connections", r.connLimiter.Active(),
		)
		if r.deps.Metrics != nil {
			r.deps.Metrics.Rejected(serviceID, "capacity")
		}
		http.Error(w, "too many concurrent websocket connections", http.StatusServiceUnavailable)
		return errors.New("ws open: concurrent connection limit reached")
	}
	defer r.connLimiter.Release()

	// Pick and resolve an endpoint: tier cascade + load-aware weighted
	// random, then the session, URL and app that go with it. The same path a
	// rebind takes later, minus the exclusions.
	target, httpMsg, err := r.resolveEndpoint(ctx, serviceID, nil)
	if err != nil {
		logger.Error("ws open: resolve endpoint", "err", err)
		http.Error(w, httpMsg, http.StatusBadGateway)
		return fmt.Errorf("ws open: %w", err)
	}
	endpointAddr, ep, url, session, appAddr := target.addr, target.ep, target.url, target.session, target.appAddr

	// Increment load counter; guarantee decrement on return — for whichever
	// endpoint is current by then, since a rebind moves it.
	var current atomic.Pointer[domain.EndpointAddr]
	current.Store(&endpointAddr)
	r.incLoad(endpointAddr)
	defer func() { r.decLoad(*current.Load()) }()

	// The session the bridge is signing under moves with a rebind; the
	// expiry watcher follows it, so a rollover becomes a rebind onto the
	// next session rather than a close.
	var sessionEnd atomic.Int64
	sessionEnd.Store(session.Header.SessionEndBlockHeight)
	var currentProc atomic.Pointer[wsMessageProcessor]

	logger = logger.With("endpoint", endpointAddr, "supplier", ep.Supplier(), "url", url)
	logger.Info("ws open: starting bridge")

	// Per-frame heuristic/reputation/observation work runs on its own
	// goroutine: the bridge routes both directions through one loop, so doing
	// this inline would sit between frame receipt and client delivery (and
	// block the opposite direction too). The payload is read-only once handed
	// back. If the worker falls behind, the analysis is dropped — never the
	// frame itself.
	frameCh := make(chan wsFrameEvent, wsFrameEventQueueSize)

	subs := r.subscriptionRegistry(serviceID)
	newProcessor := func(t *wsTarget) *wsMessageProcessor {
		addr := t.addr
		return newWSMessageProcessor(
			ctx,
			r.deps.Protocol,
			t.session.Header,
			t.ep.Supplier(),
			t.ep.Addr(),
			t.app,
			func(payload []byte, frameErr error, latency time.Duration) {
				select {
				case frameCh <- wsFrameEvent{endpoint: addr, payload: payload, err: frameErr, latency: latency}:
				default:
				}
			},
		).withSubscriptions(subs)
	}
	processor := newProcessor(target)
	currentProc.Store(processor)

	// Pocket relay miners authenticate the WS upgrade via three HTTP headers
	// set on the initial handshake. Without these the miner treats the
	// connection as anonymous and rejects the upgrade. See PATH's
	// protocol/shannon/websocket_context.go:getRelayMinerConnectionHeaders.
	supplierHeaders := http.Header{}
	supplierHeaders.Set("Target-Service-Id", string(serviceID))
	supplierHeaders.Set("App-Address", appAddr)
	if st, ok := rpcTypeToShared[domain.RPCTypeWebSocket]; ok {
		supplierHeaders.Set("Rpc-Type", strconv.Itoa(int(st)))
	}

	// Start the bridge. After upgrade succeeds, errors surface via close codes.
	var bridgeOpts []websockets.BridgeOption
	if r.deps.Metrics != nil {
		bridgeOpts = append(bridgeOpts, websockets.WithObserver(r.deps.Metrics.ForService(serviceID)))
	}
	// Data-staleness is an endpoint loss the socket does not report: live
	// subscriptions and nothing delivered for them in wsStallTimeout while
	// pings are still answered. Handled exactly like a dead socket — the
	// rebind below — so the same replay and the same limit apply.
	bridgeOpts = append(bridgeOpts, websockets.WithStallDetector(func() bool {
		if !subs.HasActive() {
			return false
		}
		last := subs.LastActivity()
		return !last.IsZero() && time.Since(last) > r.stallTimeout
	}, r.stallCheck))

	// Endpoint loss is a rebind, not a close: pick another supplier this
	// bridge has not used, move the load counter, and replay the live
	// subscriptions through a processor that signs for the new supplier.
	tried := map[domain.EndpointAddr]bool{endpointAddr: true}
	bridgeOpts = append(bridgeOpts, websockets.WithEndpointLost(func(ctx context.Context, cause error) (*websocket.Conn, websockets.MessageProcessor, [][]byte, error) {
		lost := *current.Load()
		_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, lost, domain.RPCTypeWebSocket,
			reputation.NewMajorErrorSignal("ws_endpoint_lost:"+cause.Error(), 0))

		next, _, err := r.resolveEndpoint(ctx, serviceID, tried)
		if err != nil {
			return nil, nil, nil, err
		}
		conn, err := websockets.ConnectEndpoint(logger, next.url, supplierHeaders)
		if err != nil {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, next.addr, domain.RPCTypeWebSocket,
				reputation.NewMajorErrorSignal("ws_endpoint_unavailable:"+err.Error(), 0))
			return nil, nil, nil, fmt.Errorf("dial %s: %w", next.addr, err)
		}
		tried[next.addr] = true
		r.decLoad(lost)
		r.incLoad(next.addr)
		addr := next.addr
		current.Store(&addr)
		proc := newProcessor(next)
		currentProc.Store(proc)
		sessionEnd.Store(next.session.Header.SessionEndBlockHeight)
		logger.Info("ws rebind: endpoint replaced",
			"from", lost, "to", next.addr, "supplier", next.ep.Supplier(),
			"session_end_height", next.session.Header.SessionEndBlockHeight,
		)
		return conn, proc, subs.ReplayFrames(), nil
	}))
	bridge, err := websockets.StartBridge(ctx, logger, req, w, url, supplierHeaders, processor, bridgeOpts...)
	if err != nil {
		// Pre-upgrade error: either the client handshake failed (our fault —
		// no supplier penalty) or the endpoint dial failed (supplier
		// advertised WS but isn't actually serving it — MAJOR error).
		if errors.Is(err, websockets.ErrBridgeEndpointUnavailable) {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, endpointAddr, domain.RPCTypeWebSocket,
				reputation.NewMajorErrorSignal("ws_endpoint_unavailable:"+err.Error(), 0))
		}
		logger.Error("ws open: start bridge", "err", err)
		return fmt.Errorf("ws open: start bridge: %w", err)
	}

	r.live.Store(bridge, serviceID)
	defer r.live.Delete(bridge)

	// Watch for session expiry in a goroutine; trigger graceful close.
	safego.Go(logger, "websocket.session.expiry", func() {
		r.watchSessionExpiry(&sessionEnd, &currentProc, bridge, logger)
	})

	// Drain frame events off the bridge loop until the bridge closes.
	safego.Go(logger, "websocket.frame.drain", func() {
		r.drainFrameEvents(serviceID, frameCh, bridge.Done())
	})

	<-bridge.Done()

	r.handleBridgeClose(serviceID, *current.Load())
	logger.Info("ws open: bridge shut down",
		"active_subscriptions", len(subs.Active()),
		"untracked_subscribes", subs.Dropped(),
	)
	return nil
}

// subscriptionRegistry builds the registry for one bridge from the service's
// plugin, or an inert one when nothing can classify this chain's frames.
func (r *WSRelayer) subscriptionRegistry(serviceID domain.ServiceID) *qos.SubscriptionRegistry {
	if r.deps.QoS == nil {
		return qos.NewSubscriptionRegistry(nil)
	}
	classifier, _ := r.deps.QoS.Get(serviceID).(qos.SubscriptionClassifier)
	return qos.NewSubscriptionRegistry(classifier)
}

// watchSessionExpiry closes the bridge once its own session has ended, so the
// client reconnects onto a fresh session rather than re-signing a live socket.
//
// Each bridge watches its OWN end height against a shared atomic. There is
// deliberately no expiry broadcast: a single shared channel gives competing
// receives, where one bridge consumes an event meant for another and discards
// it. That failed two ways at once — bridges on different sessions ate each
// other's events, and bridges sharing a session (the common case: sessions are
// keyed by serviceID+appAddr) were emitted only one event between them, so all
// but one never learned. Reading the height per-bridge removes the channel and
// the registry, so neither failure has anywhere to live.
//
// Height 0 means the poller has not reported yet, and a failed poll retains the
// last good height rather than zeroing. Since 0 is below every real end height,
// a height we don't trust simply never expires anyone: if we've lost sight of
// the chain, letting the miner reject frames signed against a retired session
// beats tearing down live bridges on a guess.
//
// The goroutine exits when the bridge closes for any reason, so it cannot
// outlive its connection.
func (r *WSRelayer) watchSessionExpiry(
	sessionEnd *atomic.Int64,
	current *atomic.Pointer[wsMessageProcessor],
	bridge *websockets.Bridge,
	logger *slog.Logger,
) {
	if r.chainHeight == nil {
		return
	}

	ticker := time.NewTicker(r.expiryCheck)
	defer ticker.Stop()

	// The end height the watcher last acted on. A rebind that did not move
	// the session (a stale session cache, say) leaves it where it was, and
	// then the only honest outcome is the close — never a second rebind onto
	// the same retired session.
	var actedOn int64

	for {
		select {
		case <-bridge.Done():
			return
		case <-ticker.C:
			height := r.chainHeight()
			end := sessionEnd.Load()
			if height < end {
				continue
			}
			if end != actedOn && bridge.CanRebind() {
				actedOn = end
				logger.Info("ws session ended, rebinding onto the next session",
					"session_end_height", end, "current_height", height,
				)
				// Synchronous: back here the endpoint is swapped and
				// sessionEnd moved, or the bridge is closed. A rebind that
				// lands on the same retired session leaves end == actedOn,
				// and the next tick takes the close below.
				bridge.ReplaceEndpoint(websockets.ErrBridgeSessionExpired)
				select {
				case <-bridge.Done():
					// Nothing to rebind to: retire the processor so nothing
					// still in flight is signed against an ended session.
					if p := current.Load(); p != nil {
						p.sessionActive.Store(false)
					}
					return
				default:
				}
				continue
			}
			logger.Info("ws session ended, closing bridge so the client reconnects",
				"session_end_height", end, "current_height", height,
			)
			// Deactivate before Shutdown: stops new client frames being signed
			// against a session the chain has retired, while supplier frames
			// still in flight drain out to the client.
			if p := current.Load(); p != nil {
				p.sessionActive.Store(false)
			}
			bridge.Shutdown(websockets.ErrBridgeSessionExpired)
			return
		}
	}
}

// wsFrameEventQueueSize bounds the per-bridge frame-analysis queue. Analysis
// is advisory (reputation signals + sampled observations), so dropping under
// burst is acceptable; 256 absorbs normal subscription bursts.
const wsFrameEventQueueSize = 256

// wsFrameEvent carries one endpoint frame's analysis inputs off the bridge
// loop. endpoint is the supplier that sent it: after a rebind, frames from
// the old and the new supplier can be in the queue together.
type wsFrameEvent struct {
	endpoint domain.EndpointAddr
	payload  []byte
	err      error
	latency  time.Duration
}

// drainFrameEvents consumes frame events until the bridge closes, then drains
// whatever is still buffered and exits.
func (r *WSRelayer) drainFrameEvents(
	serviceID domain.ServiceID,
	ch <-chan wsFrameEvent,
	done <-chan struct{},
) {
	for {
		select {
		case evt := <-ch:
			r.handleEndpointFrame(serviceID, evt.endpoint, evt.payload, evt.err, evt.latency)
		case <-done:
			for {
				select {
				case evt := <-ch:
					r.handleEndpointFrame(serviceID, evt.endpoint, evt.payload, evt.err, evt.latency)
				default:
					return
				}
			}
		}
	}
}

// handleEndpointFrame runs per-frame heuristic, records a reputation signal,
// and (possibly) submits to the observation pipeline.
//
// Severity is downgraded for per-frame context: a single bad frame in a
// long-lived subscription must not fatally sink an otherwise-healthy endpoint.
func (r *WSRelayer) handleEndpointFrame(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	payload []byte,
	frameErr error,
	latency time.Duration,
) {
	// A control frame is the miner reporting a condition — a session expiry,
	// most often — not the supplier answering badly. It is the one frame that
	// is graded neither up nor down: recording a success would reward a
	// supplier for an error, and recording a failure would penalise it for a
	// session boundary it does not control. The observation still goes out,
	// forced, because a client did receive a non-2xx and that is worth seeing.
	if errors.Is(frameErr, ErrEndpointControlFrame) {
		r.submitObservation(serviceID, endpointAddr, payload, latency, frameErr, true)
		return
	}

	// If the processor handed us an error (validation failure, supplier
	// signature rejected), treat as a major supplier error without running
	// heuristic on the raw bytes.
	if frameErr != nil {
		_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, endpointAddr, domain.RPCTypeWebSocket,
			reputation.NewMajorErrorSignal("ws_validate_err:"+frameErr.Error(), latency))
		r.submitObservation(serviceID, endpointAddr, payload, latency, frameErr, true)
		return
	}

	res := heuristic.AnalyzeFrame(payload, domain.RPCTypeWebSocket)
	sig := frameSeverityToSignal(res, latency)
	_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, endpointAddr, domain.RPCTypeWebSocket, sig)

	// Always submit if heuristic penalized; otherwise sample.
	forced := res.ShouldPenalize
	if forced || rand.Float64() < r.deps.FrameObservationSampleRate {
		r.submitObservation(serviceID, endpointAddr, payload, latency, nil, forced)
	}
}

// handleBridgeClose records a close-event observation. Called exactly once
// per bridge after bridge.Done() fires.
func (r *WSRelayer) handleBridgeClose(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
) {
	if rand.Float64() >= r.deps.CloseObservationSampleRate {
		return
	}
	r.deps.Observe.Submit(observe.Observation{
		ServiceID:    serviceID,
		EndpointAddr: endpointAddr,
		Timestamp:    time.Now(),
		Source:       observe.SourceRelay,
	})
}

// submitObservation builds and submits an Observation for a single frame.
func (r *WSRelayer) submitObservation(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	payload []byte,
	latency time.Duration,
	frameErr error,
	_ bool,
) {
	obs := observe.Observation{
		ServiceID:    serviceID,
		EndpointAddr: endpointAddr,
		Timestamp:    time.Now(),
		Source:       observe.SourceRelay,
		Latency:      latency,
		ResponseBody: payload,
	}
	_ = frameErr // Observation currently has no Error field; follow-up commit can add.
	r.deps.Observe.Submit(obs)
}

// frameSeverityToSignal maps a heuristic AnalysisResult to a reputation
// signal. Per-frame severity is downgraded one step (and capped at Critical)
// so a single bad frame never sinks an endpoint to probation from healthy.
func frameSeverityToSignal(res heuristic.AnalysisResult, latency time.Duration) reputation.Signal {
	if !res.ShouldPenalize {
		return reputation.NewSuccessSignal("ws_frame_ok", latency)
	}
	reason := "ws_" + res.Reason
	switch res.PenaltySeverity {
	case heuristic.SeverityFatal:
		return reputation.NewCriticalErrorSignal(reason, latency)
	case heuristic.SeverityCritical:
		return reputation.NewMajorErrorSignal(reason, latency)
	case heuristic.SeverityMajor:
		return reputation.NewMinorErrorSignal(reason, latency)
	case heuristic.SeverityMinor:
		return reputation.NewMinorErrorSignal(reason, latency)
	default:
		return reputation.NewMinorErrorSignal(reason, latency)
	}
}

// snapshotLoad returns a point-in-time map of endpoint → active bridge count.
func (r *WSRelayer) snapshotLoad() map[domain.EndpointAddr]int {
	r.loadMu.Lock()
	defer r.loadMu.Unlock()
	result := make(map[domain.EndpointAddr]int, len(r.activeLoad))
	for ep, n := range r.activeLoad {
		if n > 0 {
			result[ep] = n
		}
	}
	return result
}

func (r *WSRelayer) incLoad(ep domain.EndpointAddr) {
	r.loadMu.Lock()
	defer r.loadMu.Unlock()
	if r.activeLoad == nil {
		r.activeLoad = make(map[domain.EndpointAddr]int)
	}
	r.activeLoad[ep]++
}

func (r *WSRelayer) decLoad(ep domain.EndpointAddr) {
	r.loadMu.Lock()
	defer r.loadMu.Unlock()
	n, ok := r.activeLoad[ep]
	if !ok {
		return
	}
	if n <= 1 {
		// Zero carries no information, and the key is a supplier address that
		// will not be seen again after this session.
		delete(r.activeLoad, ep)
		return
	}
	r.activeLoad[ep] = n - 1
}

// wsTarget is one resolved supplier: everything Open or a rebind needs to
// dial it and sign for it.
type wsTarget struct {
	addr    domain.EndpointAddr
	ep      *endpoint
	url     string
	session *sessiontypes.Session
	appAddr string
	app     *apptypes.Application
}

// resolveEndpoint picks a WebSocket endpoint for serviceID and resolves its
// session, URL and signing app. tried names endpoints this connection has
// already used; they are avoided (operator-aware, like retry) and only fall
// back in when nothing else advertises WebSocket — a blip on the only host
// is still worth one reconnect. The second return is the message for the
// HTTP error Open sends when this fails before the upgrade.
func (r *WSRelayer) resolveEndpoint(ctx context.Context, serviceID domain.ServiceID, tried map[domain.EndpointAddr]bool) (*wsTarget, string, error) {
	endpoints, err := r.deps.Protocol.AvailableEndpoints(ctx, serviceID, domain.RPCTypeWebSocket)
	if err != nil {
		return nil, "no websocket endpoints available", fmt.Errorf("available endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return nil, "no websocket endpoints available", errors.New("no endpoints for rpc type websocket")
	}
	candidates := untriedFirst(endpoints, tried, r.deps.Flags.IsEnabled(ctx, featureflag.FlagOperatorAwareSelection, serviceID))
	load := r.snapshotLoad()
	addr := r.deps.Reputation.SelectSpread(ctx, serviceID, candidates, domain.RPCTypeWebSocket, load)
	if addr == "" {
		return nil, "no viable websocket endpoint", errors.New("empty selection")
	}

	appAddr, err := r.deps.Protocol.pickApp(serviceID)
	if err != nil {
		return nil, "no app configured", fmt.Errorf("pick app: %w", err)
	}
	session, err := r.deps.Protocol.sessions.getSession(ctx, string(serviceID), appAddr)
	if err != nil {
		return nil, "session unavailable", fmt.Errorf("session: %w", err)
	}
	ep, ok := r.deps.Protocol.sessions.getOrCreateEndpoints(session)[addr]
	if !ok {
		return nil, "endpoint resolution failed", fmt.Errorf("endpoint %q missing from session %s", addr, session.SessionId)
	}
	url, err := ep.GetURL(domain.RPCTypeWebSocket)
	if err != nil {
		return nil, "endpoint does not support websocket", fmt.Errorf("ws url for %s: %w", addr, err)
	}
	app, err := r.deps.Protocol.getApp(ctx, appAddr)
	if err != nil {
		return nil, "app unavailable", fmt.Errorf("fetch app: %w", err)
	}
	return &wsTarget{addr: addr, ep: ep, url: url, session: session, appAddr: appAddr, app: app}, "", nil
}

// untriedFirst narrows endpoints to the ones not in tried, preferring
// operators not in tried when operatorAware; each narrowing is a
// preference, dropped when it would leave nothing.
func untriedFirst(endpoints domain.EndpointAddrList, tried map[domain.EndpointAddr]bool, operatorAware bool) domain.EndpointAddrList {
	if len(tried) == 0 {
		return endpoints
	}
	untried := endpoints.Exclude(tried)
	if len(untried) == 0 {
		return endpoints
	}
	if !operatorAware {
		return untried
	}
	operators := make(map[string]bool, len(tried))
	for ep := range tried {
		operators[ep.Operator()] = true
	}
	return untried.ExcludeOperators(operators)
}

// RebindService asks every live bridge for serviceID to replace its supplier,
// as if the supplier had been lost: a new one is selected (avoiding the ones
// each connection has used), the live subscriptions are replayed, and the
// client sees nothing. It returns how many bridges were asked. This is the
// admin rebind route — a drill, or the way to move live connections off an
// operator that was just drained, which selection alone never touches.
func (r *WSRelayer) RebindService(serviceID domain.ServiceID) int {
	n := 0
	r.live.Range(func(key, value any) bool {
		if value.(domain.ServiceID) != serviceID {
			return true
		}
		key.(*websockets.Bridge).ReplaceEndpoint(websockets.ErrBridgeReplaceRequested)
		n++
		return true
	})
	return n
}
