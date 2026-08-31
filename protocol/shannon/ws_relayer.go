package shannon

import (
	"context"
	"errors"
	"fmt"
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
	activeLoad sync.Map // domain.EndpointAddr → *atomic.Int64

	// chainHeight reads the current chain head. A field so tests can drive
	// the height without a live block poller.
	chainHeight func() int64

	// expiryCheck is each bridge's expiry tick. A field so tests need not
	// wait seconds.
	expiryCheck time.Duration

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
		deps:        deps,
		chainHeight: deps.Protocol.LatestBlockHeight,
		expiryCheck: wsExpiryCheckInterval,
		connLimiter: websockets.NewConnectionLimiter(deps.MaxConcurrentConnections),
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

	// Pick an endpoint: tier cascade + load-aware weighted random.
	endpoints, err := r.deps.Protocol.AvailableEndpoints(ctx, serviceID, domain.RPCTypeWebSocket)
	if err != nil {
		logger.Error("ws open: list endpoints", "err", err)
		http.Error(w, "no websocket endpoints available", http.StatusBadGateway)
		return fmt.Errorf("ws open: available endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		logger.Warn("ws open: no endpoints advertise websocket")
		http.Error(w, "no websocket endpoints available", http.StatusBadGateway)
		return errors.New("ws open: no endpoints for rpc type websocket")
	}
	load := r.snapshotLoad()
	endpointAddr := r.deps.Reputation.SelectSpread(ctx, serviceID, endpoints, domain.RPCTypeWebSocket, load)
	if endpointAddr == "" {
		logger.Warn("ws open: selection returned empty")
		http.Error(w, "no viable websocket endpoint", http.StatusBadGateway)
		return errors.New("ws open: empty selection")
	}

	// Resolve supplier URL and app via the protocol layer.
	appAddr, err := r.deps.Protocol.pickApp(serviceID)
	if err != nil {
		logger.Error("ws open: pick app", "err", err)
		http.Error(w, "no app configured", http.StatusBadGateway)
		return fmt.Errorf("ws open: pick app: %w", err)
	}
	session, err := r.deps.Protocol.sessions.getSession(ctx, string(serviceID), appAddr)
	if err != nil {
		logger.Error("ws open: get session", "err", err)
		http.Error(w, "session unavailable", http.StatusBadGateway)
		return fmt.Errorf("ws open: session: %w", err)
	}
	sessionEndpoints := r.deps.Protocol.sessions.getOrCreateEndpoints(session)
	ep, ok := sessionEndpoints[endpointAddr]
	if !ok {
		logger.Error("ws open: selected endpoint missing from session",
			"endpoint", endpointAddr, "session_id", session.SessionId,
		)
		http.Error(w, "endpoint resolution failed", http.StatusBadGateway)
		return fmt.Errorf("ws open: endpoint %q missing from session", endpointAddr)
	}
	url, err := ep.GetURL(domain.RPCTypeWebSocket)
	if err != nil {
		logger.Error("ws open: endpoint has no ws url", "err", err, "endpoint", endpointAddr)
		http.Error(w, "endpoint does not support websocket", http.StatusBadGateway)
		return fmt.Errorf("ws open: ws url: %w", err)
	}
	app, err := r.deps.Protocol.getApp(ctx, appAddr)
	if err != nil {
		logger.Error("ws open: fetch app for signing", "err", err)
		http.Error(w, "app unavailable", http.StatusBadGateway)
		return fmt.Errorf("ws open: fetch app: %w", err)
	}

	// Increment load counter; guarantee decrement on return.
	r.incLoad(endpointAddr)
	defer r.decLoad(endpointAddr)

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
	processor := newWSMessageProcessor(
		ctx,
		r.deps.Protocol,
		session.Header,
		ep.Supplier(),
		ep.Addr(),
		app,
		func(payload []byte, frameErr error, latency time.Duration) {
			select {
			case frameCh <- wsFrameEvent{payload: payload, err: frameErr, latency: latency}:
			default:
			}
		},
	).withSubscriptions(subs)

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

	// Watch for session expiry in a goroutine; trigger graceful close.
	safego.Go(logger, "websocket.session.expiry", func() {
		r.watchSessionExpiry(session.Header.SessionEndBlockHeight, processor, bridge, logger)
	})

	// Drain frame events off the bridge loop until the bridge closes.
	safego.Go(logger, "websocket.frame.drain", func() {
		r.drainFrameEvents(serviceID, endpointAddr, frameCh, bridge.Done())
	})

	<-bridge.Done()

	r.handleBridgeClose(serviceID, endpointAddr)
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
	endHeight int64,
	processor *wsMessageProcessor,
	bridge *websockets.Bridge,
	logger *slog.Logger,
) {
	if r.chainHeight == nil {
		return
	}

	ticker := time.NewTicker(r.expiryCheck)
	defer ticker.Stop()

	for {
		select {
		case <-bridge.Done():
			return
		case <-ticker.C:
			height := r.chainHeight()
			if height < endHeight {
				continue
			}
			logger.Info("ws session ended, closing bridge so the client reconnects",
				"session_end_height", endHeight, "current_height", height,
			)
			// Deactivate before Shutdown: stops new client frames being signed
			// against a session the chain has retired, while supplier frames
			// still in flight drain out to the client.
			processor.sessionActive.Store(false)
			bridge.Shutdown(websockets.ErrBridgeSessionExpired)
			return
		}
	}
}

// wsFrameEventQueueSize bounds the per-bridge frame-analysis queue. Analysis
// is advisory (reputation signals + sampled observations), so dropping under
// burst is acceptable; 256 absorbs normal subscription bursts.
const wsFrameEventQueueSize = 256

// wsFrameEvent carries one endpoint frame's analysis inputs off the bridge loop.
type wsFrameEvent struct {
	payload []byte
	err     error
	latency time.Duration
}

// drainFrameEvents consumes frame events until the bridge closes, then drains
// whatever is still buffered and exits.
func (r *WSRelayer) drainFrameEvents(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	ch <-chan wsFrameEvent,
	done <-chan struct{},
) {
	for {
		select {
		case evt := <-ch:
			r.handleEndpointFrame(serviceID, endpointAddr, evt.payload, evt.err, evt.latency)
		case <-done:
			for {
				select {
				case evt := <-ch:
					r.handleEndpointFrame(serviceID, endpointAddr, evt.payload, evt.err, evt.latency)
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
	result := make(map[domain.EndpointAddr]int)
	r.activeLoad.Range(func(key, value any) bool {
		ep, _ := key.(domain.EndpointAddr)
		counter, _ := value.(*atomic.Int64)
		if counter != nil {
			if n := int(counter.Load()); n > 0 {
				result[ep] = n
			}
		}
		return true
	})
	return result
}

func (r *WSRelayer) incLoad(ep domain.EndpointAddr) {
	v, _ := r.activeLoad.LoadOrStore(ep, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (r *WSRelayer) decLoad(ep domain.EndpointAddr) {
	v, ok := r.activeLoad.Load(ep)
	if !ok {
		return
	}
	v.(*atomic.Int64).Add(-1)
}
