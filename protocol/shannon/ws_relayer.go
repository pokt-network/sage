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
	"net"
	"net/http"
	"net/netip"
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
	// wsStallTimeout is how long a periodic subscription (newHeads,
	// slotSubscribe, NewBlock — one delivery per block whatever the client
	// filters) may go without one before its supplier is replaced. A minute:
	// longer than any chain's block time by a wide margin. Only periodic
	// feeds are judged (qos.SubscriptionRegistry.Heartbeat): a logs filter
	// that matches nothing for minutes is a quiet feed, and judging it used
	// to cost an honest supplier a major penalty and the client a rebind
	// every minute, then a 1012 when the rebinds ran out.
	// wsStallCheckInterval is the poll.
	wsStallTimeout       = 60 * time.Second
	wsStallCheckInterval = 5 * time.Second

	// wsSuccessSignalInterval is the least time between two success signals
	// a bridge records for its supplier. A subscription frame is not a
	// request: grading each one as a success let a supplier's score follow
	// its feed's chattiness, so a chatty feed erased any penalty within two
	// frames and a supplier could buy immunity from demotion by pushing more.
	// One success per interval grades what reputation is for — the
	// connection kept working — and failures are still recorded every time.
	wsSuccessSignalInterval = 30 * time.Second

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

	// RequestTimeout is the service's relay timeout: a request a supplier
	// holds longer with no answer moves the connection (ws_no_answer). Nil or
	// zero waits for ever, as before.
	RequestTimeout func(domain.ServiceID) time.Duration

	// QoS resolves the service's plugin. A plugin that implements
	// qos.SubscriptionClassifier gives the bridge a subscription registry —
	// the knowledge a rebind and a stall watchdog need. Optional: nil, or a
	// plugin without the interface, means no tracking.
	QoS *qos.Registry

	// ClientIP attributes an upgrade request to a client, the same way the
	// HTTP chain's client_ip middleware does (trusted-proxy aware). Optional:
	// nil takes the direct peer, which behind a proxy is the proxy.
	ClientIP func(*http.Request) netip.Addr
}

// WSMetrics is what the relayer needs from the metrics package: a per-service
// observer for each bridge, a counter for the upgrades it refuses before a
// bridge exists, and per-supplier accounting — which supplier served which
// connection for how long, and what it pushed. metrics.WebSocketMetrics
// satisfies it.
type WSMetrics interface {
	ForService(serviceID domain.ServiceID) websockets.Observer
	Rejected(serviceID domain.ServiceID, reason string)
	SupplierBound(serviceID domain.ServiceID, operator, owner string)
	SupplierReleased(serviceID domain.ServiceID, operator, owner string, tenure time.Duration)
	SupplierFrame(serviceID domain.ServiceID, operator, owner string, source websockets.MessageSource)
	SupplierNotification(serviceID domain.ServiceID, operator, owner string, note qos.Notification)
	SupplierHead(serviceID domain.ServiceID, operator, owner string, lagBlocks uint64, delay time.Duration, delayKnown bool)
	SupplierHeadMismatch(serviceID domain.ServiceID, operator, owner string)
	Probed(serviceID domain.ServiceID, result string)
	ShareCap(serviceID domain.ServiceID, outcome string)
	SessionEndAction(serviceID domain.ServiceID, action string, blocksPast int64)
	SupplierReissued(serviceID domain.ServiceID, operator, owner, reason string, n int)
	SupplierRetyped(serviceID domain.ServiceID, operator, owner string)
	SupplierAnswer(serviceID domain.ServiceID, operator string, took time.Duration)
	OpenPhase(serviceID domain.ServiceID, phase string, took time.Duration)
}

// WSRelayer is the only public entry point for opening WebSocket bridges in
// SAGE. It is responsible for selecting a supplier with load-aware spread,
// constructing the Shannon MessageProcessor that signs every outbound frame,
// and hooking each supplier frame into reputation + heuristic + observation.
type WSRelayer struct {
	deps WSRelayerDeps

	// samples keeps a sample of what each supplier pushed (ws_samples.go).
	samples *wsNotificationSamples
	// heads compares the operators' newHeads feeds (ws_heads.go).
	heads *wsHeadTracker

	// probeBackoff spaces out recovery probes of URLs that keep failing.
	probeBackoff wsProbeBackoff

	// noAnswers is when each endpoint last left a request unanswered
	// (noAnswerSeverity).
	noAnswerMu sync.Mutex
	noAnswers  map[string]time.Time

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
	live sync.Map // *websockets.Bridge → *wsLive

	// clients records which supplier served each client connection for how
	// long, for the admin clients route. See ws_clients.go.
	clients *wsClientLedger

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
		clients:      newWSClientLedger(nil),
		samples:      newWSNotificationSamples(),
		heads:        newWSHeadTracker(),
	}
}

// consensusHead reads the service's block consensus head, for the head
// signals; nil when the service's plugin tracks none.
func (r *WSRelayer) consensusHead(serviceID domain.ServiceID) func() uint64 {
	if r.deps.QoS == nil {
		return nil
	}
	if bt, ok := r.deps.QoS.Get(serviceID).(qos.BlockHeightTracker); ok {
		return bt.PerceivedBlockHeight
	}
	return nil
}

// NotificationSamples returns the sampled WebSocket notification hashes (see
// ws_samples.go), for one service or, with serviceID "", every service. It is
// the admin notification-samples route.
func (r *WSRelayer) NotificationSamples(serviceID domain.ServiceID) []WSNotificationSample {
	return r.samples.snapshot(serviceID)
}

// Clients reports, per client address, which suppliers served its WebSocket
// connections over the last one to two hours, busiest first. serviceID ""
// covers every service; onlyShopping keeps only clients flagged as shopping.
// It is the admin clients route.
func (r *WSRelayer) Clients(serviceID domain.ServiceID, limit int, onlyShopping bool) WSClientsSnapshot {
	return r.clients.snapshot(serviceID, limit, onlyShopping)
}

// ShoppingClients counts clients flagged as shopping for a supplier (see
// ws_clients.go), for the sage_websocket_shopping_clients gauge.
func (r *WSRelayer) ShoppingClients() int {
	return r.clients.shoppingClients()
}

// clientIP resolves the address an upgrade is attributed to.
func (r *WSRelayer) clientIP(req *http.Request) string {
	if r.deps.ClientIP != nil {
		if a := r.deps.ClientIP(req); a.IsValid() {
			return a.String()
		}
		return ""
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return req.RemoteAddr
	}
	return host
}

// bindSupplier starts the accounting for a supplier taking over a client
// connection.
func (r *WSRelayer) bindSupplier(serviceID domain.ServiceID, p *wsMessageProcessor) {
	if r.deps.Metrics != nil {
		r.deps.Metrics.SupplierBound(serviceID, p.operator, p.owner)
	}
}

// releaseSupplier ends it: the tenure, its frames, and whether the client
// was the one who ended it.
func (r *WSRelayer) releaseSupplier(serviceID domain.ServiceID, clientIP string, p *wsMessageProcessor, clientQuit bool) {
	tenure := time.Since(p.boundAt)
	if r.deps.Metrics != nil {
		r.deps.Metrics.SupplierReleased(serviceID, p.operator, p.owner, tenure)
	}
	r.clients.tenure(clientIP,
		wsSupplierKey{service: serviceID, operator: p.operator, owner: p.owner},
		tenure, p.endpointFrames.Load(), p.topicCounts(), clientQuit)
}

// SetMaxConcurrentConnections moves the live-bridge cap on a running relayer.
// <= 0 removes it. Bridges already open above a lowered cap are not closed.
func (r *WSRelayer) SetMaxConcurrentConnections(n int) {
	r.connLimiter.SetMax(n)
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
	if !r.deps.Flags.IsEnabled(ctx, featureflag.FlagWebsocketRelays, serviceID) {
		logger.Info("ws open: feature flag off")
		http.Error(w, "websocket relays disabled for this service", http.StatusServiceUnavailable)
		return fmt.Errorf("ws open: %s flag disabled for %q", featureflag.FlagWebsocketRelays, serviceID)
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
	resolveStart := time.Now()
	target, httpMsg, err := r.resolveEndpoint(ctx, serviceID, nil, nil)
	r.openPhase(serviceID, "resolve", resolveStart)
	if err != nil {
		logger.Error("ws open: resolve endpoint", "err", err)
		http.Error(w, httpMsg, http.StatusBadGateway)
		return fmt.Errorf("ws open: %w", err)
	}
	endpointAddr, ep, url, session, appAddr := target.addr, target.ep, target.url, target.session, target.appAddr
	clientIP := r.clientIP(req)

	// Increment load counter; guarantee decrement on return — for whichever
	// endpoint is current by then, since a rebind moves it.
	var current atomic.Pointer[domain.EndpointAddr]
	current.Store(&endpointAddr)
	r.incLoad(endpointAddr)

	// swapMu orders a rebind's swap of the current endpoint and processor
	// against the bridge's close. They race when the client leaves while a
	// rebind is dialling: the close released the old supplier, then the
	// rebind released it again and bound a new one nobody would release.
	// On mainnet (2026-09-28) that left sage_websocket_supplier_connections
	// negative for one owner and inflated for the others. Once closed, a
	// rebind drops its new connection instead of swapping.
	var swapMu sync.Mutex
	closed := false
	defer func() {
		swapMu.Lock()
		defer swapMu.Unlock()
		r.decLoad(*current.Load())
	}()

	// The session the bridge is signing under moves with a rebind; the
	// expiry watcher follows it, so a rollover becomes a rebind onto the
	// next session rather than a close.
	var sessionEnd atomic.Int64
	sessionEnd.Store(session.Header.SessionEndBlockHeight)
	var currentProc atomic.Pointer[wsMessageProcessor]
	// The bridge as the share cap sees it; registered once the bridge is up.
	live := &wsLive{service: serviceID, current: &current, proc: &currentProc, opened: time.Now()}

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
	consensusHead := r.consensusHead(serviceID)
	// The bridge, once up: a processor asks it whether a rate-limited
	// request can be reissued after a rebind.
	var bridgeRef atomic.Pointer[websockets.Bridge]
	newProcessor := func(t *wsTarget) *wsMessageProcessor {
		addr := t.addr
		p := newWSMessageProcessor(
			ctx,
			r.deps.Protocol,
			t.session.Header,
			t.ep.Supplier(),
			t.ep.Addr(),
			t.app,
			r.frameSink(serviceID, addr, frameCh),
		)
		p.subs, p.samples = subs, r.samples
		p.staleness = r.staleness(serviceID, addr)
		p.canRebind = func() bool {
			b := bridgeRef.Load()
			return b != nil && b.CanRebind()
		}
		p.heads, p.consensusHead = r.heads, consensusHead
		p.metrics, p.owner, p.operator, p.boundAt = r.deps.Metrics, t.ep.Owner(), p.endpointAddr.Operator(), time.Now()
		return p
	}
	processor := newProcessor(target)
	currentProc.Store(processor)

	supplierHeaders := relayMinerHeaders(serviceID, appAddr)

	// Start the bridge. After upgrade succeeds, errors surface via close codes.
	var bridgeOpts []websockets.BridgeOption
	if r.deps.Metrics != nil {
		bridgeOpts = append(bridgeOpts, websockets.WithObserver(r.deps.Metrics.ForService(serviceID)))
	}
	// Data-staleness is an endpoint loss the socket does not report: live
	// subscriptions and nothing delivered for them in wsStallTimeout while
	// pings are still answered. Handled exactly like a dead socket — the
	// rebind below — so the same replay and the same limit apply.
	//
	// So is a request the supplier has held past the service's relay
	// timeout with no answer (noAnswer tells the rebind which it was): a
	// supplier that takes requests and stays silent held its clients
	// forever, ungraded. Only while a rebind is left: past the limit the
	// rebind would close the connection, and a slow answer is better than
	// none.
	var noAnswer atomic.Bool
	bridgeOpts = append(bridgeOpts, websockets.WithStallDetector(func() bool {
		if stalled(subs, r.stallTimeout) {
			return true
		}
		if r.deps.RequestTimeout == nil {
			return false
		}
		timeout := r.deps.RequestTimeout(serviceID)
		oldest := subs.OldestInFlight()
		if timeout <= 0 || oldest.IsZero() || time.Since(oldest) <= timeout {
			return false
		}
		if b := bridgeRef.Load(); b == nil || !b.CanRebind() {
			return false
		}
		noAnswer.Store(true)
		return true
	}, r.stallCheck))

	// Endpoint loss is a rebind, not a close: pick another supplier, move
	// the load counter, and replay the live subscriptions through a
	// processor that signs for the new supplier. tried holds the endpoints
	// this connection lost to a failure since its last planned rebind
	// (noteRebind), avoided within the best tier.
	tried := map[domain.EndpointAddr]bool{}
	bridgeOpts = append(bridgeOpts, websockets.WithEndpointLost(func(ctx context.Context, cause error) (*websocket.Conn, websockets.MessageProcessor, [][]byte, error) {
		lost := *current.Load()
		// A miner closing because the supplier's allocation for the session
		// is spent (the HA miner's 4002, the poktroll miner's wording): it is
		// out for the rest of the session, here and on HTTP (overServed).
		if v, ok := heuristic.MinerRefusal(cause); ok && v.Reason == heuristic.ReasonOverServiced {
			r.deps.Protocol.markOverServed(serviceID, lost.Supplier(), sessionEnd.Load())
		}
		lostReason := "lost"
		switch {
		case errors.Is(cause, websockets.ErrBridgeStalled) && noAnswer.Swap(false):
			lostReason = "no_answer"
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, lost, domain.RPCTypeWebSocket,
				reputation.NewSignal(r.noAnswerSeverity(serviceID, lost), "ws_no_answer", 0))
		case lossIsSuppliers(cause):
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, lost, domain.RPCTypeWebSocket,
				reputation.NewSignal(reputation.SignalMajorError, "ws_endpoint_lost:"+cause.Error(), 0))
		}
		noteRebind(tried, lost, cause)

		next, _, err := r.resolveEndpoint(ctx, serviceID, tried, live)
		if err != nil {
			return nil, nil, nil, err
		}
		dialStart := time.Now()
		conn, err := websockets.ConnectEndpoint(logger, next.url, supplierHeaders)
		r.openPhase(serviceID, "rebind_dial", dialStart)
		if err != nil {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, next.addr, domain.RPCTypeWebSocket,
				reputation.NewSignal(reputation.SignalMajorError, "ws_endpoint_unavailable:"+err.Error(), 0))
			return nil, nil, nil, fmt.Errorf("dial %s: %w", next.addr, err)
		}
		swapMu.Lock()
		defer swapMu.Unlock()
		if closed {
			_ = conn.Close()
			return nil, nil, nil, errors.New("bridge closed during rebind")
		}
		r.decLoad(lost)
		r.incLoad(next.addr)
		addr := next.addr
		current.Store(&addr)
		old := currentProc.Load()
		r.releaseSupplier(serviceID, clientIP, old, false)
		live.retired.Add(old.endpointFrames.Load())
		proc := newProcessor(next)
		r.bindSupplier(serviceID, proc)
		currentProc.Store(proc)
		sessionEnd.Store(next.session.Header.SessionEndBlockHeight)
		logger.Info("ws rebind: endpoint replaced",
			"from", lost, "to", next.addr, "supplier", next.ep.Supplier(),
			"session_end_height", next.session.Header.SessionEndBlockHeight,
		)
		// What the old supplier owed goes to the new one; a write it may
		// have applied is answered here instead, so no client waits on it.
		replay := subs.Replay()
		if b := bridgeRef.Load(); b != nil {
			for _, req := range replay.Abandoned {
				_ = b.SendToClient(lostWriteAnswer(req))
			}
		}
		if r.deps.Metrics != nil && replay.Lost > 0 {
			r.deps.Metrics.SupplierReissued(serviceID, old.operator, old.owner, lostReason, replay.Lost)
		}
		return conn, proc, replay.Frames, nil
	}))
	// StartBridge starts the endpoint read loop before it returns, so a loss
	// can reach the rebind handler — which releases the current supplier
	// under swapMu — before the first supplier is bound. Holding swapMu
	// across the start and the bind makes that release wait for the bind.
	swapMu.Lock()
	dialStart := time.Now()
	bridge, err := websockets.StartBridge(ctx, logger, req, w, url, supplierHeaders, processor, bridgeOpts...)
	r.openPhase(serviceID, "dial", dialStart)
	if err == nil {
		bridgeRef.Store(bridge)
		r.clients.opened(clientIP)
		r.bindSupplier(serviceID, processor)
	}
	swapMu.Unlock()
	if err != nil {
		// Pre-upgrade error: either the client handshake failed (our fault —
		// no supplier penalty) or the endpoint dial failed (supplier
		// advertised WS but isn't actually serving it — MAJOR error).
		if errors.Is(err, websockets.ErrBridgeEndpointUnavailable) {
			_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, endpointAddr, domain.RPCTypeWebSocket,
				reputation.NewSignal(reputation.SignalMajorError, "ws_endpoint_unavailable:"+err.Error(), 0))
		}
		logger.Error("ws open: start bridge", "err", err)
		return fmt.Errorf("ws open: start bridge: %w", err)
	}

	r.live.Store(bridge, live)
	defer r.live.Delete(bridge)

	// Watch for session expiry in a goroutine; trigger graceful close.
	safego.Go(logger, "websocket.session.expiry", func() {
		r.watchSessionExpiry(serviceID, &sessionEnd, &currentProc, bridge, logger, r.nextSession(serviceID))
	})

	// Drain frame events off the bridge loop until the bridge closes.
	safego.Go(logger, "websocket.frame.drain", func() {
		r.drainFrameEvents(serviceID, frameCh, bridge.Done(), func() int {
			p := currentProc.Load()
			if p == nil || !r.deps.Flags.IsEnabled(context.Background(), featureflag.FlagWSRateCountsAnswers, serviceID) {
				return 0
			}
			return int(p.answered.Swap(0))
		})
	})

	<-bridge.Done()

	swapMu.Lock()
	closed = true
	r.releaseSupplier(serviceID, clientIP, currentProc.Load(), bridge.ClosedBy() == websockets.InitiatorClient)
	swapMu.Unlock()
	r.handleBridgeClose(serviceID, *current.Load())
	logger.Info("ws open: bridge shut down",
		"active_subscriptions", len(subs.Active()),
		"untracked_subscribes", subs.Dropped(),
	)
	return nil
}

// lossIsSuppliers reports whether a rebind's cause is the supplier failing. A
// session that ended, or an operator's rebind request, is not: both used to
// be recorded as a major error against the supplier left, at every session
// end, for serving its session to the last block. Healing kept the scores up
// (mainnet 2026-09-29: WebSocket keys scored no lower than JSON-RPC ones), so
// it showed as noise in the signal rather than as a floor.
//
// Nor is a relay miner's own refusal that the HTTP path scores as nothing:
// a session it no longer serves, or an allocation that is spent
// (heuristic.MinerRefusal, the table every path reads).
// wsDrainRebindCooldown spaces the moves off a drained supplier on one
// connection: if the rebind found nowhere but another drained one, it is not
// tried again every few seconds.
const wsDrainRebindCooldown = 5 * time.Minute

// moveOffDrained rebinds a connection whose supplier a drain now covers, as a
// planned move (ws_drain_rebind), and reports whether it did. A drain changed
// only who new connections drew; on mainnet robinhood (2026-10-04) thirteen
// connections stayed on a drained, stale supplier until their sessions
// ended.
func (r *WSRelayer) moveOffDrained(serviceID domain.ServiceID, current *atomic.Pointer[wsMessageProcessor], bridge *websockets.Bridge, movedAt *time.Time) bool {
	p := current.Load()
	if p == nil || !bridge.Rebindable() || time.Since(*movedAt) < wsDrainRebindCooldown ||
		!r.drainedWS(serviceID, p.endpointAddr) ||
		!r.deps.Flags.IsEnabled(context.Background(), featureflag.FlagWSDrainRebind, serviceID) {
		return false
	}
	*movedAt = time.Now()
	bridge.ReplaceEndpoint(websockets.ErrBridgeReplaceRequested)
	return true
}

// drainedWS reports whether a drain covers ep's WebSocket face on serviceID,
// by the URL it serves WebSocket on, as AvailableEndpoints judges it.
func (r *WSRelayer) drainedWS(serviceID domain.ServiceID, ep domain.EndpointAddr) bool {
	proto := r.deps.Protocol
	if proto == nil || proto.drains == nil || proto.sessions == nil {
		return false
	}
	e, ok := proto.sessions.lookupEndpoint(serviceID, ep)
	if !ok {
		return false
	}
	url, err := e.GetURL(domain.RPCTypeWebSocket)
	if err != nil {
		return false
	}
	return proto.drains.Drained(serviceID, operatorOf(url), domain.RPCTypeWebSocket)
}

// wsNoAnswerWindow is how long a ws_no_answer is remembered against its
// endpoint: a second inside it is a supplier that goes silent, not a one-off.
const wsNoAnswerWindow = 10 * time.Minute

// noAnswerSeverity grades one ws_no_answer against ep: minor the first time
// in wsNoAnswerWindow on this pod, major after (ws_no_answer_minor_first);
// major every time with the flag off.
func (r *WSRelayer) noAnswerSeverity(serviceID domain.ServiceID, ep domain.EndpointAddr) reputation.SignalType {
	now := time.Now()
	key := string(serviceID) + "|" + string(ep)
	r.noAnswerMu.Lock()
	defer r.noAnswerMu.Unlock()
	if r.noAnswers == nil {
		r.noAnswers = make(map[string]time.Time)
	}
	prev, seen := r.noAnswers[key]
	r.noAnswers[key] = now
	// ponytail: pruned only when large; endpoints rotate with sessions, so
	// the map holds at most a few windows' worth of silent ones.
	if len(r.noAnswers) > 4096 {
		for k, at := range r.noAnswers {
			if now.Sub(at) > wsNoAnswerWindow {
				delete(r.noAnswers, k)
			}
		}
	}
	if !r.deps.Flags.IsEnabled(context.Background(), featureflag.FlagWSNoAnswerMinorFirst, serviceID) ||
		(seen && now.Sub(prev) < wsNoAnswerWindow) {
		return reputation.SignalMajorError
	}
	return reputation.SignalMinorError
}

// openPhase records how long one phase of opening or rebinding a connection
// took: resolve (choosing the supplier and its session), dial (the client
// upgrade and the supplier's WebSocket handshake, before which a client's
// first request waits) or rebind_dial.
func (r *WSRelayer) openPhase(serviceID domain.ServiceID, phase string, start time.Time) {
	if r.deps.Metrics != nil {
		r.deps.Metrics.OpenPhase(serviceID, phase, time.Since(start))
	}
}

// lostWriteAnswer is the client's answer to a write request in flight when
// its supplier was lost: an error, not a retry, since the supplier may have
// applied it.
func lostWriteAnswer(request []byte) []byte {
	id := qos.JSONRPCRequestID(request)
	if id == "" {
		id = "null"
	}
	return []byte(`{"jsonrpc":"2.0","id":` + id + `,"error":{"code":-32603,"message":"upstream connection lost before the answer; the request may or may not have been applied"}}`)
}

func lossIsSuppliers(cause error) bool {
	if errors.Is(cause, websockets.ErrBridgeSessionExpired) || errors.Is(cause, websockets.ErrBridgeReplaceRequested) {
		return false
	}
	if v, ok := heuristic.MinerRefusal(cause); ok {
		return v.ShouldPenalize
	}
	// Any other frame that failed processing (a response that failed
	// verification) was graded when it arrived (handleEndpointFrame); the
	// loss it causes is the same fault, not a second one.
	return !errors.Is(cause, websockets.ErrBridgeMessageProcessing)
}

// stalled reports whether a connection's periodic feed has gone silent for
// longer than timeout. A connection with no periodic subscription is never
// stalled; see wsStallTimeout.
func stalled(subs *qos.SubscriptionRegistry, timeout time.Duration) bool {
	periodic, last := subs.Heartbeat()
	return periodic && !last.IsZero() && time.Since(last) > timeout
}

// relayMinerHeaders are the three headers a Pocket relay miner authenticates
// a WebSocket upgrade by. Without them the miner treats the connection as
// anonymous and rejects it. See PATH's
// protocol/shannon/websocket_context.go:getRelayMinerConnectionHeaders.
func relayMinerHeaders(serviceID domain.ServiceID, appAddr string) http.Header {
	h := http.Header{}
	h.Set("Target-Service-Id", string(serviceID))
	h.Set("App-Address", appAddr)
	if st, ok := rpcTypeToShared[domain.RPCTypeWebSocket]; ok {
		h.Set("Rpc-Type", strconv.Itoa(int(st)))
	}
	return h
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

// sessionEndActionKind is what a bridge's expiry watcher does on one tick.
type sessionEndActionKind int

const (
	sessionWait sessionEndActionKind = iota
	sessionRebind
	sessionClose
)

// sessionEndAction decides one watcher tick for a bridge whose session ends
// at end. A rebind needs the next session: through the end block, and through
// the grace period until a background refresh lands, the session manager still
// hands out the ended one, and a rebind taken then lands on it and closes the
// bridge on the next tick. On mainnet (2026-09-27) that closed about half of
// all WebSocket connections at every session boundary with 1012 — every
// bridge of a service at once, as they share one app's session — which is why
// SAGE's connections lived less than half as long as PATH's. So wait while
// the ended session is still honoured; past the grace period the manager
// refreshes synchronously, and the rebind is taken regardless. A connection
// that spent its loss budget still rolls over: the budget stops losses, not
// planned moves (websockets.Bridge.rebind). Only a bridge that cannot rebind
// at all (rebindable false) is closed.
func sessionEndAction(height, end, actedOn, graceEnd int64, rebindable, nextReady bool) sessionEndActionKind {
	switch {
	case height < end:
		return sessionWait
	case end == actedOn || !rebindable:
		return sessionClose
	case nextReady || height > graceEnd:
		return sessionRebind
	default:
		return sessionWait
	}
}

// sessionEndLabel names a non-wait action for the session-end metric by what
// forced it. Past grace a rebind is taken whatever the session manager holds,
// so it is grace_elapsed even when the lookup said ready (a failed lookup
// reports ready, and that case is the one signing against a retired session).
func sessionEndLabel(action sessionEndActionKind, nextReady, pastGrace bool) string {
	switch {
	case action == sessionClose:
		return "close"
	case pastGrace || !nextReady:
		return "rebind_grace_elapsed"
	default:
		return "rebind_next_ready"
	}
}

// nextSession reports, for a bridge of serviceID whose session ends at end,
// whether the session manager already holds a later session, and the last
// height the ended one is honoured at. Asking also starts the background
// refresh once the end is past.
func (r *WSRelayer) nextSession(serviceID domain.ServiceID) func(end int64) (bool, int64) {
	return func(end int64) (bool, int64) {
		p := r.deps.Protocol
		graceEnd := end + p.sessions.graceBlocks.Load()
		appAddr, err := p.pickApp(serviceID)
		if err != nil {
			return true, graceEnd
		}
		session, err := p.sessions.getSession(context.Background(), string(serviceID), appAddr)
		if err != nil || session == nil || session.Header == nil {
			return true, graceEnd
		}
		return session.Header.SessionEndBlockHeight > end, graceEnd
	}
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
	serviceID domain.ServiceID,
	sessionEnd *atomic.Int64,
	current *atomic.Pointer[wsMessageProcessor],
	bridge *websockets.Bridge,
	logger *slog.Logger,
	next func(end int64) (ready bool, graceEnd int64),
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
	var drainMovedAt time.Time

	for {
		select {
		case <-bridge.Done():
			return
		case <-ticker.C:
			if r.moveOffDrained(serviceID, current, bridge, &drainMovedAt) {
				continue
			}
			height := r.chainHeight()
			end := sessionEnd.Load()
			if height < end {
				continue
			}
			ready, graceEnd := true, end
			if next != nil {
				ready, graceEnd = next(end)
			}
			action := sessionEndAction(height, end, actedOn, graceEnd, bridge.Rebindable(), ready)
			if action == sessionWait {
				continue
			}
			if r.deps.Metrics != nil {
				r.deps.Metrics.SessionEndAction(serviceID, sessionEndLabel(action, ready, height > graceEnd), height-end)
			}
			if action == sessionRebind {
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
// whatever is still buffered and exits. It is the bridge's only frame grader,
// so the success-signal gate lives here, one per bridge.
func (r *WSRelayer) drainFrameEvents(
	serviceID domain.ServiceID,
	ch <-chan wsFrameEvent,
	done <-chan struct{},
	answered func() int,
) {
	gate := wsSuccessGate{answered: answered}
	for {
		select {
		case evt := <-ch:
			r.handleEndpointFrame(serviceID, evt.endpoint, evt.payload, evt.err, evt.latency, &gate)
		case <-done:
			for {
				select {
				case evt := <-ch:
					r.handleEndpointFrame(serviceID, evt.endpoint, evt.payload, evt.err, evt.latency, &gate)
				default:
					return
				}
			}
		}
	}
}

// wsSuccessGate admits one success signal per supplier per
// wsSuccessSignalInterval. A rebind moves the bridge to another supplier,
// whose first success is admitted at once.
type wsSuccessGate struct {
	endpoint domain.EndpointAddr
	last     time.Time
	// answered, when set, takes the count of client requests the bridge's
	// supplier answered since it was last asked (ws_rate_counts_answers).
	answered func() int
}

// weight is how many attempts an admitted success stands for in the failure
// rate (reputation.Signal.Weight): itself, and every request answered since
// the last one.
func (g *wsSuccessGate) weight() int {
	if g == nil || g.answered == nil {
		return 0
	}
	return 1 + g.answered()
}

// admit reports whether a success for endpoint at now should be recorded. A
// nil gate admits everything.
func (g *wsSuccessGate) admit(endpoint domain.EndpointAddr, now time.Time) bool {
	if g == nil {
		return true
	}
	if endpoint == g.endpoint && now.Sub(g.last) < wsSuccessSignalInterval {
		return false
	}
	g.endpoint, g.last = endpoint, now
	return true
}

// frameSink is the processor's per-frame callback for a bridge: frames go
// to the bridge's analysis queue, dropped when it is full (analysis is
// advisory). A frame that failed verification is not: it is graded on its
// own, because the loss it causes is not charged again (lossIsSuppliers), so
// dropped from a full queue it was graded nowhere. Such frames are rare, and
// a rebind follows each.
func (r *WSRelayer) frameSink(serviceID domain.ServiceID, addr domain.EndpointAddr, frameCh chan<- wsFrameEvent) func([]byte, error, time.Duration) {
	return func(payload []byte, frameErr error, latency time.Duration) {
		if frameErr != nil && !errors.Is(frameErr, ErrEndpointControlFrame) {
			safego.Go(r.deps.Logger, "shannon.ws.frame_error", func() {
				r.handleEndpointFrame(serviceID, addr, payload, frameErr, latency, nil)
			})
			return
		}
		select {
		case frameCh <- wsFrameEvent{endpoint: addr, payload: payload, err: frameErr, latency: latency}:
		default:
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
	gate *wsSuccessGate,
) {
	// A control frame is the miner reporting a condition — a session expiry,
	// most often — not the supplier answering badly. It is the one frame that
	// is graded neither up nor down: recording a success would reward a
	// supplier for an error, and recording a failure would penalise it for a
	// session boundary it does not control. The observation still goes out,
	// forced, because a client did receive a non-2xx and that is worth seeing.
	if errors.Is(frameErr, ErrEndpointControlFrame) {
		r.submitObservation(serviceID, endpointAddr, payload)
		return
	}

	// If the processor handed us an error (validation failure, supplier
	// signature rejected), treat as a major supplier error without running
	// heuristic on the raw bytes.
	if frameErr != nil {
		_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, endpointAddr, domain.RPCTypeWebSocket,
			reputation.NewSignal(reputation.SignalMajorError, "ws_validate_err:"+frameErr.Error(), latency))
		r.submitObservation(serviceID, endpointAddr, payload)
		return
	}

	res := heuristic.AnalyzeFrame(payload, domain.RPCTypeWebSocket)
	// A penalty is always recorded; a success only through the gate (see
	// wsSuccessSignalInterval). The observation below is sampled either way.
	if res.ShouldPenalize || gate.admit(endpointAddr, time.Now()) {
		sig := frameSeverityToSignal(res, latency)
		if !res.ShouldPenalize {
			sig.Weight = gate.weight()
		}
		_ = r.deps.Reputation.RecordSignal(context.Background(), serviceID, endpointAddr, domain.RPCTypeWebSocket, sig)
	}

	// Always submit if heuristic penalized; otherwise sample.
	if res.ShouldPenalize || rand.Float64() < r.deps.FrameObservationSampleRate {
		r.submitObservation(serviceID, endpointAddr, payload)
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
		Source:       observe.SourceRelay,
	})
}

// submitObservation builds and submits an Observation for a single frame.
func (r *WSRelayer) submitObservation(
	serviceID domain.ServiceID,
	endpointAddr domain.EndpointAddr,
	payload []byte,
) {
	obs := observe.Observation{
		ServiceID:    serviceID,
		EndpointAddr: endpointAddr,
		Source:       observe.SourceRelay,
		ResponseBody: payload,
	}
	r.deps.Observe.Submit(obs)
}

// frameSeverityToSignal maps a heuristic AnalysisResult to a reputation
// signal. Per-frame severity is downgraded one step (and capped at Critical)
// so a single bad frame never sinks an endpoint to probation from healthy.
func frameSeverityToSignal(res heuristic.AnalysisResult, latency time.Duration) reputation.Signal {
	if !res.ShouldPenalize {
		return reputation.NewSignal(reputation.SignalSuccess, "ws_frame_ok", latency)
	}
	reason := "ws_" + res.Reason
	if res.Reason == heuristic.ReasonQuotaExceeded {
		// Not downgraded: a spent quota is not one bad frame in a healthy
		// subscription, it is every answer until the window resets.
		return reputation.NewSignal(reputation.SignalMajorError, reason, latency)
	}
	switch res.PenaltySeverity {
	case heuristic.SeverityFatal:
		return reputation.NewSignal(reputation.SignalCriticalError, reason, latency)
	case heuristic.SeverityCritical:
		return reputation.NewSignal(reputation.SignalMajorError, reason, latency)
	default: // Major and below all land on Minor.
		return reputation.NewSignal(reputation.SignalMinorError, reason, latency)
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
// session, URL and signing app. The second return is the message for the
// HTTP error Open sends when this fails before the upgrade.
//
// The order is the point. Fresh endpoints first (freshest), then the best
// reputation tier among them, and only within that tier the share cap and
// the avoidance of tried (endpoints this connection lost to a failure,
// operator-aware like retry). Untried came first until 2026-10-03, and tried
// held every endpoint a connection had ever been bound to, session-end
// rebinds included: a connection rebinding at every session end ran out of
// untried tier-1 operators within the hour, and its next rebind went to
// whatever was left, a trust-penalised owner among them (mainnet robinhood:
// 11% of connections). The next tier is reached only when every endpoint of
// the best one failed on this connection.
//
// self is the connection being placed (nil when opening), for the share cap.
func (r *WSRelayer) resolveEndpoint(ctx context.Context, serviceID domain.ServiceID, tried map[domain.EndpointAddr]bool, self *wsLive) (*wsTarget, string, error) {
	appAddr, err := r.deps.Protocol.pickApp(serviceID)
	if err != nil {
		return nil, "no app configured", fmt.Errorf("pick app: %w", err)
	}
	// The session a WebSocket is signed for is the one at the chain's
	// current height (currentSession). Fetched first, so the candidates
	// below come from it too.
	session, err := r.deps.Protocol.sessions.currentSession(ctx, string(serviceID), appAddr)
	if err != nil {
		return nil, "session unavailable", fmt.Errorf("session: %w", err)
	}
	endpoints, err := r.deps.Protocol.AvailableEndpoints(ctx, serviceID, domain.RPCTypeWebSocket)
	if err != nil {
		return nil, "no websocket endpoints available", fmt.Errorf("available endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return nil, "no websocket endpoints available", errors.New("no endpoints for rpc type websocket")
	}
	candidates := r.freshest(serviceID, endpoints)
	if top := r.topTier(ctx, serviceID, candidates); !allTried(top, tried) {
		candidates = top
	}
	candidates = r.capShare(ctx, serviceID, candidates, self)
	candidates = untriedFirst(candidates, tried, r.deps.Flags.IsEnabled(ctx, featureflag.FlagOperatorAwareSelection, serviceID))
	load := r.snapshotLoad()
	addr := r.deps.Reputation.SelectSpread(ctx, serviceID, candidates, domain.RPCTypeWebSocket, load)
	if addr == "" {
		return nil, "no viable websocket endpoint", errors.New("empty selection")
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

// freshest narrows WebSocket candidates by the service plugin's block-height
// filter — the same one HTTP selection applies, with its sync allowance and
// degradation tiers. A lagging node handed a newHeads subscriber streams
// stale heads for as long as the connection lives, and a WebSocket
// connection lives for hours; HTTP relays were the only thing kept off stale
// nodes. Heights come from the endpoint's JSON-RPC health checks, keyed by the
// same address the WebSocket path resolves.
//
// HTTP's other guard — never narrow onto endpoints reputation does not vouch
// for — is not applied: most WebSocket keys have never had a connection and
// are unvouched, so it would keep the stale-but-scored endpoints in exactly
// the case this exists for. A fresh endpoint that turns out not to serve
// WebSocket costs a dial failure, a penalty and a rebind, and the probes.
// Never narrows to nothing.
func (r *WSRelayer) freshest(serviceID domain.ServiceID, candidates domain.EndpointAddrList) domain.EndpointAddrList {
	if r.deps.QoS == nil || len(candidates) < 2 {
		return candidates
	}
	plugin := r.deps.QoS.Get(serviceID)
	if plugin == nil {
		return candidates
	}
	filtered, err := plugin.SelectEndpoints(candidates, nil)
	if err != nil || len(filtered) == 0 {
		return candidates
	}
	return filtered
}

// topTier narrows candidates to the best reputation tier present, when the
// reputation service can say (reputation.TopTierer).
func (r *WSRelayer) topTier(ctx context.Context, serviceID domain.ServiceID, candidates domain.EndpointAddrList) domain.EndpointAddrList {
	t, ok := r.deps.Reputation.(reputation.TopTierer)
	if !ok {
		return candidates
	}
	if top := t.TopTier(ctx, serviceID, candidates, domain.RPCTypeWebSocket); len(top) > 0 {
		return top
	}
	return candidates
}

// allTried reports whether every endpoint in eps is in tried: every one of
// them failed on this connection.
func allTried(eps domain.EndpointAddrList, tried map[domain.EndpointAddr]bool) bool {
	if len(tried) == 0 || len(eps) == 0 {
		return false
	}
	for _, ep := range eps {
		if !tried[ep] {
			return false
		}
	}
	return true
}

// noteRebind updates tried for a rebind away from lost. A planned rebind
// (a session end, an operator's request) is no failure and starts afresh:
// tried is cleared. A loss marks lost tried, avoided by the next picks.
func noteRebind(tried map[domain.EndpointAddr]bool, lost domain.EndpointAddr, cause error) {
	if errors.Is(cause, websockets.ErrBridgeSessionExpired) || errors.Is(cause, websockets.ErrBridgeReplaceRequested) {
		clear(tried)
		return
	}
	tried[lost] = true
}

// untriedFirst narrows endpoints to the ones not in tried, preferring ones
// affiliated with no tried endpoint — neither its operator nor its owner —
// when operatorAware; each narrowing is a preference, dropped when it would
// leave nothing.
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
	var affiliates domain.Affiliates
	for ep := range tried {
		affiliates.Add(ep)
	}
	return untried.ExcludeAffiliates(affiliates)
}

// RebindService asks every live bridge for serviceID to replace its supplier,
// as if the supplier had been lost: a new one is selected (avoiding the ones
// each connection has used), the live subscriptions are replayed, and the
// client sees nothing. It returns how many bridges were asked. This is the
// admin rebind route — a drill, or the way to move live connections off an
// operator that was just drained, which selection alone never touches.
//
// Each rebind resolves and dials a supplier, so they run in the background:
// done one after another, the admin request took the sum of every dial.
func (r *WSRelayer) RebindService(serviceID domain.ServiceID) int {
	n := 0
	r.live.Range(func(key, value any) bool {
		if value.(*wsLive).service != serviceID {
			return true
		}
		bridge := key.(*websockets.Bridge)
		safego.Go(r.deps.Logger, "websocket.rebind", func() {
			bridge.ReplaceEndpoint(websockets.ErrBridgeReplaceRequested)
		})
		n++
		return true
	})
	return n
}
