package shannon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/reputation"
	"github.com/pokt-network/sage/websockets"
)

// WebSocket recovery probes.
//
// A WebSocket key earns score only from connections, and SelectSpread hands
// connections only to the top tier while one is populated. A key demoted once
// therefore got no connection, so no signal, so no way back: on mainnet the
// WebSocket keys below tier 1 sat at the same count for 24 hours. The
// health-check executor cannot help — it sends one-shot HTTP relays — so the
// relayer probes its own: dial, one signed request, one validated answer,
// close. That tests what a client would use, the relay miner's WebSocket
// bridge and the backend's WebSocket port, not the supplier's HTTP port.
//
// Only keys below full score are probed, lowest first, one endpoint per URL
// (the reputation key), a bounded number per cycle. Reputation ignores a probe
// success on a key traffic is already grading, so a probe moves only what
// nothing else is measuring. Each pod probes for itself, as each pod grades
// its own traffic.
const (
	wsProbeInterval    = time.Minute
	wsProbeTimeout     = 10 * time.Second
	wsProbeMaxPerCycle = 32
	wsProbeWorkers     = 4
	// wsProbeMaxBackoff caps the wait between probes of a URL that keeps
	// failing. Every probe is two paid relays (the signed request and its
	// answer's claim); on mainnet (2026-09-26) the same dead URLs were
	// redialled every minute, ~1,600 dial failures an hour. Doubling from
	// the interval reaches the cap after six straight failures.
	wsProbeMaxBackoff = 32 * time.Minute
)

// wsProbeBackoff holds, per probed URL, when it may be probed again. A URL
// is dropped from it on its first success.
// ponytail: grows with every WebSocket URL that ever failed a probe; staked
// URLs number in the hundreds, prune by age if that stops holding.
type wsProbeBackoff struct {
	mu   sync.Mutex
	next map[string]wsProbeRetry
}

type wsProbeRetry struct {
	at    time.Time
	fails int
}

// due reports whether key may be probed at now.
func (b *wsProbeBackoff) due(key string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !now.Before(b.next[key].at)
}

// record books key's next probe: none pending after a success, an
// exponentially later one after each consecutive failure.
func (b *wsProbeBackoff) record(key string, failed bool, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !failed {
		delete(b.next, key)
		return
	}
	if b.next == nil {
		b.next = make(map[string]wsProbeRetry)
	}
	r := b.next[key]
	wait := wsProbeInterval << min(r.fails, 5)
	r.fails++
	r.at = now.Add(min(wait, wsProbeMaxBackoff))
	b.next[key] = r
}

// Probe results, the closed set sage_websocket_probes_total is labelled by.
const (
	wsProbeOK            = "ok"
	wsProbeOtherDialect  = "other_dialect"  // answered -32601: alive, speaks another API on this socket; graded ok
	wsProbeUnresolved    = "unresolved"     // no session, URL or app to sign with: not the supplier's fault, not graded
	wsProbeDialFailed    = "dial_failed"    // the upgrade was refused or never completed
	wsProbeNoAnswer      = "no_answer"      // connected, but no valid answer in time
	wsProbeInvalid       = "invalid"        // the answer failed relay validation
	wsProbeErrorResponse = "error_response" // a valid relay whose payload is a JSON-RPC error or no result
)

// jsonRPCMethodNotFound is the JSON-RPC 2.0 "method not found" error code.
const jsonRPCMethodNotFound = -32601

// ProbeWebSockets runs recovery probes for services until ctx ends. Call it
// once, on its own goroutine.
func (r *WSRelayer) ProbeWebSockets(ctx context.Context, services []domain.ServiceID) {
	if len(services) == 0 {
		return
	}
	ticker := time.NewTicker(wsProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(r.deps.Logger, "websocket.probe.cycle", func() { r.probeCycle(ctx, services) })
		}
	}
}

// wsProbeTarget is one endpoint to probe and the frame to send it.
type wsProbeTarget struct {
	serviceID domain.ServiceID
	addr      domain.EndpointAddr
	url       string
	score     float64
	frame     []byte
}

// probeCycle probes the lowest-scoring WebSocket keys across services.
func (r *WSRelayer) probeCycle(ctx context.Context, services []domain.ServiceID) {
	var targets []wsProbeTarget
	for _, sid := range services {
		targets = append(targets, r.probeTargets(ctx, sid)...)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].score < targets[j].score })
	if len(targets) > wsProbeMaxPerCycle {
		targets = targets[:wsProbeMaxPerCycle]
	}

	work := make(chan wsProbeTarget)
	var wg sync.WaitGroup
	for i := 0; i < wsProbeWorkers; i++ {
		wg.Add(1)
		safego.Go(r.deps.Logger, "websocket.probe.worker", func() {
			defer wg.Done()
			for t := range work {
				r.probeEndpoint(ctx, t)
			}
		})
	}
	for _, t := range targets {
		work <- t
	}
	close(work)
	wg.Wait()
}

// probeTargets lists the service's WebSocket endpoints worth a probe: below
// full score, one per URL.
func (r *WSRelayer) probeTargets(ctx context.Context, serviceID domain.ServiceID) []wsProbeTarget {
	if !r.deps.Flags.IsEnabled(ctx, featureflag.FlagWebsocketProbes, serviceID) ||
		!r.deps.Flags.IsEnabled(ctx, featureflag.FlagWebsocketRelays, serviceID) {
		return nil
	}
	var prober qos.WebSocketProber
	if r.deps.QoS != nil {
		prober, _ = r.deps.QoS.Get(serviceID).(qos.WebSocketProber)
	}
	if prober == nil {
		return nil
	}
	endpoints, err := r.deps.Protocol.AvailableEndpoints(ctx, serviceID, domain.RPCTypeWebSocket)
	if err != nil {
		return nil
	}
	frame := prober.WebSocketProbe()
	seen := make(map[string]bool, len(endpoints))
	var out []wsProbeTarget
	for _, addr := range endpoints {
		ep, ok := r.deps.Protocol.sessions.lookupEndpoint(addr)
		if !ok {
			continue
		}
		url, err := ep.GetURL(domain.RPCTypeWebSocket)
		if err != nil || seen[url] {
			continue
		}
		seen[url] = true
		if !r.probeBackoff.due(string(serviceID)+"|"+url, time.Now()) {
			continue
		}
		score, err := r.deps.Reputation.GetScore(ctx, serviceID, addr, domain.RPCTypeWebSocket)
		if err != nil || score >= 100 {
			continue
		}
		out = append(out, wsProbeTarget{serviceID: serviceID, addr: addr, url: url, score: score, frame: frame})
	}
	return out
}

// probeEndpoint dials one endpoint, sends the probe frame signed as a relay,
// and grades the answer.
func (r *WSRelayer) probeEndpoint(ctx context.Context, t wsProbeTarget) {
	result := r.runProbe(ctx, t)
	if r.deps.Metrics != nil {
		r.deps.Metrics.Probed(t.serviceID, result)
	}
	if result == wsProbeUnresolved {
		return
	}
	failed := result != wsProbeOK && result != wsProbeOtherDialect
	r.probeBackoff.record(string(t.serviceID)+"|"+t.url, failed, time.Now())
	sig := reputation.NewSuccessSignal("ws_probe_"+result, 0)
	if failed {
		sig = reputation.NewMajorErrorSignal("ws_probe_"+result, 0)
	}
	sig.Probe = true
	_ = r.deps.Reputation.RecordSignal(context.Background(), t.serviceID, t.addr, domain.RPCTypeWebSocket, sig)
}

// probeLogger discards: a probe failing is the expected case (it is aimed at
// keys that already failed), and the connect path logs at error.
var probeLogger = slog.New(slog.DiscardHandler)

// errSignedDial marks a dialSigned failure at the dial itself, after the
// frame was signed: the endpoint's fault, not the gateway's.
var errSignedDial = errors.New("dial failed")

// dialSigned signs frame as a relay to ep under session h and dials url with
// the relay miner's headers: the one signed connection a probe and a debug
// subscription open outside any bridge. The processor it returns validates
// the answers. A dial failure wraps errSignedDial; any other error is the
// gateway failing to sign.
func (r *WSRelayer) dialSigned(ctx context.Context, serviceID domain.ServiceID, h *sessiontypes.SessionHeader, ep *endpoint, addr domain.EndpointAddr, url string, frame []byte) (*wsMessageProcessor, []byte, *websocket.Conn, error) {
	app, err := r.deps.Protocol.getApp(ctx, h.ApplicationAddress)
	if err != nil {
		return nil, nil, nil, errors.New("application unavailable for signing")
	}
	proc := newWSMessageProcessor(ctx, r.deps.Protocol, h, ep.Supplier(), addr, app, nil)
	wire, err := proc.ProcessClientMessage(frame)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("signing the frame failed: %w", err)
	}
	conn, err := websockets.ConnectEndpoint(probeLogger, url, relayMinerHeaders(serviceID, h.ApplicationAddress))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %w", errSignedDial, err)
	}
	return proc, wire, conn, nil
}

// runProbe performs one probe and returns its result.
func (r *WSRelayer) runProbe(ctx context.Context, t wsProbeTarget) string {
	ep, ok := r.deps.Protocol.sessions.lookupEndpoint(t.addr)
	if !ok {
		return wsProbeUnresolved
	}
	session := ep.Session()
	if session == nil || session.Header == nil {
		return wsProbeUnresolved
	}
	proc, wire, conn, err := r.dialSigned(ctx, t.serviceID, session.Header, ep, t.addr, t.url, t.frame)
	if errors.Is(err, errSignedDial) {
		return wsProbeDialFailed
	}
	if err != nil {
		return wsProbeUnresolved
	}
	defer conn.Close()
	deadline := time.Now().Add(wsProbeTimeout)
	_ = conn.SetWriteDeadline(deadline)
	if err := conn.WriteMessage(websocket.TextMessage, wire); err != nil {
		return wsProbeDialFailed
	}
	_ = conn.SetReadDeadline(deadline)
	// The answer is the first frame carrying the probe's id; anything else a
	// miner sends first (it sends nothing unasked on a fresh socket, but a
	// probe must not be fooled by one) is skipped.
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return wsProbeNoAnswer
		}
		payload, err := proc.ProcessEndpointMessage(data)
		if err != nil {
			return wsProbeInvalid
		}
		if qos.JSONRPCRequestID(payload) != "1" {
			continue
		}
		// "Method not found" is a live backend that speaks another dialect
		// on this socket, and that is all a probe asks. Cosmos EVM chains
		// stake their EVM surface as WebSocket: the plugin's CometBFT
		// status probe gets -32601 from every healthy supplier there, and
		// grading that as a failure held sei's WebSocket keys down.
		if qos.JSONRPCHasError(payload) {
			if gjson.GetBytes(payload, "error.code").Int() == jsonRPCMethodNotFound {
				return wsProbeOtherDialect
			}
			return wsProbeErrorResponse
		}
		if !gjson.GetBytes(payload, "result").Exists() {
			return wsProbeErrorResponse
		}
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "probe done"), time.Now().Add(time.Second))
		return wsProbeOK
	}
}
