package shannon

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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
)

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
		score, err := r.deps.Reputation.GetScore(ctx, serviceID, addr, domain.RPCTypeWebSocket)
		if err != nil || score >= 100 {
			continue
		}
		out = append(out, wsProbeTarget{serviceID: serviceID, addr: addr, score: score, frame: frame})
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
	sig := reputation.NewSuccessSignal("ws_probe_"+result, 0)
	if result != wsProbeOK && result != wsProbeOtherDialect {
		sig = reputation.NewMajorErrorSignal("ws_probe_"+result, 0)
	}
	sig.Probe = true
	_ = r.deps.Reputation.RecordSignal(context.Background(), t.serviceID, t.addr, domain.RPCTypeWebSocket, sig)
}

// probeLogger discards: a probe failing is the expected case (it is aimed at
// keys that already failed), and the connect path logs at error.
var probeLogger = slog.New(slog.DiscardHandler)

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
	url, err := ep.GetURL(domain.RPCTypeWebSocket)
	if err != nil {
		return wsProbeUnresolved
	}
	appAddr := session.Header.ApplicationAddress
	app, err := r.deps.Protocol.getApp(ctx, appAddr)
	if err != nil {
		return wsProbeUnresolved
	}
	proc := newWSMessageProcessor(ctx, r.deps.Protocol, session.Header, ep.Supplier(), t.addr, app, nil)
	wire, err := proc.ProcessClientMessage(t.frame)
	if err != nil {
		return wsProbeUnresolved
	}

	conn, err := websockets.ConnectEndpoint(probeLogger, url, relayMinerHeaders(t.serviceID, appAddr))
	if err != nil {
		return wsProbeDialFailed
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
