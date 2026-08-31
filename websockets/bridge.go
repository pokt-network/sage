// Package websockets provides a generic, protocol-agnostic bidirectional
// WebSocket bridge between a client and a backend endpoint.
//
// The protocol layer hooks in via the MessageProcessor interface; the bridge
// itself has no knowledge of Shannon, QoS, or any application protocol.
//
// Architecture note — supplier rotation readiness:
//
//	Client ←─ clientConn ─→ Bridge ←─ endpointConn ─→ Endpoint
//
// The client-facing and endpoint-facing connections are deliberately separate
// fields so that the endpointConn can be swapped in the future to support
// supplier rotation within a single client session. The current implementation
// is sticky (one endpoint per Bridge lifetime).
package websockets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/internal/safego"
)

// MessageProcessor transforms messages before forwarding them across the bridge.
// A processor that returns (nil, nil) consumes the message: nothing is
// forwarded and the bridge carries on. That is how a rebind's replay
// acknowledgements — answers to requests the client never sent — stay out
// of the client's stream.
// The protocol layer implements this interface to sign, validate, or observe
// messages without the bridge needing to know anything about the protocol.
type MessageProcessor interface {
	// ProcessClientMessage is called for every message received from the client
	// before it is forwarded to the endpoint.
	ProcessClientMessage(data []byte) ([]byte, error)

	// ProcessEndpointMessage is called for every message received from the
	// endpoint before it is forwarded to the client.
	ProcessEndpointMessage(data []byte) ([]byte, error)
}

// Bridge routes data bidirectionally between a client WebSocket connection and
// an endpoint WebSocket connection.
//
// Lifecycle:
//  1. StartBridge upgrades the client HTTP connection, dials the endpoint, and
//     starts internal goroutines.
//  2. Each side runs an independent readLoop goroutine that feeds messages into
//     msgChan.
//  3. The main loop reads from msgChan, routes through the MessageProcessor,
//     and writes to the other side.
//  4. Any error triggers Shutdown, which cancels the context, sends close
//     frames, closes both connections, and closes the done channel.
//
// Error Handling:
//   - Read errors in readLoop → Shutdown (close codes captured first)
//   - Processing errors in main loop → Shutdown
//   - Write errors in main loop → Shutdown
//   - All paths converge at Shutdown via shutdownOnce (idempotent)
type Bridge struct {
	ctx       context.Context
	cancelCtx context.CancelFunc
	logger    *slog.Logger

	clientConn *Connection

	// endpointConn and processor are replaced together by a rebind.
	// endpointMu serialises that swap against route and the replay writes,
	// so a client frame that arrives mid-rebind waits rather than hitting a
	// dead socket; endpointConn is additionally an atomic pointer so
	// Shutdown and the close-code logic can read it without the lock.
	endpointMu   sync.RWMutex
	endpointConn atomic.Pointer[Connection]
	processor    MessageProcessor

	msgChan chan message

	shutdownOnce sync.Once
	done         chan struct{}

	// pingPeriod / pongWait are the liveness knobs; see connection.go.
	pingPeriod time.Duration
	pongWait   time.Duration

	// observer, when non-nil, is told about frames and the close.
	observer Observer

	// endpointLost, when set, is asked for a replacement when the endpoint
	// side is lost; rebinds counts how many times it succeeded, against
	// rebindLimit.
	endpointLost EndpointLostHandler
	rebindLimit  int
	rebinds      int

	// stalled, when set, is polled every stallPeriod; true means the
	// endpoint is delivering nothing the client is waiting for.
	stalled     func() bool
	stallPeriod time.Duration
}

// defaultRebindLimit is how many endpoints one client connection may burn
// through before it is told to reconnect. Three: a pool where the third
// replacement also dies is not a pool a fourth pick will fix.
const defaultRebindLimit = 3

// BridgeOption tunes a bridge at StartBridge.
type BridgeOption func(*Bridge)

// WithLiveness sets how often each peer is pinged and how long it may stay
// silent before the bridge declares it gone. pingPeriod must be below
// pongWait; zero pongWait disables the check entirely.
func WithLiveness(pingPeriod, pongWait time.Duration) BridgeOption {
	return func(b *Bridge) {
		b.pingPeriod = pingPeriod
		b.pongWait = pongWait
	}
}

// StartBridge creates a Bridge, upgrades the client HTTP connection to WebSocket,
// connects to the endpoint WebSocket URL, starts the message routing goroutines,
// and returns the Bridge. The caller can block on Bridge.Done() to wait for
// shutdown.
//
// On error, any partially-created resources are cleaned up before returning.
//
// IMPORTANT: The only approved caller is shannon.WSRelayer.Open. That
// wrapper guarantees reputation, observation, and heuristic are always wired
// to the bridge's MessageProcessor. Calling StartBridge from elsewhere will
// ship a WS path that silently bypasses those systems — which is exactly
// the PATH failure this package exists to prevent. If you need WS from a
// new code path, add it to WSRelayer, don't call this function directly.
func StartBridge(
	ctx context.Context,
	logger *slog.Logger,
	clientReq *http.Request,
	clientWriter http.ResponseWriter,
	endpointURL string,
	endpointHeaders http.Header,
	processor MessageProcessor,
	opts ...BridgeOption,
) (*Bridge, error) {
	logger = logger.With("component", "websocket_bridge")

	// Upgrade the client HTTP connection to WebSocket first.
	rawClient, err := UpgradeClient(logger, clientReq, clientWriter)
	if err != nil {
		// UpgradeClient already wrote an HTTP error to clientWriter.
		return nil, fmt.Errorf("StartBridge: %w", err)
	}

	// Dial the backend endpoint.
	rawEndpoint, err := ConnectEndpoint(logger, endpointURL, endpointHeaders)
	if err != nil {
		// The client upgrade already succeeded, so the client is a live WebSocket
		// peer by now. Closing the socket bare skips the close handshake, and the
		// client reports that as 1006 or a protocol error — as though it had done
		// something wrong, when in truth the endpoint we picked for it is down.
		//
		// This is the one failure that happens after the upgrade but before the
		// bridge exists, so Shutdown's close-frame path is not available yet.
		//
		// 1013 (try again later) is the actionable answer: reconnecting usually
		// draws a different supplier.
		closeClientWithReason(logger, rawClient, websocket.CloseTryAgainLater,
			"upstream endpoint unavailable, please reconnect")
		return nil, fmt.Errorf("StartBridge: %w", err)
	}

	bridgeCtx, cancelCtx := context.WithCancel(ctx)

	b := &Bridge{
		ctx:         bridgeCtx,
		cancelCtx:   cancelCtx,
		logger:      logger,
		clientConn:  NewConnection(rawClient, SourceClient, logger.With("conn", "client")),
		processor:   processor,
		msgChan:     make(chan message, 32),
		done:        make(chan struct{}),
		pingPeriod:  defaultPingPeriod,
		pongWait:    defaultPongWait,
		rebindLimit: defaultRebindLimit,
	}
	b.endpointConn.Store(NewConnection(rawEndpoint, SourceEndpoint, logger.With("conn", "endpoint")))
	for _, opt := range opts {
		opt(b)
	}
	if b.pongWait > 0 {
		b.clientConn.setLiveness(b.pongWait)
		b.endpointConn.Load().setLiveness(b.pongWait)
	}
	if b.observer != nil {
		b.observer.Opened()
	}

	safego.Go(b.logger, "websocket.bridge", b.run)
	return b, nil
}

// Done returns a channel that is closed when the bridge has fully shut down.
func (b *Bridge) Done() <-chan struct{} {
	return b.done
}

// Shutdown performs a graceful, idempotent bridge shutdown:
//  1. Cancels the bridge context (signals readLoop goroutines to exit).
//  2. Sends a WebSocket close frame to both connections.
//  3. Closes both connections.
//  4. Closes the done channel to unblock Done() waiters.
//
// Safe to call from any goroutine and any number of times.
func (b *Bridge) Shutdown(err error) {
	b.shutdownOnce.Do(func() {
		b.logger.Warn("websocket: bridge shutting down", "err", err)

		// Cancel context first so readLoop goroutines stop sending on msgChan before
		// we stop draining it.
		b.cancelCtx()

		closeCode, closeText := b.determineCloseCode(err)
		closeCode = sanitizeCloseCode(closeCode)
		clientMsg := websocket.FormatCloseMessage(closeCode, closeText)
		if b.observer != nil {
			b.observer.Closed(b.closeInitiator(), closeCode)
		}

		// The two peers do not get the same frame. SAGE sits in the middle —
		//
		//   client --(SAGE is the server)-- SAGE --(SAGE is the client)--> relay miner
		//
		// — so a code that is correct facing one direction is nonsense facing
		// the other. See endpointCloseCode.
		endpointMsg := clientMsg
		if endpointCode := endpointCloseCode(closeCode); endpointCode != closeCode {
			endpointMsg = websocket.FormatCloseMessage(endpointCode, closeText)
		}

		deadline := time.Now().Add(time.Second)

		for _, c := range []struct {
			conn *Connection
			msg  []byte
		}{
			{b.clientConn, clientMsg},
			{b.endpointConn.Load(), endpointMsg},
		} {
			if c.conn == nil {
				continue
			}
			if writeErr := c.conn.WriteControl(websocket.CloseMessage, c.msg, deadline); writeErr != nil {
				b.logger.Warn("websocket: could not send close frame", "err", writeErr)
			}
			_ = c.conn.Close()
		}

		// NOTE: msgChan is intentionally NOT closed here. Closing it would cause a
		// "send on closed channel" panic if a readLoop goroutine concurrently tries
		// to send. Context cancellation is the signal for readLoop to exit; the
		// channel is garbage-collected once all goroutines have exited.
		close(b.done)
	})
}

// ---------- Internal ----------

// run is the main loop. It reads from msgChan, processes the message, and
// writes it to the other side. It also starts the two readLoop goroutines.
func (b *Bridge) run() {
	b.logger.Info("websocket: bridge started")
	safego.Go(b.logger, "websocket.read.client", func() { b.readLoop(b.clientConn) })
	b.startEndpointReadLoop(b.endpointConn.Load())
	if b.pongWait > 0 && b.pingPeriod > 0 {
		safego.Go(b.logger, "websocket.ping", b.pingLoop)
	}
	if b.stalled != nil && b.stallPeriod > 0 {
		safego.Go(b.logger, "websocket.stall", b.stallLoop)
	}

	for {
		select {
		case msg := <-b.msgChan:
			b.route(msg)

		case <-b.ctx.Done():
			b.Shutdown(ErrBridgeContextCanceled)
			return
		}
	}
}

// route processes a single message and forwards it to the opposite connection.
func (b *Bridge) route(msg message) {
	b.endpointMu.RLock()
	defer b.endpointMu.RUnlock()
	endpoint := b.endpointConn.Load()

	switch msg.source {
	case SourceClient:
		processed, err := b.processor.ProcessClientMessage(msg.data)
		if err != nil {
			b.logger.Error("websocket: client message processing failed", "err", err)
			b.Shutdown(fmt.Errorf("%w: %w", ErrBridgeMessageProcessing, err))
			return
		}
		if processed == nil {
			return // Consumed by the processor.
		}
		if writeErr := endpoint.WriteMessage(msg.messageType, processed); writeErr != nil {
			// The endpoint's read loop sees the same dead socket and drives
			// the rebind; this side only has to not shut the bridge down
			// underneath it. Without a handler it is the old failure.
			b.logger.Error("websocket: write to endpoint failed", "err", writeErr)
			if b.endpointLost == nil {
				b.Shutdown(fmt.Errorf("%w: write to endpoint: %w", ErrBridgeConnectionFailed, writeErr))
			}
			return
		}
		if b.observer != nil {
			b.observer.Frame(SourceClient, len(processed))
		}

	case SourceEndpoint:
		if msg.conn != endpoint {
			return // Read from an endpoint that has since been replaced.
		}
		processed, err := b.processor.ProcessEndpointMessage(msg.data)
		if err != nil {
			b.logger.Error("websocket: endpoint message processing failed", "err", err)
			b.Shutdown(fmt.Errorf("%w: %w", ErrBridgeMessageProcessing, err))
			return
		}
		if processed == nil {
			return // Consumed by the processor.
		}
		if writeErr := b.clientConn.WriteMessage(msg.messageType, processed); writeErr != nil {
			b.logger.Error("websocket: write to client failed", "err", writeErr)
			b.Shutdown(fmt.Errorf("%w: write to client: %w", ErrBridgeConnectionFailed, writeErr))
			return
		}
		if b.observer != nil {
			b.observer.Frame(SourceEndpoint, len(processed))
		}
	}
}

// closeInitiator reports who ended the bridge: a peer that sent a close
// frame, else the gateway (a deadline, a processing error, a shutdown).
func (b *Bridge) closeInitiator() CloseInitiator {
	if ep := b.endpointConn.Load(); ep != nil {
		if code, _ := ep.GetCloseInfo(); code != 0 {
			return InitiatorEndpoint
		}
	}
	if b.clientConn != nil {
		if code, _ := b.clientConn.GetCloseInfo(); code != 0 {
			return InitiatorClient
		}
	}
	return InitiatorGateway
}

// pingLoop pings both peers every pingPeriod. A ping that cannot be written
// is a dead socket found early; a ping that is written but never answered is
// found by the read deadline in readLoop.
func (b *Bridge) pingLoop() {
	ticker := time.NewTicker(b.pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			deadline := time.Now().Add(writeWait)
			for _, c := range []*Connection{b.clientConn, b.endpointConn.Load()} {
				if err := c.Ping(deadline); err != nil {
					b.logger.Warn("websocket: ping failed", "source", c.source, "err", err)
					if c.source == SourceEndpoint && b.endpointLost != nil {
						continue // The endpoint read loop drives the rebind.
					}
					b.Shutdown(fmt.Errorf("%w: ping %v: %w", ErrBridgeConnectionFailed, c.source, err))
					return
				}
			}
		}
	}
}

// stallLoop polls the stall detector. A stall is an endpoint loss the socket
// did not report: the same path as one it did.
func (b *Bridge) stallLoop() {
	ticker := time.NewTicker(b.stallPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if !b.stalled() {
				continue
			}
			b.logger.Warn("websocket: subscriptions stalled, replacing endpoint")
			b.observe(func(o Observer) { o.Stalled() })
			b.ReplaceEndpoint(ErrBridgeStalled)
		}
	}
}

// ReplaceEndpoint treats the current endpoint as lost for the given cause:
// a rebind when a handler is installed, otherwise a close with the client
// told to reconnect. Safe to call from any goroutine; a bridge already
// shutting down ignores it.
func (b *Bridge) ReplaceEndpoint(cause error) {
	if b.ctx.Err() != nil {
		return
	}
	if b.endpointLost == nil {
		b.Shutdown(fmt.Errorf("%w: %w", ErrBridgeEndpointUnavailable, cause))
		return
	}
	// Synchronous: the rebind runs here, and returns once the endpoint is
	// swapped or the bridge is closed. The old endpoint's read loop then
	// fails on its closed socket and finds its endpoint already replaced.
	b.rebind(b.endpointConn.Load(), cause)
}

// CanRebind reports whether an endpoint loss would be met with a rebind
// rather than a close: a handler is installed and the limit is not spent.
func (b *Bridge) CanRebind() bool {
	if b.endpointLost == nil {
		return false
	}
	b.endpointMu.RLock()
	defer b.endpointMu.RUnlock()
	return b.rebinds < b.rebindLimit
}

// readLoop continuously reads from conn and sends messages to msgChan.
// It exits when the connection closes (error from ReadMessage) or when the
// bridge context is canceled. On read error the close code is captured and
// Shutdown is called to trigger graceful teardown.
func (b *Bridge) readLoop(conn *Connection) {
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			// Capture close code for bidirectional propagation before shutting down.
			code, text := extractCloseInfo(err)
			if code != 0 {
				conn.SetCloseInfo(code, text)
				b.logger.Info("websocket: peer sent close frame",
					"source", conn.source,
					"code", code,
					"text", text,
				)
			} else if isTimeout(err) {
				// Nothing — no data, no pong — for a whole pong wait. The
				// peer is gone whatever the socket says.
				b.logger.Warn("websocket: peer unresponsive", "source", conn.source, "pong_wait", b.pongWait)
				// Only the side that caused the close: once Shutdown has run
				// the other side's read fails too, and a deadline that
				// happens to fire in that window is not a second finding.
				if b.observer != nil && b.ctx.Err() == nil {
					b.observer.Unresponsive(conn.source)
				}
				b.endpointGone(conn, fmt.Errorf("%w: %v silent for %v", ErrBridgePeerUnresponsive, conn.source, b.pongWait))
				return
			} else {
				b.logger.Warn("websocket: read error", "source", conn.source, "err", err)
			}
			b.endpointGone(conn, fmt.Errorf("%w: read from %v: %w", ErrBridgeConnectionFailed, conn.source, err))
			return
		}

		// Send to msgChan, but bail out if the context has been canceled to avoid
		// sending on a channel that is no longer being drained.
		select {
		case b.msgChan <- message{source: conn.source, messageType: msgType, data: data, conn: conn}:
		case <-b.ctx.Done():
			return
		}
	}
}

// startEndpointReadLoop runs a read loop for one endpoint connection. Each
// endpoint a bridge ever holds gets its own; an old loop ends with its
// socket, and its last error is ignored because the socket is no longer
// the bridge's.
func (b *Bridge) startEndpointReadLoop(conn *Connection) {
	safego.Go(b.logger, "websocket.read.endpoint", func() { b.readLoop(conn) })
}

// endpointGone is where a read error goes: a client loss, or an endpoint
// loss with no handler, shuts the bridge down as before; an endpoint loss
// with a handler is a rebind.
func (b *Bridge) endpointGone(conn *Connection, cause error) {
	if conn.source != SourceEndpoint || b.endpointLost == nil || b.ctx.Err() != nil {
		b.Shutdown(cause)
		return
	}
	b.rebind(conn, cause)
}

// rebind replaces a lost endpoint with one the handler supplies, holding the
// endpoint lock for the whole swap so client frames queue behind it rather
// than reaching the dead socket. On success the new endpoint's read loop
// starts and the replay frames are signed and sent; on failure, or past the
// limit, the bridge closes with the client told to reconnect.
func (b *Bridge) rebind(lost *Connection, cause error) {
	b.endpointMu.Lock()
	if b.endpointConn.Load() != lost {
		b.endpointMu.Unlock()
		return // A loop on an endpoint that was already replaced.
	}
	if b.rebinds >= b.rebindLimit {
		b.endpointMu.Unlock()
		b.logger.Warn("websocket: rebind limit reached, closing", "rebinds", b.rebinds, "cause", cause)
		b.observe(func(o Observer) { o.Rebound(RebindExhausted) })
		b.Shutdown(fmt.Errorf("%w: rebind limit %d reached: %w", ErrBridgeEndpointUnavailable, b.rebindLimit, cause))
		return
	}
	b.logger.Warn("websocket: endpoint lost, rebinding", "rebind", b.rebinds+1, "cause", cause)

	raw, processor, replay, err := b.endpointLost(b.ctx, cause)
	if err != nil {
		b.endpointMu.Unlock()
		b.logger.Warn("websocket: rebind failed, closing", "err", err)
		b.observe(func(o Observer) { o.Rebound(RebindFailed) })
		b.Shutdown(fmt.Errorf("%w: rebind: %w", ErrBridgeEndpointUnavailable, err))
		return
	}
	if b.ctx.Err() != nil {
		// Shut down while the handler was dialling: Shutdown closed the old
		// endpoint, so this one is ours to close.
		b.endpointMu.Unlock()
		_ = raw.Close()
		return
	}
	_ = lost.Close()
	next := NewConnection(raw, SourceEndpoint, b.logger.With("conn", "endpoint"))
	if b.pongWait > 0 {
		next.setLiveness(b.pongWait)
	}
	b.endpointConn.Store(next)
	b.processor = processor
	if !errors.Is(cause, ErrBridgeSessionExpired) {
		// A session rollover is a planned move, not evidence of a dying
		// pool; only losses count toward the limit.
		b.rebinds++
	}
	b.endpointMu.Unlock()

	b.observe(func(o Observer) { o.Rebound(RebindOK) })
	b.startEndpointReadLoop(next)

	// Replay under the read lock, like any client frame: a second loss during
	// the replay is the new loop's to handle.
	b.endpointMu.RLock()
	defer b.endpointMu.RUnlock()
	for _, frame := range replay {
		wire, err := b.processor.ProcessClientMessage(frame)
		if err != nil {
			b.logger.Warn("websocket: replay frame failed to process", "err", err)
			continue
		}
		if wire == nil {
			continue
		}
		if err := next.WriteMessage(websocket.TextMessage, wire); err != nil {
			b.logger.Warn("websocket: replay write failed", "err", err)
			return
		}
	}
}

// observe runs fn against the observer when there is one.
func (b *Bridge) observe(fn func(Observer)) {
	if b.observer != nil {
		fn(b.observer)
	}
}

// closeClientWithReason sends a best-effort close frame to an already-upgraded
// client connection, then closes it.
//
// Both steps are best-effort: the client may already be gone, and there is
// nothing to do about it if so — we are on our way out either way.
func closeClientWithReason(logger *slog.Logger, conn *websocket.Conn, closeCode int, reason string) {
	closeMsg := websocket.FormatCloseMessage(sanitizeCloseCode(closeCode), reason)
	if err := conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(time.Second)); err != nil {
		logger.Warn("websocket: could not send close frame to client", "err", err)
	}
	if err := conn.Close(); err != nil {
		logger.Warn("websocket: could not close client connection", "err", err)
	}
}

// isSendableCloseCode reports whether a close code is legal to put on the wire.
//
// RFC 6455 §7.4.1 reserves 1005, 1006 and 1015 for what a local endpoint
// *infers* — no status was sent, the connection dropped, TLS failed — and
// forbids sending them. Everything else in the protocol range is fine, as is
// the 3000-4999 application range. Mirrors gorilla's validReceivedCloseCodes,
// which is what our peers judge our frames by.
func isSendableCloseCode(code int) bool {
	switch code {
	case websocket.CloseNoStatusReceived, // 1005
		websocket.CloseAbnormalClosure, // 1006
		websocket.CloseTLSHandshake:    // 1015
		return false
	}
	if code >= 3000 && code <= 4999 {
		return true
	}
	return code >= 1000 && code <= 1014
}

// sanitizeCloseCode maps a close code we may have inferred locally onto one we
// are allowed to send, leaving legal codes untouched.
//
// This exists because determineCloseCode propagates the peer's close code, and
// a peer that vanishes gives us one we may not repeat. gorilla reports an
// abruptly dropped TCP connection as *CloseError{Code: 1006}; extractCloseInfo
// stores it, determineCloseCode hands it back, and FormatCloseMessage — which
// only special-cases 1005 — encodes it into a frame we then write to BOTH
// peers. The receiver checks it against validReceivedCloseCodes, finds 1006
// marked false, and rejects the whole frame as a protocol error. So the client
// never learns why it was disconnected, and the relay miner rejects ours the
// same way.
//
// 1011 (internal server error) is the honest substitute: something went wrong
// on our side of the connection and we cannot say more.
func sanitizeCloseCode(code int) int {
	if isSendableCloseCode(code) {
		return code
	}
	return websocket.CloseInternalServerErr // 1011
}

// endpointCloseCode adapts a client-facing close code for the UPSTREAM
// direction.
//
// SAGE is the server to the external client but the CLIENT to the relay miner,
// and RFC 6455 §7.4.1 defines 1011/1012/1013 as things a server tells a client:
// "internal server error", "service restarting, reconnect", "try again later".
// Sent upstream they invert the roles. "service restarting, please reconnect"
// — which is what every session rollover and every gateway shutdown currently
// sends the miner — asks the miner to reconnect to us, which is not a thing it
// does; "internal server error" reports our fault as if the endpoint had one.
// Neither describes what happened.
//
// 1001 Going Away is defined for both directions ("a server going down OR a
// browser having navigated away") and says it exactly: the peer that dialed you
// is leaving.
//
// Everything else passes through unchanged — 1000 means the same thing in both
// directions, and application codes (3000-4999, e.g. the relay miner's own 4000
// at session expiry) are propagated deliberately.
//
// gorilla accepts 1012 on read, so this is not about a protocol error; it is
// about the operator on the other end being told something true.
func endpointCloseCode(clientCode int) int {
	switch clientCode {
	case websocket.CloseInternalServerErr, // 1011
		websocket.CloseServiceRestart, // 1012
		websocket.CloseTryAgainLater:  // 1013
		return websocket.CloseGoingAway // 1001
	}
	return clientCode
}

// determineCloseCode picks the appropriate WebSocket close code and text based
// on the shutdown error and any close codes already received from the peers.
//
// Priority:
//  1. Endpoint-initiated close code (e.g. 4000 session expired) → propagate to client.
//  2. Client-initiated close code → propagate to endpoint.
//  3. Error-type mapping for internal shutdowns.
func (b *Bridge) determineCloseCode(err error) (int, string) {
	// A rebind that gave up is the bridge's own verdict about the endpoint
	// side, and it outranks whatever the dead endpoint last said (usually a
	// 1006 it never sent): the client's remedy is to reconnect, so 1012.
	if errors.Is(err, ErrBridgeEndpointUnavailable) {
		return websocket.CloseServiceRestart, "endpoint temporarily unavailable, please reconnect"
	}
	if ep := b.endpointConn.Load(); ep != nil {
		if code, text := ep.GetCloseInfo(); code != 0 {
			b.logger.Info("websocket: propagating endpoint close code to client", "code", code, "text", text)
			return code, text
		}
	}
	if b.clientConn != nil {
		if code, text := b.clientConn.GetCloseInfo(); code != 0 {
			b.logger.Info("websocket: propagating client close code to endpoint", "code", code, "text", text)
			return code, text
		}
	}

	switch {
	case err == nil,
		errors.Is(err, ErrBridgeContextCanceled),
		errors.Is(err, context.Canceled):
		return websocket.CloseServiceRestart, "service restarting, please reconnect"

	case errors.Is(err, ErrBridgeSessionExpired):
		return websocket.CloseServiceRestart, "session ended, please reconnect"

	case errors.Is(err, ErrBridgeEndpointUnavailable):
		return websocket.CloseServiceRestart, "endpoint temporarily unavailable, please reconnect"

	case errors.Is(err, ErrBridgeStalled), errors.Is(err, ErrBridgeReplaceRequested):
		return websocket.CloseServiceRestart, "endpoint replaced, please reconnect"

	case errors.Is(err, ErrBridgePeerUnresponsive):
		// 1012 to the client: reconnecting draws a fresh supplier through
		// selection, which is the only remedy for a supplier that went quiet.
		// (endpointCloseCode turns it into 1001 facing the supplier.)
		return websocket.CloseServiceRestart, "peer unresponsive, please reconnect"

	case errors.Is(err, ErrBridgeMessageProcessing):
		return websocket.CloseInternalServerErr, "message processing error"

	case errors.Is(err, ErrBridgeConnectionFailed):
		return websocket.CloseInternalServerErr, "connection error"

	default:
		return websocket.CloseInternalServerErr, fmt.Sprintf("bridge error: %s", err.Error())
	}
}

// isTimeout reports whether a read error is the read deadline firing: gorilla
// surfaces it as the net.Error from the underlying conn.
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
