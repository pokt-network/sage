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
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageProcessor transforms messages before forwarding them across the bridge.
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

	clientConn   *Connection
	endpointConn *Connection

	processor MessageProcessor
	msgChan   chan message

	shutdownOnce sync.Once
	done         chan struct{}
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
		ctx:          bridgeCtx,
		cancelCtx:    cancelCtx,
		logger:       logger,
		clientConn:   NewConnection(rawClient, SourceClient, logger.With("conn", "client")),
		endpointConn: NewConnection(rawEndpoint, SourceEndpoint, logger.With("conn", "endpoint")),
		processor:    processor,
		msgChan:      make(chan message, 32),
		done:         make(chan struct{}),
	}

	go b.run()
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
		closeMsg := websocket.FormatCloseMessage(closeCode, closeText)
		deadline := time.Now().Add(time.Second)

		for _, conn := range []*Connection{b.clientConn, b.endpointConn} {
			if conn == nil {
				continue
			}
			if writeErr := conn.WriteControl(websocket.CloseMessage, closeMsg, deadline); writeErr != nil {
				b.logger.Warn("websocket: could not send close frame", "err", writeErr)
			}
			_ = conn.Close()
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
	go b.readLoop(b.clientConn)
	go b.readLoop(b.endpointConn)

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
	switch msg.source {
	case SourceClient:
		processed, err := b.processor.ProcessClientMessage(msg.data)
		if err != nil {
			b.logger.Error("websocket: client message processing failed", "err", err)
			b.Shutdown(fmt.Errorf("%w: %w", ErrBridgeMessageProcessing, err))
			return
		}
		if writeErr := b.endpointConn.WriteMessage(msg.messageType, processed); writeErr != nil {
			b.logger.Error("websocket: write to endpoint failed", "err", writeErr)
			b.Shutdown(fmt.Errorf("%w: write to endpoint: %w", ErrBridgeConnectionFailed, writeErr))
		}

	case SourceEndpoint:
		processed, err := b.processor.ProcessEndpointMessage(msg.data)
		if err != nil {
			b.logger.Error("websocket: endpoint message processing failed", "err", err)
			b.Shutdown(fmt.Errorf("%w: %w", ErrBridgeMessageProcessing, err))
			return
		}
		if writeErr := b.clientConn.WriteMessage(msg.messageType, processed); writeErr != nil {
			b.logger.Error("websocket: write to client failed", "err", writeErr)
			b.Shutdown(fmt.Errorf("%w: write to client: %w", ErrBridgeConnectionFailed, writeErr))
		}
	}
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
			} else {
				b.logger.Warn("websocket: read error", "source", conn.source, "err", err)
			}
			b.Shutdown(fmt.Errorf("%w: read from %v: %w", ErrBridgeConnectionFailed, conn.source, err))
			return
		}

		// Send to msgChan, but bail out if the context has been canceled to avoid
		// sending on a channel that is no longer being drained.
		select {
		case b.msgChan <- message{source: conn.source, messageType: msgType, data: data}:
		case <-b.ctx.Done():
			return
		}
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

// determineCloseCode picks the appropriate WebSocket close code and text based
// on the shutdown error and any close codes already received from the peers.
//
// Priority:
//  1. Endpoint-initiated close code (e.g. 4000 session expired) → propagate to client.
//  2. Client-initiated close code → propagate to endpoint.
//  3. Error-type mapping for internal shutdowns.
func (b *Bridge) determineCloseCode(err error) (int, string) {
	if b.endpointConn != nil {
		if code, text := b.endpointConn.GetCloseInfo(); code != 0 {
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

	case errors.Is(err, ErrBridgeMessageProcessing):
		return websocket.CloseInternalServerErr, "message processing error"

	case errors.Is(err, ErrBridgeConnectionFailed):
		return websocket.CloseInternalServerErr, "connection error"

	default:
		return websocket.CloseInternalServerErr, fmt.Sprintf("bridge error: %s", err.Error())
	}
}
