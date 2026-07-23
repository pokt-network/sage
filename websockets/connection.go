package websockets

import (
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// maxMessageBytes caps a single inbound WebSocket frame from either peer.
	//
	// gorilla buffers a whole frame in memory before returning it, so with no
	// limit one oversized frame OOMs the process — and either side can send it:
	// a malicious client, or a supplier we do not control.
	//
	// 15MB matches go-ethereum's wsMessageSizeLimit, which is what bounds the
	// other end of these connections in practice. A geth node will not send a
	// frame larger than this, so the cap cannot cut off legitimate traffic —
	// including the large eth_getLogs responses that are the reason not to pick
	// a smaller number. Exceeding it makes ReadMessage return an error, which
	// tears the bridge down through the existing path.
	maxMessageBytes = 15 * 1024 * 1024

	// writeWait bounds how long a single data-frame write may block.
	//
	// gorilla's WriteMessage blocks indefinitely once the peer stops reading and
	// its TCP receive buffer fills. The bridge writes from its single routing
	// goroutine and does not select on ctx while doing so, so one stalled reader
	// wedges that goroutine — and the connection's resources with it — until the
	// OS-level TCP timeout, which can be many minutes. The deadline turns that
	// into a fast, ordinary write error.
	writeWait = 10 * time.Second
)

// MessageSource identifies which side of the bridge a message originates from.
type MessageSource int

const (
	SourceClient   MessageSource = iota
	SourceEndpoint MessageSource = iota
)

// message is an internal envelope routing data through the bridge's msgChan.
type message struct {
	source      MessageSource
	messageType int
	data        []byte
}

// Connection wraps a gorilla websocket.Conn with:
//   - write mutex (gorilla is not concurrent-write safe)
//   - thread-safe close code storage for bidirectional propagation
//   - labelled source for logging and routing
type Connection struct {
	conn   *websocket.Conn
	logger *slog.Logger
	source MessageSource

	writeMu sync.Mutex

	closeInfoMu   sync.Mutex
	lastCloseCode int
	lastCloseText string
}

// NewConnection creates a Connection wrapper. It does not start any goroutines.
func NewConnection(conn *websocket.Conn, source MessageSource, logger *slog.Logger) *Connection {
	// Bound inbound frames on both sides: a client and a supplier are equally
	// capable of sending one large enough to exhaust memory.
	conn.SetReadLimit(maxMessageBytes)

	return &Connection{
		conn:   conn,
		logger: logger,
		source: source,
	}
}

// ReadMessage reads the next message from the underlying WebSocket connection.
// It is safe to call from a single goroutine; do not call concurrently.
func (c *Connection) ReadMessage() (messageType int, data []byte, err error) {
	return c.conn.ReadMessage()
}

// WriteMessage writes a data message to the connection, bounded by writeWait.
// It is safe to call from multiple goroutines.
//
// The deadline covers only data frames; WriteControl callers pass their own.
func (c *Connection) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return c.conn.WriteMessage(messageType, data)
}

// WriteControl writes a WebSocket control frame (e.g. close, ping, pong).
// It is safe to call concurrently with WriteMessage.
func (c *Connection) WriteControl(messageType int, data []byte, deadline time.Time) error {
	// gorilla's WriteControl acquires its own internal lock, but we serialise
	// with WriteMessage for correctness on all gorilla versions.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(messageType, data, deadline)
}

// Close closes the underlying network connection immediately.
func (c *Connection) Close() error {
	return c.conn.Close()
}

// GetCloseInfo returns the close code and text captured from the last read error.
// Returns 0 and empty string when no close frame has been received.
// It is safe to call from multiple goroutines.
func (c *Connection) GetCloseInfo() (code int, text string) {
	c.closeInfoMu.Lock()
	defer c.closeInfoMu.Unlock()
	return c.lastCloseCode, c.lastCloseText
}

// SetCloseInfo records the close code and text, typically extracted from a
// *websocket.CloseError. It is safe to call from multiple goroutines.
func (c *Connection) SetCloseInfo(code int, text string) {
	c.closeInfoMu.Lock()
	defer c.closeInfoMu.Unlock()
	c.lastCloseCode = code
	c.lastCloseText = text
}

// extractCloseInfo attempts to pull a close code and text out of an error returned
// by ReadMessage. Returns 0,"" when the error is not a close error.
func extractCloseInfo(err error) (int, string) {
	var closeErr *websocket.CloseError
	if websocket.IsCloseError(err, websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseProtocolError,
		websocket.CloseUnsupportedData,
		websocket.CloseNoStatusReceived,
		websocket.CloseAbnormalClosure,
		websocket.CloseInvalidFramePayloadData,
		websocket.ClosePolicyViolation,
		websocket.CloseMessageTooBig,
		websocket.CloseMandatoryExtension,
		websocket.CloseInternalServerErr,
		websocket.CloseServiceRestart,
		websocket.CloseTryAgainLater,
		websocket.CloseTLSHandshake,
	) {
		// Use type assertion to get code/text after confirming it is a close error.
		if ce, ok := err.(*websocket.CloseError); ok {
			closeErr = ce
		}
	} else if ce, ok := err.(*websocket.CloseError); ok {
		// Non-standard close codes (e.g. 4000 for session expiry)
		closeErr = ce
	}
	if closeErr != nil {
		return closeErr.Code, closeErr.Text
	}
	return 0, ""
}
