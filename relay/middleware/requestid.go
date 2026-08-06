package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"

	"github.com/pokt-network/sage/relay"
)

const (
	requestIDHeader = "X-Request-ID"
)

// RequestID returns a middleware that ensures every relay has a unique request ID.
// If the incoming HTTP request carries an X-Request-ID header, that value is
// reused so callers can correlate logs end-to-end. Otherwise a new 32-char
// hex ID is generated using crypto/rand (16 random bytes).
//
// The ID is stored on ctx.RequestID, added to ctx.Logger, and written back
// to the client as an X-Request-ID response header after the inner handler
// completes.
func RequestID() relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Reuse an existing ID supplied by the caller, if any.
			id := ""
			if ctx.HTTPRequest != nil {
				id = ctx.HTTPRequest.Header.Get(requestIDHeader)
			}

			if id == "" {
				id = generateRequestID()
			}

			ctx.RequestID = id
			ctx.Logger = withRequestID(ctx.Logger, id)

			err := next.HandleRelay(ctx)

			// Write ID to response so callers can correlate.
			if ctx.Writer != nil {
				ctx.Writer.SetHeader(requestIDHeader, id)
			}

			return err
		})
	}
}

// withRequestID returns a logger that stamps request_id on every record.
//
// slog.Logger.With would do the same, but it pre-formats the attribute into a
// clone of the handler's buffer immediately — work paid on every relay, whether
// or not a single line is ever emitted at the configured level. On a gateway
// that logs at warn in production, that is the common case. requestIDHandler
// defers the attribute to Handle, which only runs for records that survive the
// level check.
func withRequestID(logger *slog.Logger, id string) *slog.Logger {
	if logger == nil {
		return nil
	}
	return slog.New(&requestIDHandler{Handler: logger.Handler(), id: id})
}

// requestIDHandler adds request_id to each record as it is handled.
type requestIDHandler struct {
	slog.Handler
	id string
}

func (h *requestIDHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(slog.String("request_id", h.id))
	return h.Handler.Handle(ctx, r)
}

// WithAttrs and WithGroup must rewrap: the embedded handler's versions return
// the inner handler, which would drop the request ID from that point on.
func (h *requestIDHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &requestIDHandler{Handler: h.Handler.WithAttrs(attrs), id: h.id}
}

func (h *requestIDHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	// Deferring past an open group would nest the ID inside it — records would
	// carry inner.request_id instead of request_id, which is not what
	// slog.Logger.With does and not what log searches expect. Materialize it
	// here, at the ungrouped level, and stop deferring. Nothing on the relay
	// hot path opens a group, so the eager cost is not paid per relay.
	return h.Handler.
		WithAttrs([]slog.Attr{slog.String("request_id", h.id)}).
		WithGroup(name)
}

// generateRequestID produces a cryptographically random 32-character hex string.
func generateRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Extremely unlikely; fall back to a fixed sentinel so callers can detect it.
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}
