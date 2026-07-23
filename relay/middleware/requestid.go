package middleware

import (
	"crypto/rand"
	"encoding/hex"

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
			ctx.Logger = ctx.Logger.With("request_id", id)

			err := next.HandleRelay(ctx)

			// Write ID to response so callers can correlate.
			if ctx.Writer != nil {
				ctx.Writer.SetHeader(requestIDHeader, id)
			}

			return err
		})
	}
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
