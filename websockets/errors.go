package websockets

import "errors"

// Bridge shutdown error types used to determine appropriate WebSocket close codes.
var (
	// ErrBridgeContextCanceled indicates the bridge was shut down due to context cancellation.
	// This typically happens during graceful shutdown or when the parent context is canceled.
	ErrBridgeContextCanceled = errors.New("bridge context canceled")

	// ErrBridgeEndpointUnavailable indicates the bridge was shut down because the endpoint
	// became unavailable or could not be reached.
	ErrBridgeEndpointUnavailable = errors.New("endpoint unavailable")

	// ErrBridgeMessageProcessing indicates the bridge was shut down due to a message
	// processing failure in the MessageProcessor.
	ErrBridgeMessageProcessing = errors.New("message processing failed")

	// ErrBridgeConnectionFailed indicates the bridge was shut down due to a connection-level
	// failure such as a write error or unexpected network drop.
	ErrBridgeConnectionFailed = errors.New("connection failed")

	// ErrBridgeClientUpgradeFailed indicates the incoming HTTP connection could not be
	// upgraded to a WebSocket connection.
	ErrBridgeClientUpgradeFailed = errors.New("client upgrade failed")

	// ErrBridgeSessionExpired indicates the Shannon session backing the
	// bridge has crossed its SessionEndBlockHeight and the bridge is being
	// closed so the client can reconnect with a fresh session.
	ErrBridgeSessionExpired = errors.New("session expired")
	// ErrBridgePeerUnresponsive indicates one side sent nothing — no data, no
	// pong — for a whole pong wait. The socket may still look open; the peer
	// behind it is gone.
	ErrBridgePeerUnresponsive = errors.New("peer unresponsive")
)
