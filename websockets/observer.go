package websockets

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
)

// Observer is told what a bridge does, for metrics. Every method may be called
// from any bridge goroutine; implementations must be safe for that. A nil
// Observer is allowed everywhere and observes nothing.
//
// It is an interface rather than a metrics.Recorder so this package stays
// free of Prometheus: the bridge knows when a frame moved and who closed,
// and nothing else.
type Observer interface {
	// Opened is called once both sides of a bridge are connected.
	Opened()
	// Frame is called for every data frame routed, after processing, with
	// the side it came from and its size on the wire.
	Frame(source MessageSource, bytes int)
	// Unresponsive is called when a side sent nothing for a whole pong wait.
	Unresponsive(source MessageSource)
	// Closed is called once per bridge, when it shuts down, with who ended it
	// and the close code the client was sent.
	Closed(initiator CloseInitiator, code int)
	// Rebound is called after each attempt to replace a lost endpoint.
	Rebound(result RebindResult)
	// Stalled is called when the stall detector fires: subscriptions are
	// established and nothing has arrived for them in too long.
	Stalled()
}

// RebindResult is the outcome of one attempt to replace a lost endpoint.
type RebindResult string

// The three ways a rebind attempt ends.
const (
	// RebindOK: a new endpoint is serving the same client connection.
	RebindOK RebindResult = "ok"
	// RebindFailed: the handler could not supply one; the bridge closed.
	RebindFailed RebindResult = "failed"
	// RebindExhausted: the limit was already spent; the bridge closed.
	RebindExhausted RebindResult = "exhausted"
)

// CloseInitiator names who ended a bridge.
type CloseInitiator string

// The three parties that can end a bridge.
const (
	InitiatorClient   CloseInitiator = "client"
	InitiatorEndpoint CloseInitiator = "endpoint"
	InitiatorGateway  CloseInitiator = "gateway"
)

// String implements fmt.Stringer for log fields.
func (s MessageSource) String() string {
	switch s {
	case SourceClient:
		return "client"
	case SourceEndpoint:
		return "endpoint"
	}
	return "unknown"
}

// WithObserver attaches an Observer to a bridge.
func WithObserver(o Observer) BridgeOption {
	return func(b *Bridge) { b.observer = o }
}

// EndpointLostHandler replaces a lost endpoint. It returns the connection to
// the new endpoint, the processor to use with it (signing is per supplier,
// so the old one cannot be reused), and the raw client frames to replay to
// it — a subscription registry's ReplayFrames. An error means there is
// nowhere to go; the bridge then closes as it would have without a handler.
type EndpointLostHandler func(ctx context.Context, cause error) (*websocket.Conn, MessageProcessor, [][]byte, error)

// WithEndpointLost installs the handler the bridge calls instead of shutting
// down when the ENDPOINT side is lost. A client-side loss is never a rebind.
func WithEndpointLost(h EndpointLostHandler) BridgeOption {
	return func(b *Bridge) { b.endpointLost = h }
}

// WithRebindLimit caps how many times one bridge may replace its endpoint.
// Past it the next loss closes the bridge. Default defaultRebindLimit.
func WithRebindLimit(n int) BridgeOption {
	return func(b *Bridge) { b.rebindLimit = n }
}

// WithStallDetector installs a data-staleness check, polled every period.
// When it reports true the endpoint is treated as lost: a rebind with a
// handler installed, 1012 to the client without one. The detector decides
// what "stalled" means — typically "has live subscriptions and no data for
// a while" from a subscription registry — so the bridge stays chain-agnostic.
func WithStallDetector(stalled func() bool, period time.Duration) BridgeOption {
	return func(b *Bridge) {
		b.stalled = stalled
		b.stallPeriod = period
	}
}
