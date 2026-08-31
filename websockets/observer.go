package websockets

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
}

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
