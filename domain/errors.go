package domain

import (
	"errors"
	"fmt"
)

// ErrorKind categorizes relay errors for typed handling.
// Circuit breaker, retry, and reputation logic switch on ErrorKind
// instead of string matching.
type ErrorKind int

// The error kinds a relay attempt can fail with. Retry, circuit-breaking, and
// reputation branch on these rather than on message text, so rewording an
// error never silently changes routing.
const (
	ErrTransport  ErrorKind = iota // network/timeout/dial
	ErrProtocol                    // Shannon signing/verification
	ErrEndpoint                    // upstream returned error
	ErrRateLimit                   // 429
	ErrValidation                  // bad client request
	ErrCapability                  // archival not supported, etc.
)

// ErrRetryVerdict is the cause carried by the error the heuristic middleware
// returns when a response came back but is worth trying another supplier for.
// It is an error only so that Retry goes again: a response exists, and if no
// further attempt does better, that response — the chain's own `execution
// reverted`, the node's `block not found` — is what the client should get,
// not a gateway-made failure. The router tells the two apart by this.
var ErrRetryVerdict = errors.New("heuristic verdict: retry")

// ErrEndpointsStale is the cause carried when a selected endpoint is not in the
// session current at send time — the session rolled over between selection and
// send. It is nobody's fault: no relay reached the supplier, and the client
// did nothing wrong. Retry keys on it to reselect from the fresh session
// instead of trying the same stale list's other members, and scoring keys on
// it to record no signal.
var ErrEndpointsStale = errors.New("endpoints stale: session rolled over")

// ErrResponseTooLarge is the cause carried when a supplier's response exceeds
// the gateway's response ceiling. The response is not read past the ceiling:
// a signed relay has to be held whole to be verified, so an unbounded one is
// an unbounded allocation — one 1 GB answer OOM-killed a mainnet pod on
// 2026-09-15. It is the request's size, not the supplier's fault, and another
// supplier would send the same bytes, so it is neither scored nor retried.
var ErrResponseTooLarge = errors.New("upstream response too large")

// ErrRPCTypeUnsupported is the cause Validate attaches when a request's RPC
// type is one the service does not declare. The router counts it in
// sage_rpc_type_mismatch_total{reason="unsupported"}: a request that was
// classified as something the service cannot serve is either a client
// mistake or a detection rule to fix, and the counter is how the second
// is noticed.
var ErrRPCTypeUnsupported = errors.New("rpc type not declared by service")

// ClientMessage is what may be shown to the caller of a relay.
//
// Error() renders the whole cause chain, which is right for a log line and
// wrong for a response body: the chain routinely carries the operator's own
// infrastructure — a dial failure names the fullnode's host and port, a
// transport error names the supplier URL and the local address it came from.
// None of that is the client's business, and on a gateway with no client
// authentication it is handed to anyone who can send a request.
//
// The Message is written by SAGE and is safe by construction; the Cause is
// whatever the network or a library produced. So the message is returned alone,
// with the kind for context, and the chain stays in the log where it is useful.
func ClientMessage(err error) string {
	if err == nil {
		return ""
	}
	var re *RelayError
	if errors.As(err, &re) && re.Message != "" {
		return re.Kind.String() + ": " + re.Message
	}
	return "relay failed"
}

// String names the kind for a client-facing message. Values are stable — they
// are part of the response body clients see.
func (k ErrorKind) String() string {
	switch k {
	case ErrTransport:
		return "transport error"
	case ErrProtocol:
		return "protocol error"
	case ErrEndpoint:
		return "endpoint error"
	case ErrRateLimit:
		return "rate limited"
	case ErrValidation:
		return "invalid request"
	case ErrCapability:
		return "unsupported capability"
	default:
		return "relay error"
	}
}

// RelayError is a typed error from a relay attempt.
type RelayError struct {
	Kind      ErrorKind
	Message   string
	Cause     error
	Retryable bool
}

func (e *RelayError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *RelayError) Unwrap() error { return e.Cause }

// NewRelayError creates a new RelayError.
func NewRelayError(kind ErrorKind, msg string, cause error, retryable bool) *RelayError {
	return &RelayError{Kind: kind, Message: msg, Cause: cause, Retryable: retryable}
}

// ConnectError marks a transport failure that happened before any connection
// to the host was obtained: the dial never completed, the name did not
// resolve, the TLS handshake failed. It is set by the protocol layer from an
// httptrace observation rather than inferred from the error's shape, because
// the shape does not carry the fact: a host that drops SYNs and a host that
// accepted and went quiet both surface as the same http timeout once
// Client.Timeout fires. The heuristic keys on this to grade a dead host as
// dead (circuit breaker) rather than as slow on one method (method block).
type ConnectError struct {
	Cause error
}

func (e *ConnectError) Error() string { return "no connection to host: " + e.Cause.Error() }

// Unwrap exposes the underlying transport error, so errors.Is on
// context.DeadlineExceeded or a net.Error still sees through it.
func (e *ConnectError) Unwrap() error { return e.Cause }

// IsRetryable returns true if the error indicates the request can be retried.
func IsRetryable(err error) bool {
	if re, ok := err.(*RelayError); ok {
		return re.Retryable
	}
	return false
}


// UpstreamStatusError is the cause carried when a relay miner's HTTP layer
// answered with a non-2xx status before producing a signed RelayResponse:
// its own 502 or 503, a 413 for a payload it will not take, a 429. The body
// is the miner's error page, not a relay, so the status is the whole fact.
type UpstreamStatusError struct {
	Status int
}

func (e *UpstreamStatusError) Error() string {
	return fmt.Sprintf("relay miner answered HTTP %d", e.Status)
}
