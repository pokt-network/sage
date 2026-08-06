package domain

import "fmt"

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

// IsRetryable returns true if the error indicates the request can be retried.
func IsRetryable(err error) bool {
	if re, ok := err.(*RelayError); ok {
		return re.Retryable
	}
	return false
}

// ErrorKindOf extracts the ErrorKind from an error, defaulting to ErrTransport.
func ErrorKindOf(err error) ErrorKind {
	if re, ok := err.(*RelayError); ok {
		return re.Kind
	}
	return ErrTransport
}
