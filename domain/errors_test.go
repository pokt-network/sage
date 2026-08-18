package domain

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestRelayError(t *testing.T) {
	cause := errors.New("connection refused")
	err := NewRelayError(ErrTransport, "relay failed", cause, true)

	if err.Kind != ErrTransport {
		t.Errorf("Kind = %v, want ErrTransport", err.Kind)
	}
	if !err.Retryable {
		t.Error("Retryable should be true")
	}
	if !errors.Is(err, cause) {
		t.Error("Unwrap should return cause")
	}
	if err.Error() != "relay failed: connection refused" {
		t.Errorf("Error() = %q", err.Error())
	}
}

func TestIsRetryable(t *testing.T) {
	retryable := NewRelayError(ErrTransport, "timeout", nil, true)
	notRetryable := NewRelayError(ErrValidation, "bad request", nil, false)
	plainErr := errors.New("plain error")

	if !IsRetryable(retryable) {
		t.Error("should be retryable")
	}
	if IsRetryable(notRetryable) {
		t.Error("should not be retryable")
	}
	if IsRetryable(plainErr) {
		t.Error("plain error should not be retryable")
	}
}

func TestErrorKindOf(t *testing.T) {
	re := NewRelayError(ErrRateLimit, "429", nil, true)
	if ErrorKindOf(re) != ErrRateLimit {
		t.Error("should be ErrRateLimit")
	}
	if ErrorKindOf(errors.New("unknown")) != ErrTransport {
		t.Error("unknown errors should default to ErrTransport")
	}
}

// The cause chain belongs in the log, not in a response body. On a gateway
// with no client authentication, err.Error() hands the operator's own
// infrastructure to anyone who can send a request.
func TestClientMessage_DoesNotLeakTheCauseChain(t *testing.T) {
	// What a fullnode dial failure actually looks like on the way out.
	cause := fmt.Errorf(`rpc error: code = Unavailable desc = connection error: dial tcp 10.4.2.17:9090: connect: connection refused`)
	err := NewRelayError(ErrTransport, "failed to get session", cause, true)

	msg := ClientMessage(err)

	for _, secret := range []string{"10.4.2.17", "9090", "dial tcp"} {
		if strings.Contains(msg, secret) {
			t.Errorf("client message %q leaks %q", msg, secret)
		}
	}
	if !strings.Contains(msg, "failed to get session") {
		t.Errorf("client message %q dropped the part SAGE wrote and controls", msg)
	}
	if !strings.Contains(err.Error(), "10.4.2.17") {
		t.Error("Error() no longer carries the cause — the log line needs it")
	}
}

func TestClientMessage_UnknownErrorsAreOpaque(t *testing.T) {
	if got := ClientMessage(fmt.Errorf("dial tcp 10.4.2.17:9090: refused")); got != "relay failed" {
		t.Errorf("ClientMessage on a bare error = %q, want an opaque string", got)
	}
	if got := ClientMessage(nil); got != "" {
		t.Errorf("ClientMessage(nil) = %q, want empty", got)
	}
}

func TestErrorKind_StringIsStable(t *testing.T) {
	// These strings reach clients, so a rename is an API change.
	want := map[ErrorKind]string{
		ErrTransport:  "transport error",
		ErrProtocol:   "protocol error",
		ErrEndpoint:   "endpoint error",
		ErrRateLimit:  "rate limited",
		ErrValidation: "invalid request",
		ErrCapability: "unsupported capability",
	}
	for kind, s := range want {
		if got := kind.String(); got != s {
			t.Errorf("ErrorKind(%d).String() = %q, want %q", kind, got, s)
		}
	}
}
