package domain

import (
	"errors"
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
