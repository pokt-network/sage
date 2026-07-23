package middleware

import (
	"errors"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

func timeoutCfg(d time.Duration) func(domain.ServiceID) time.Duration {
	return func(_ domain.ServiceID) time.Duration { return d }
}

func TestTimeout_WithinDeadline_Succeeds(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Timeout(timeoutCfg(100 * time.Millisecond))
	h := mw(inner)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected no error within deadline, got %v", err)
	}
}

func TestTimeout_DeadlineExceeded_ReturnsError(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// Block until context is done.
		<-ctx.Ctx.Done()
		return ctx.Ctx.Err()
	})

	mw := Timeout(timeoutCfg(10 * time.Millisecond))
	h := mw(inner)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error when deadline exceeded")
	}

	var re *domain.RelayError
	if !errors.As(err, &re) {
		t.Fatalf("expected *domain.RelayError, got %T: %v", err, err)
	}
	if re.Kind != domain.ErrTransport {
		t.Errorf("expected ErrTransport kind, got %v", re.Kind)
	}
}

func TestTimeout_DeadlineExceeded_IsRetryable(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		<-ctx.Ctx.Done()
		return ctx.Ctx.Err()
	})

	mw := Timeout(timeoutCfg(10 * time.Millisecond))
	h := mw(inner)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if !domain.IsRetryable(err) {
		t.Error("timeout error should be retryable so it can be tried on another endpoint")
	}
}

func TestTimeout_ZeroDuration_PassesThrough(t *testing.T) {
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200}
		return nil
	})

	mw := Timeout(timeoutCfg(0))
	h := mw(inner)

	ctx := baseContext()
	if err := h.HandleRelay(ctx); err != nil {
		t.Fatalf("expected pass-through with zero timeout, got %v", err)
	}
}

func TestTimeout_InnerErrorPropagated(t *testing.T) {
	innerErr := retryableErr("inner error")
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		return innerErr
	})

	mw := Timeout(timeoutCfg(100 * time.Millisecond))
	h := mw(inner)

	ctx := baseContext()
	err := h.HandleRelay(ctx)
	if !errors.Is(err, innerErr) {
		t.Errorf("expected inner error %v, got %v", innerErr, err)
	}
}
