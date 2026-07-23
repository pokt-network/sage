package middleware

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// trackingShadowWriter wraps mockResponseWriter and records SetShadow calls.
type trackingShadowWriter struct {
	mockResponseWriter
	shadowCalled bool
	shadowValue  bool
}

func (w *trackingShadowWriter) SetShadow(shadow bool) {
	w.shadowCalled = true
	w.shadowValue = shadow
}

func makeShadowCtx() (*relay.Context, *trackingShadowWriter) {
	w := &trackingShadowWriter{}
	ctx := baseContext()
	ctx.Writer = w
	return ctx, w
}

func TestShadow_SetsWriterShadowWhenFlagEnabled(t *testing.T) {
	flags := newFlags(featureflag.FlagShadowMode)

	called := false
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called = true
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	ctx, w := makeShadowCtx()
	err := Shadow(flags)(inner).HandleRelay(ctx)

	if err != nil {
		t.Fatalf("shadow middleware should always return nil, got %v", err)
	}
	if !called {
		t.Error("inner handler was not called — shadow must still send the relay")
	}
	if !w.shadowCalled {
		t.Error("SetShadow was not called on the writer")
	}
	if !w.shadowValue {
		t.Error("SetShadow should be called with true")
	}
}

func TestShadow_ReturnsNilEvenWhenInnerFails(t *testing.T) {
	flags := newFlags(featureflag.FlagShadowMode)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		return domain.NewRelayError(domain.ErrTransport, "backend down", nil, true)
	})

	ctx, _ := makeShadowCtx()
	err := Shadow(flags)(inner).HandleRelay(ctx)

	if err != nil {
		t.Errorf("shadow middleware should suppress inner errors, got %v", err)
	}
}

func TestShadow_PassesThroughWhenFlagDisabled(t *testing.T) {
	flags := newFlags() // shadow_mode NOT enabled

	innerErr := domain.NewRelayError(domain.ErrTransport, "real error", nil, true)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		return innerErr
	})

	ctx, w := makeShadowCtx()
	err := Shadow(flags)(inner).HandleRelay(ctx)

	if err != innerErr {
		t.Errorf("expected inner error to propagate when shadow disabled, got %v", err)
	}
	if w.shadowCalled {
		t.Error("SetShadow should NOT be called when shadow_mode flag is disabled")
	}
}

func TestShadow_InnerHandlerReceivesContext(t *testing.T) {
	flags := newFlags(featureflag.FlagShadowMode)

	var capturedServiceID domain.ServiceID
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		capturedServiceID = ctx.ServiceID
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	ctx, _ := makeShadowCtx()
	ctx.ServiceID = "poly"
	_ = Shadow(flags)(inner).HandleRelay(ctx)

	if capturedServiceID != "poly" {
		t.Errorf("expected inner handler to see ServiceID %q, got %q", "poly", capturedServiceID)
	}
}
