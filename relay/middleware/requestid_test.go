package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// mockResponseWriter records SetHeader calls for assertions.
type mockResponseWriter struct {
	headers map[string]string
	shadow  bool
}

func (m *mockResponseWriter) SetHeader(key, value string) {
	if m.headers == nil {
		m.headers = make(map[string]string)
	}
	m.headers[key] = value
}

func (m *mockResponseWriter) SetStatusCode(_ int) {}

func (m *mockResponseWriter) Write(_ []byte) error { return nil }

func (m *mockResponseWriter) SetShadow(s bool) { m.shadow = s }

// makeRequestIDCtx builds a minimal Context with an HTTP request.
func makeRequestIDCtx(r *http.Request) *relay.Context {
	w := &mockResponseWriter{}
	ctx := relay.NewContext(context.Background(), r, slog.Default(), w)
	ctx.ServiceID = domain.ServiceID("eth")
	return ctx
}

func TestRequestID_GeneratesID(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	ctx := makeRequestIDCtx(req)

	called := false
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called = true
		if ctx.RequestID == "" {
			t.Error("expected RequestID to be set, got empty string")
		}
		if len(ctx.RequestID) != 32 {
			t.Errorf("expected 32-char hex ID, got %q (len=%d)", ctx.RequestID, len(ctx.RequestID))
		}
		return nil
	})

	mw := RequestID()
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("inner handler was not called")
	}
}

func TestRequestID_PreservesIncomingID(t *testing.T) {
	const existingID = "abc123def456abc123def456abc12345"
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	req.Header.Set(requestIDHeader, existingID)

	ctx := makeRequestIDCtx(req)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		if ctx.RequestID != existingID {
			t.Errorf("expected RequestID %q, got %q", existingID, ctx.RequestID)
		}
		return nil
	})

	mw := RequestID()
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRequestID_SetsResponseHeader(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	ctx := makeRequestIDCtx(req)

	inner := relay.HandlerFunc(func(ctx *relay.Context) error { return nil })

	mw := RequestID()
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	w := ctx.Writer.(*mockResponseWriter)
	if w.headers[requestIDHeader] == "" {
		t.Error("expected X-Request-ID response header to be set")
	}
	if w.headers[requestIDHeader] != ctx.RequestID {
		t.Errorf("response header %q does not match ctx.RequestID %q",
			w.headers[requestIDHeader], ctx.RequestID)
	}
}

func TestRequestID_SetsResponseHeader_WithExistingID(t *testing.T) {
	const existingID = "deadbeefdeadbeefdeadbeefdeadbeef"
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	req.Header.Set(requestIDHeader, existingID)

	ctx := makeRequestIDCtx(req)
	inner := relay.HandlerFunc(func(*relay.Context) error { return nil })

	mw := RequestID()
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	w := ctx.Writer.(*mockResponseWriter)
	if w.headers[requestIDHeader] != existingID {
		t.Errorf("expected response header %q, got %q", existingID, w.headers[requestIDHeader])
	}
}

func TestRequestID_GeneratesUniqueIDs(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
		ctx := makeRequestIDCtx(req)
		inner := relay.HandlerFunc(func(*relay.Context) error { return nil })
		_ = RequestID()(inner).HandleRelay(ctx)
		if seen[ctx.RequestID] {
			t.Fatalf("duplicate request ID generated: %q", ctx.RequestID)
		}
		seen[ctx.RequestID] = true
	}
}
