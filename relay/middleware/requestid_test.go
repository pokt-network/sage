package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

// decodeLogLines parses newline-delimited JSON log records.
func decodeLogLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

func TestWithRequestID_StampsEveryRecord(t *testing.T) {
	var buf bytes.Buffer
	logger := withRequestID(slog.New(slog.NewJSONHandler(&buf, nil)), "req-1")

	logger.Info("first")
	logger.Error("second")

	records := decodeLogLines(t, &buf)
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	for i, rec := range records {
		if rec["request_id"] != "req-1" {
			t.Errorf("record %d request_id = %v, want req-1", i, rec["request_id"])
		}
	}
}

// A middleware further down the chain may add its own attributes or a group.
// Neither may drop the request ID.
func TestWithRequestID_SurvivesWithAttrsAndGroup(t *testing.T) {
	var buf bytes.Buffer
	base := withRequestID(slog.New(slog.NewJSONHandler(&buf, nil)), "req-2")

	base.With("service_id", "eth").Info("attrs")
	base.WithGroup("inner").Info("group", "k", "v")

	records := decodeLogLines(t, &buf)
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0]["request_id"] != "req-2" || records[0]["service_id"] != "eth" {
		t.Errorf("With() record lost an attribute: %v", records[0])
	}
	if records[1]["request_id"] != "req-2" {
		t.Errorf("WithGroup() record lost the request ID: %v", records[1])
	}
	// The group must still apply to attributes added after it.
	inner, ok := records[1]["inner"].(map[string]any)
	if !ok || inner["k"] != "v" {
		t.Errorf("WithGroup() did not group later attributes: %v", records[1])
	}
}

func TestWithRequestID_RespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := withRequestID(
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		"req-3",
	)

	logger.Info("dropped")
	if buf.Len() != 0 {
		t.Errorf("below-threshold record was emitted: %s", buf.String())
	}

	logger.Warn("kept")
	if records := decodeLogLines(t, &buf); len(records) != 1 || records[0]["request_id"] != "req-3" {
		t.Errorf("warn record = %v", records)
	}
}

func TestWithRequestID_NilLogger(t *testing.T) {
	if got := withRequestID(nil, "req-4"); got != nil {
		t.Errorf("want nil logger passed through, got %v", got)
	}
}

// loggerSink keeps the benchmark results alive: without an escaping use the
// compiler elides the allocation being measured and reports a fictional 0 B/op.
var loggerSink *slog.Logger

// BenchmarkRequestIDLogger measures the per-relay cost of attaching the ID at a
// production log level, where nothing is emitted.
func BenchmarkRequestIDLogger(b *testing.B) {
	base := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))

	b.Run("slog_with", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			loggerSink = base.With("request_id", "0123456789abcdef0123456789abcdef")
		}
	})
	b.Run("wrapped_handler", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			loggerSink = withRequestID(base, "0123456789abcdef0123456789abcdef")
		}
	})
}
