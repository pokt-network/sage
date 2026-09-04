package middleware_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func tracedCtx(buf *bytes.Buffer) *relay.Context {
	req := newPOSTRequest("/v1", `{}`)
	ctx := relay.NewContext(context.Background(), req, slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &mockWriter{})
	ctx.ServiceID = "eth"
	ctx.RequestID = "req-1"
	return ctx
}

func TestTracing_FlagOn_LogsStartAndEndWithDuration(t *testing.T) {
	var buf bytes.Buffer
	ctx := tracedCtx(&buf)
	flags := featureflag.NewMemoryStore(map[string]bool{featureflag.FlagTracing: true})
	next := relay.HandlerFunc(func(c *relay.Context) error { c.Endpoint = "ep1"; return nil })
	if err := middleware.Tracing(flags)(next).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"relay_start", "relay_end", "request_id=req-1", "service_id=eth", "endpoint=ep1", "duration_ms="} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %s", want, out)
		}
	}
}

func TestTracing_FlagOff_LogsNothing(t *testing.T) {
	var buf bytes.Buffer
	ctx := tracedCtx(&buf)
	flags := featureflag.NewMemoryStore(map[string]bool{featureflag.FlagTracing: false})
	called := false
	next := relay.HandlerFunc(func(*relay.Context) error { called = true; return nil })
	if err := middleware.Tracing(flags)(next).HandleRelay(ctx); err != nil || !called {
		t.Fatalf("err=%v called=%v", err, called)
	}
	if buf.Len() != 0 {
		t.Fatalf("tracing off must log nothing: %s", buf.String())
	}
	// A nil store is off too, not a panic.
	if err := middleware.Tracing(nil)(next).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
}
