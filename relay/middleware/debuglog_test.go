package middleware

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// logBuffer captures slog output for assertions.
type logBuffer struct {
	buf bytes.Buffer
}

func (b *logBuffer) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&b.buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func makeDebugLogCtx(req *http.Request, log *slog.Logger) *relay.Context {
	w := &mockResponseWriter{}
	ctx := relay.NewContext(context.Background(), req, log, w)
	ctx.ServiceID = domain.ServiceID("eth")
	return ctx
}

func TestDebugLog_LogsRequestAndResponseWhenEnabled(t *testing.T) {
	buf := &logBuffer{}
	log := buf.logger()

	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	ctx := makeDebugLogCtx(req, log)
	ctx.Payloads = []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}

	flags := newFlags(featureflag.FlagDebugLog)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Endpoint = "supplierA-https://node.example.com"
		ctx.Response = &domain.Response{
			HTTPStatusCode: http.StatusOK,
			Body:           []byte(`{"result":"0x100"}`),
		}
		return nil
	})

	mw := DebugLog(flags)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.buf.String()
	for _, want := range []string{"relay_request", "relay_response", "eth_blockNumber"} {
		if !strings.Contains(output, want) {
			t.Errorf("expected log output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestDebugLog_PassesThroughWhenFlagDisabled(t *testing.T) {
	buf := &logBuffer{}
	log := buf.logger()

	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	ctx := makeDebugLogCtx(req, log)

	flags := newFlags() // debug_log NOT enabled
	called := false
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		called = true
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	mw := DebugLog(flags)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !called {
		t.Error("inner handler should be called even when debug_log is disabled")
	}

	output := buf.buf.String()
	if strings.Contains(output, "relay_request") || strings.Contains(output, "relay_response") {
		t.Errorf("expected no log output when flag disabled, got:\n%s", output)
	}
}

func TestDebugLog_LogsAtDebugLevel(t *testing.T) {
	// With level=Info handler, debug messages should be suppressed.
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	ctx := makeDebugLogCtx(req, log)

	flags := newFlags(featureflag.FlagDebugLog)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: http.StatusOK}
		return nil
	})

	_ = DebugLog(flags)(inner).HandleRelay(ctx)

	// Nothing should be logged because the handler only accepts Info+.
	if strings.Contains(buf.String(), "relay_request") {
		t.Errorf("debug messages should be suppressed at Info level, got:\n%s", buf.String())
	}
}

func TestDebugLog_LogsResponseBodyAndStatus(t *testing.T) {
	buf := &logBuffer{}
	log := buf.logger()

	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	ctx := makeDebugLogCtx(req, log)

	flags := newFlags(featureflag.FlagDebugLog)
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{
			HTTPStatusCode: 200,
			Body:           []byte(`{"result":"0xdeadbeef"}`),
		}
		return nil
	})

	_ = DebugLog(flags)(inner).HandleRelay(ctx)

	output := buf.buf.String()
	if !strings.Contains(output, "http_status") {
		t.Errorf("expected http_status in log, got:\n%s", output)
	}
	if !strings.Contains(output, "response_body") {
		t.Errorf("expected response_body in log, got:\n%s", output)
	}
}
