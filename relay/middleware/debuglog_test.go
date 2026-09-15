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
	"github.com/pokt-network/sage/qos"
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

	mw := DebugLog(flags, nil, nil)
	if err := mw(inner).HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.buf.String()
	for _, want := range []string{"relay_request", "relay_response", "eth_blockNumber", "http_method=POST", "path=/", "error=\"\""} {
		if !strings.Contains(output, want) {
			t.Errorf("expected log output to contain %q, got:\n%s", want, output)
		}
	}
}

// restNormPlugin catalogues one REST route, the way the cosmos plugin does.
type restNormPlugin struct{ normPlugin }

func (restNormPlugin) NormalizeMethod(p domain.Payload) string {
	if strings.HasPrefix(p.Path(), "/cosmos/bank/v1beta1/balances/") {
		return "/cosmos/bank/v1beta1/balances/:var"
	}
	return qos.MethodOther
}

// urlProvider answers the URL an attempt dials per RPC type, the way the
// Shannon protocol does for an operator staking one host per type.
type urlProvider struct{ urls map[domain.RPCType]string }

func (urlProvider) AvailableEndpoints(context.Context, domain.ServiceID, domain.RPCType) (domain.EndpointAddrList, error) {
	return nil, nil
}

func (r urlProvider) EndpointURLFor(_ domain.EndpointAddr, rpcType domain.RPCType) (string, bool) {
	u, ok := r.urls[rpcType]
	return u, ok
}

// A REST attempt logs its verb, path and catalogue name, the URL actually
// dialed for the type rather than the address's primary one, and on failure
// the error text, so a debug capture can be compared with PATH's without
// guessing which host answered or why the attempt ended.
func TestDebugLog_LogsPathVerbNormalizedMethodDialedURLAndError(t *testing.T) {
	buf := &logBuffer{}
	log := buf.logger()

	req, _ := http.NewRequest(http.MethodGet, "http://localhost/cosmos/bank/v1beta1/balances/osmo1abc", nil)
	ctx := makeDebugLogCtx(req, log)
	ctx.ServiceID = "osmosis"
	ctx.RPCType = domain.RPCTypeREST
	ctx.Endpoint = "pokt1abc-https://eu-s-01-osmosis-json.opd.example"
	ctx.Payloads = []domain.Payload{
		domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/cosmos/bank/v1beta1/balances/osmo1abc?pagination.limit=1", "GET"),
	}
	reg := qos.NewRegistry()
	if err := reg.Register("osmosis", restNormPlugin{}); err != nil {
		t.Fatal(err)
	}
	provider := urlProvider{urls: map[domain.RPCType]string{
		domain.RPCTypeJSONRPC: "https://eu-s-01-osmosis-json.opd.example",
		domain.RPCTypeREST:    "https://eu-s-01-osmosis-rest.opd.example",
	}}

	inner := relay.HandlerFunc(func(_ *relay.Context) error {
		return domain.NewRelayError(domain.ErrEndpoint, "upstream endpoint unavailable", &domain.UpstreamStatusError{Status: 502}, true)
	})
	_ = DebugLog(newFlags(featureflag.FlagDebugLog), reg, provider)(inner).HandleRelay(ctx)

	output := buf.buf.String()
	for _, want := range []string{
		"http_method=GET",
		`path="/cosmos/bank/v1beta1/balances/osmo1abc?pagination.limit=1"`,
		"normalized_method=/cosmos/bank/v1beta1/balances/:var ",
		"url=https://eu-s-01-osmosis-rest.opd.example",
		"rpc_type=rest",
		`error="upstream endpoint unavailable: relay miner answered HTTP 502"`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("expected log output to contain %q, got:\n%s", want, output)
		}
	}
	if strings.Contains(output, "url=https://eu-s-01-osmosis-json") {
		t.Errorf("the request line must name the host dialed for REST, not the primary: %s", output)
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

	mw := DebugLog(flags, nil, nil)
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

	_ = DebugLog(flags, nil, nil)(inner).HandleRelay(ctx)

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

	_ = DebugLog(flags, nil, nil)(inner).HandleRelay(ctx)

	output := buf.buf.String()
	if !strings.Contains(output, "http_status") {
		t.Errorf("expected http_status in log, got:\n%s", output)
	}
	if !strings.Contains(output, "response_body") {
		t.Errorf("expected response_body in log, got:\n%s", output)
	}
}
