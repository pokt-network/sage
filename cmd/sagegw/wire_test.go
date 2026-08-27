package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/tuning"
)

// freshRegistry points the default Prometheus registerer at an empty registry
// for one test.
//
// Build registers its collectors on the global registerer, so it is a
// once-per-process call by construction — which is correct for a gateway binary
// and fatal for a test binary that needs to call it repeatedly. Swapping the var
// keeps that property in production while letting each test start clean, without
// bending the product to be testable.
func freshRegistry(t *testing.T) {
	t.Helper()
	prev := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = prev })
}

// mockConfig returns a config that Build can satisfy entirely in-process: the
// mock protocol backend needs no fullnode, no identity and no Redis.
func mockConfig(chain []string) *config.Config {
	cfg := &config.Config{
		Protocol: config.ProtocolConfig{
			Type: config.ProtocolTypeMock,
			Mock: config.MockProtocolConfig{EndpointCount: 3},
		},
		Gateway: config.GatewayConfig{
			GatewayMode:     "centralized",
			MiddlewareChain: chain,
			Services: []config.ServiceConfig{
				{ID: "eth", Type: "evm", RPCTypes: []string{"json_rpc"}, SyncAllowance: 5},
			},
		},
	}
	cfg.Concurrency.MaxConcurrentRelays = 100
	cfg.Concurrency.MaxBatchPayloads = 10
	return cfg
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestBuild_DefaultChainBuilds is the guard against drift between the two lists
// that must agree: every name in relay.DefaultChainOrder has to be registered in
// Build. A name added to the order but not registered fails here rather than at
// startup in production.
func TestBuild_DefaultChainBuilds(t *testing.T) {
	freshRegistry(t)
	app, err := Build(t.Context(), mockConfig(nil), testLogger())
	if err != nil {
		t.Fatalf("the default chain must build: %v", err)
	}
	if app.Router == nil {
		t.Error("Build returned no router")
	}
}

// TestBuild_ChainComesFromConfig is the point of the whole exercise. The chain
// used to be a literal in Build while gateway_config.middleware_chain parsed
// into a field nothing read — a config that asked for a different chain got the
// hard-coded one, silently. An unknown name proves the config now reaches
// BuildChain: it can only fail if Build actually read it.
//
// The chain is otherwise valid (parse, then send_relay innermost) so the
// failure is the unknown name rather than a missing send_relay, which
// validateConfig would catch first.
func TestBuild_ChainComesFromConfig(t *testing.T) {
	freshRegistry(t)
	chain := []string{relay.MWParse, "nonexistent_middleware", relay.MWSendRelay}
	_, err := Build(t.Context(), mockConfig(chain), testLogger())
	if err == nil {
		t.Fatal("Build accepted an unknown middleware name — the configured chain is being ignored")
	}
	if !strings.Contains(err.Error(), "nonexistent_middleware") {
		t.Errorf("err = %v, want it to name the unknown middleware", err)
	}
}

// TestBuild_ConfigCannotBreakOrderingInvariants: taking the order from YAML must
// not hand an operator a way to silently disable endpoint rotation.
func TestBuild_ConfigCannotBreakOrderingInvariants(t *testing.T) {
	freshRegistry(t)
	// select_endpoint outside retry: retry would re-run a chain that never
	// re-selects, so every attempt would reuse the same endpoint.
	chain := []string{relay.MWParse, relay.MWSelectEndpoint, relay.MWRetry, relay.MWSendRelay}

	_, err := Build(t.Context(), mockConfig(chain), testLogger())
	if err == nil {
		t.Fatal("Build accepted a chain that breaks endpoint rotation")
	}
	if !strings.Contains(err.Error(), "must precede") {
		t.Errorf("err = %v, want an ordering violation", err)
	}
}

// TestBuild_RejectsChainWithoutSendRelay: without the one middleware that
// relays, every request would reach the terminal and error. Fail at startup.
func TestBuild_RejectsChainWithoutSendRelay(t *testing.T) {
	freshRegistry(t)
	_, err := Build(t.Context(), mockConfig([]string{relay.MWParse, relay.MWValidate}), testLogger())
	if err == nil {
		t.Fatal("Build accepted a chain with no send_relay — nothing would ever be relayed")
	}
	if !strings.Contains(err.Error(), relay.MWSendRelay) {
		t.Errorf("err = %v, want it to name the missing send_relay", err)
	}
}

// TestBuild_AcceptsReducedChain: a config may legitimately drop middlewares, so
// the invariants must not amount to "the default order or nothing".
func TestBuild_AcceptsReducedChain(t *testing.T) {
	freshRegistry(t)
	chain := []string{relay.MWParse, relay.MWRetry, relay.MWSelectEndpoint, relay.MWSendRelay}

	if _, err := Build(t.Context(), mockConfig(chain), testLogger()); err != nil {
		t.Fatalf("a valid reduced chain should build: %v", err)
	}
}

// An unknown middleware name must stop the process rather than quietly build a
// chain missing a step the operator asked for, and the error has to say what
// IS registered — otherwise the only way to find the right spelling is to read
// Build.
func TestBuild_UnknownMiddlewareNameIsAStartupError(t *testing.T) {
	freshRegistry(t)

	_, err := Build(context.Background(), mockConfig([]string{"parse", "not_a_middleware", "send_relay"}), testLogger())
	if err == nil {
		t.Fatal("Build accepted a middleware name nothing registered")
	}
	if !strings.Contains(err.Error(), "not_a_middleware") {
		t.Errorf("error %q does not name the offending entry", err)
	}
	if !strings.Contains(err.Error(), "parse") {
		t.Errorf("error %q does not list the registered names, so an operator cannot find the right spelling", err)
	}
}

// blocked_domains is a standing ban. A malformed entry must refuse to boot: a
// ban that silently covers less than it reads as covering is worse than none,
// because it is trusted.
func TestBuild_MalformedBlockedDomainRefusesToBoot(t *testing.T) {
	for _, entry := range []config.BlockedDomain{
		{Domain: "   "},
		{Domain: "op.example", RPCTypes: []string{"websockets"}},
	} {
		freshRegistry(t)
		cfg := mockConfig(nil)
		cfg.Gateway.BlockedDomains = []config.BlockedDomain{entry}

		if _, err := Build(context.Background(), cfg, testLogger()); err == nil {
			t.Errorf("Build accepted blocked_domains entry %+v", entry)
		}
	}
}

// The mock backend exists so the gateway can be run and load-tested without a
// fullnode; that only holds if Build actually wires it.
func TestBuild_MockProtocolNeedsNoFullnode(t *testing.T) {
	freshRegistry(t)

	app, err := Build(context.Background(), mockConfig(nil), testLogger())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if app.Router == nil {
		t.Error("no router")
	}
	if app.Metrics == nil {
		t.Error("no metrics handler — prometheus_addr would serve nothing")
	}
}

// newRetryFn must read App.Config on every call, not the *config.Config it
// closed over at wire time — that is the whole mechanism a config reload
// (App.Config.Store) relies on to change hedge_delay without a restart.
//
// Revert-check: capturing the boot pointer instead of app.Config.Load (i.e.
// `cfgFn := func() *config.Config { return cfg }`) makes this fail, because
// the second call would still see the boot config's zero hedge_delay.
func TestNewRetryFn_ReadsConfigSnapshot(t *testing.T) {
	var app App
	app.Config.Store(mockConfig(nil))
	store := tuning.NewStore()

	retryFn := newRetryFn(app.Config.Load, store)

	got := retryFn("eth")
	if got.HedgeDelay != 0 {
		t.Fatalf("boot config has no hedge_delay set, got %v", got.HedgeDelay)
	}

	reloaded := mockConfig(nil)
	// EffectiveDefaults only returns Gateway.Defaults at all when MaxRetries or
	// RelayTimeout is also set on it (config/service.go); MaxRetries>0 is what
	// makes it a "configured" retry block rather than a zero value to fall
	// through past.
	reloaded.Gateway.Defaults.Retry.MaxRetries = 1
	reloaded.Gateway.Defaults.Retry.HedgeDelay = 300 * time.Millisecond
	app.Config.Store(reloaded)

	got = retryFn("eth")
	if got.HedgeDelay != 300*time.Millisecond {
		t.Fatalf("HedgeDelay = %v after reload, want 300ms", got.HedgeDelay)
	}
}

// Same guarantee as TestNewRetryFn_ReadsConfigSnapshot, for the timeout knob.
func TestNewTimeoutFn_ReadsConfigSnapshot(t *testing.T) {
	var app App
	app.Config.Store(mockConfig(nil))
	store := tuning.NewStore()

	timeoutFn := newTimeoutFn(app.Config.Load, store)

	got := timeoutFn("eth")
	if got != 0 {
		t.Fatalf("boot config has no relay_timeout set, got %v", got)
	}

	reloaded := mockConfig(nil)
	reloaded.Gateway.Defaults.Timeout.RelayTimeout = 5 * time.Second
	app.Config.Store(reloaded)

	got = timeoutFn("eth")
	if got != 5*time.Second {
		t.Fatalf("RelayTimeout = %v after reload, want 5s", got)
	}
}

// Every QoS plugin type has to survive wiring, including the passthrough that
// an unrecognized type falls back to.
func TestBuild_WiresEveryQoSPluginType(t *testing.T) {
	for _, qosType := range []string{"evm", "cosmos", "solana", "something-else"} {
		freshRegistry(t)
		cfg := mockConfig(nil)
		cfg.Gateway.Services = []config.ServiceConfig{
			{ID: "svc", Type: qosType, RPCTypes: []string{"json_rpc"}, SyncAllowance: 5},
		}

		if _, err := Build(context.Background(), cfg, testLogger()); err != nil {
			t.Errorf("Build with qos type %q: %v", qosType, err)
		}
	}
}

// TestBuild_MockProtocolGetsNoDrainStore pins the honesty of the drain routes
// under the mock backend. The mock hands out endpoints without consulting a
// drain store, so a store wired there would accept a drain, record it and
// answer `applied: true` for a bench that never happens. No store means the
// routes answer 503 and say why.
func TestBuild_MockProtocolGetsNoDrainStore(t *testing.T) {
	freshRegistry(t)

	app, err := Build(context.Background(), mockConfig(nil), testLogger())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if app.Admin == nil {
		t.Fatal("no admin API")
	}

	mux := http.NewServeMux()
	app.Admin.RegisterRoutes(mux)

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/admin/reputation/drain/eth", `{"domain":"example.com","duration":"30m"}`},
		{http.MethodGet, "/admin/reputation/drain/eth", ""},
		{http.MethodDelete, "/admin/reputation/drain/eth/example.com", ""},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s %s: status = %d, want 503 (no drain store under the mock backend), body=%s",
				tc.method, tc.path, rec.Code, rec.Body.String())
		}
	}
}
