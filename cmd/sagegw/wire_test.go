package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/relay"
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
func TestBuild_ChainComesFromConfig(t *testing.T) {
	freshRegistry(t)
	_, err := Build(t.Context(), mockConfig([]string{"nonexistent_middleware"}), testLogger())
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
