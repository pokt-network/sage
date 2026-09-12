package config

import (
	"strings"
	"testing"
)

// A comet_bft fallback on a cosmos service that declares comet_bft is where
// SAGE and PATH read the same file into different pools; the contract is
// that the difference is said at startup, once per service, naming the pool.
func TestCometBFTFaceWarnings(t *testing.T) {
	services := []ServiceConfig{
		{ID: "pocket", Type: "cosmos", RPCTypes: []string{"comet_bft", "json_rpc", "rest"}, RPCTypeFallbacks: map[string]string{"comet_bft": "json_rpc"}},
		{ID: "osmosis", Type: "cosmos", RPCTypes: []string{"comet_bft", "rest"}, RPCTypeFallbacks: map[string]string{"comet_bft": "rest"}},
		// No comet_bft declared: the mapping is the plain pool-level fallback, as in PATH.
		{ID: "juno", Type: "cosmos", RPCTypes: []string{"json_rpc", "rest"}, RPCTypeFallbacks: map[string]string{"comet_bft": "json_rpc"}},
		// Mapping onto a type the service does not declare is ignored by the classifier too.
		{ID: "cheqd", Type: "cosmos", RPCTypes: []string{"comet_bft"}, RPCTypeFallbacks: map[string]string{"comet_bft": "json_rpc"}},
		// Not cosmos: tron's json_rpc->rest is somebody else's business.
		{ID: "tron", Type: "tron", RPCTypes: []string{"rest", "json_rpc"}, RPCTypeFallbacks: map[string]string{"json_rpc": "rest"}},
		{ID: "eth", Type: "evm", RPCTypes: []string{"json_rpc"}},
	}
	got := cometBFTFaceWarnings(services)
	if len(got) != 2 {
		t.Fatalf("got %d warnings, want 2 (pocket, osmosis):\n%s", len(got), strings.Join(got, "\n"))
	}
	if !strings.Contains(got[0], `"pocket"`) || !strings.Contains(got[0], "JSON-RPC bodies") || !strings.Contains(got[0], "json_rpc-staked pool") {
		t.Errorf("pocket warning = %q", got[0])
	}
	if !strings.Contains(got[1], `"osmosis"`) || !strings.Contains(got[1], "HTTP paths") || !strings.Contains(got[1], "rest-staked pool") {
		t.Errorf("osmosis warning = %q", got[1])
	}
}
