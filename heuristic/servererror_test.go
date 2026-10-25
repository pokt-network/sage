package heuristic

import (
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// -32000 wordings about the request are the client's answer (delivered, not
// retried, nobody scored); a node saying it is behind is the supplier's, minor;
// a skipped slot is the chain's. Wordings seen on mainnet (2026-10-03).
func TestClassifyServerError_ClientLagAndChainWordings(t *testing.T) {
	for _, tc := range []struct {
		msg, reason string
		retry       bool
	}{
		{"execution reverted", "client_error", false},
		{"insufficient balance for transfer", "client_error", false},
		{"failed with 16777216 gas: insufficient funds for gas * price + value: address 0x1 have 1 want 2", "client_error", false},
		{"rlp: value size exceeds available input length", "client_error", false},
		{"nonce too low: next nonce 5, tx nonce 4", "client_error", false},
		{"Transaction simulation failed: Blockhash not found", "client_error", false},
		{"Node is behind by 120 slots", "node_behind", true},
		{"Slot 123 was skipped, or missing due to ledger jump to recent snapshot", "blockchain_error", true},
		{"EVM error: InvalidFEOpcode", "client_error", false},
		{"filter not found", "client_error", false},
		{"Transaction version (0) is not supported by the requesting client", "client_error", false},
		{"all suitable upstream RPC backends unavailable", "node_behind", true},
		{"Transaction history is not available from this node", "blockchain_error", true},
		{"state 0xabc is not available", "blockchain_error", true},
		{"eth_getLogs block range exceeds server limit", "blockchain_error", true},
		{"height 30000000 must be less than or equal to the current blockchain height 29999990", "blockchain_error", true},
		{"something nobody catalogued", "server_error", true},
	} {
		body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"` + tc.msg + `"}}`)
		got := Analyze(body, 200, domain.RPCTypeJSONRPC)
		if got.Reason != tc.reason || got.ShouldRetry != tc.retry {
			t.Errorf("%q: %s retry=%v, want %s retry=%v", tc.msg, got.Reason, got.ShouldRetry, tc.reason, tc.retry)
		}
	}
}

// eth_simulateV1 answers the request's own faults with -380xx codes, whatever
// the wording; -38026 is the node's own limit, retried elsewhere; any other
// code outside the spec stays unknown and retried.
func TestClassifyJSONRPCError_SimulateCodesAreTheClients(t *testing.T) {
	for _, tc := range []struct {
		code, msg, reason string
		retry             bool
	}{
		{"-38010", "err: nonce too low: address 0x1, tx: 4 state: 5", "client_error", false},
		{"-38011", "err: nonce too high: address 0x1, tx: 9 state: 5", "client_error", false},
		{"-38014", "err: insufficient funds for gas * price + value", "client_error", false},
		{"-38015", "block gas limit reached: remaining: 1, required: 2", "client_error", false},
		{"-38026", "too many blocks", "blockchain_error", true},
		{"-37999", "something nobody catalogued", "unknown_error_code", true},
		{"-38100", "something nobody catalogued", "unknown_error_code", true},
	} {
		body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":` + tc.code + `,"message":"` + tc.msg + `"}}`)
		got := Analyze(body, 200, domain.RPCTypeJSONRPC)
		if got.Reason != tc.reason || got.ShouldRetry != tc.retry {
			t.Errorf("%s %q: %s retry=%v, want %s retry=%v", tc.code, tc.msg, got.Reason, got.ShouldRetry, tc.reason, tc.retry)
		}
	}
}

// An error member that is not {code, message, data} still says something;
// the verdict keeps it, so the unclassified metric can name the wording.
func TestClassifyJSONRPCError_BareErrorKeepsItsText(t *testing.T) {
	for body, want := range map[string]string{
		`{"jsonrpc":"2.0","id":1,"error":"boom"}`:      "boom",
		`{"jsonrpc":"2.0","id":1,"error":{"why":"x"}}`: `{"why":"x"}`,
	} {
		got := Analyze([]byte(body), 200, domain.RPCTypeCometBFT)
		if got.Reason != "unknown_error_code" || !strings.HasSuffix(got.Details, ": "+want) {
			t.Errorf("%s: %s %q, want unknown_error_code ending %q", body, got.Reason, got.Details, want)
		}
	}
}
