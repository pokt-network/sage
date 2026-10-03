package heuristic

import (
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
		{"something nobody catalogued", "server_error", true},
	} {
		body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"` + tc.msg + `"}}`)
		got := Analyze(body, 200, domain.RPCTypeJSONRPC)
		if got.Reason != tc.reason || got.ShouldRetry != tc.retry {
			t.Errorf("%q: %s retry=%v, want %s retry=%v", tc.msg, got.Reason, got.ShouldRetry, tc.reason, tc.retry)
		}
	}
}
