package heuristic

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// Solana answers getProgramAccounts for a program the node has no secondary
// account index for with -32010 "<key> excluded from account secondary
// indexes; this RPC method unavailable for key". That is how the node was
// configured, not a fault — another operator serves the identical call from
// its index. PATH measured a dapp polling three such queries continuously
// charging one operator a circuit-breaker failure and a reputation penalty on
// every poll, until two of its four endpoints sat at score 0.
//
// Both tiers must recognise it: Tier 2 when the JSON-RPC error parses, Tier 3
// when it arrives inside a body that does not.
func TestAnalyze_SolanaAccountIndexExclusion(t *testing.T) {
	const msg = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA excluded from account secondary indexes; this RPC method unavailable for key"

	tests := []struct {
		name string
		body string
	}{
		{
			name: "tier 2: parsed JSON-RPC error",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32010,"message":"` + msg + `"}}`,
		},
		{
			name: "tier 3: unparsed body carrying the same wording",
			body: `{"note":"upstream said ` + msg + `"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Analyze([]byte(tt.body), http.StatusOK, domain.RPCTypeJSONRPC)
			if result.Attribution != AttrBlockchain {
				t.Fatalf("attribution = %v, want blockchain", result.Attribution)
			}
			if result.ShouldPenalize {
				t.Fatal("a missing account index must not penalize the supplier")
			}
			if result.ShouldCircuitBreak {
				t.Fatal("a missing account index must not circuit-break the domain")
			}
			if !result.ShouldRetry {
				t.Fatal("must retry elsewhere — another operator has the index")
			}
		})
	}

	// Deliberately NOT an archival pattern: an index exclusion is not missing
	// historical state, and must not route through the archival demotion path.
	if ReportsMissingHistoricalState(msg) {
		t.Fatal("account-index exclusion must not read as missing historical state")
	}
}
