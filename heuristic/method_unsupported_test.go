package heuristic

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// An endpoint answering "I do not serve this method" is the structured form
// of a method timeout: not a fault, but this host should not get that method
// again for a while. Historical-state wordings are per BLOCK and Solana's
// index exclusion is per PROGRAM; marking on those would exclude a host from
// every eth_getBalance for one honest pruned-state answer.
func TestAnalyze_MethodBlocking(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"-32601 method not found", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method debug_traceTransaction does not exist/is not available"}}`, true},
		{"tron lite fullnode (tier 2)", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"this api is not supported on lite fullnode"}}`, true},
		{"api not supported (tier 3, unparsed)", `{"note":"upstream said: api is not supported"}`, true},
		{"lite fullnode (tier 3, unparsed)", `{"note":"lite fullnode restriction"}`, true},
		{"pruned state is per block, not per method", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"missing trie node abc"}}`, false},
		{"pbss pruned state", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"metadata is not found, 0x14a5f1c"}}`, false},
		{"solana index exclusion is per program", `{"jsonrpc":"2.0","id":1,"error":{"code":-32010,"message":"Tokenkeg excluded from account secondary indexes; this RPC method unavailable for key"}}`, false},
		{"plain success", `{"jsonrpc":"2.0","id":1,"result":"0x1"}`, false},
		{"5xx", ``, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := http.StatusOK
			if tt.name == "5xx" {
				status = http.StatusBadGateway
			}
			r := Analyze([]byte(tt.body), status, domain.RPCTypeJSONRPC)
			if r.MethodBlocking != tt.want {
				t.Fatalf("MethodBlocking = %v, want %v (%+v)", r.MethodBlocking, tt.want, r)
			}
		})
	}
}

// -32601 keeps its client attribution and no-retry: a bogus method name must
// not bounce across every host in the pool. The mark is what changes — the
// NEXT request for that method goes elsewhere.
func TestAnalyze_MethodNotFoundStillDoesNotRetry(t *testing.T) {
	r := Analyze([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`), http.StatusOK, domain.RPCTypeJSONRPC)
	if r.ShouldRetry || r.ShouldPenalize || r.Attribution != AttrClient {
		t.Fatalf("-32601 grading changed: %+v", r)
	}
}
