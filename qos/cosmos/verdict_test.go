package cosmos

import (
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
)

// A 5xx on a route the node answers 5xx to by design is the chain's answer
// to the client's query: delivered, not retried, nobody scored. Any other
// route, RPC type or reason is left to the analyzer.
func TestRefineVerdict_Query5xx(t *testing.T) {
	p := NewPlugin(nil, Config{})
	rest := func(path string) domain.Payload {
		return domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP(path, "GET")
	}
	node5xx := heuristic.AnalysisResult{ShouldRetry: true, ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMajor, Attribution: heuristic.AttrSupplier, Reason: "http_5xx"}
	miner5xx := heuristic.AnalysisResult{ShouldRetry: true, ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMinor, Attribution: heuristic.AttrSupplier, Reason: "upstream_5xx"}

	for _, tc := range []struct {
		name string
		p    domain.Payload
		in   heuristic.AnalysisResult
		want bool
	}{
		{"smart query, node 500", rest("/cosmwasm/wasm/v1/contract/osmo1abc/smart/eyJ0b2tlbl9pbmZvIjp7fX0="), node5xx, true},
		{"smart query with query string, miner 502", rest("/cosmwasm/wasm/v1/contract/osmo1abc/smart/eyJ9?x=1"), miner5xx, true},
		{"txs by block", rest("/cosmos/tx/v1beta1/txs/block/99999999"), node5xx, true},
		{"contract state is not a client query", rest("/cosmwasm/wasm/v1/contract/osmo1abc/state"), node5xx, false},
		{"txs by hash", rest("/cosmos/tx/v1beta1/txs/ABCDEF0123456789"), node5xx, false},
		{"another route", rest("/cosmos/bank/v1beta1/balances/osmo1abc"), node5xx, false},
		{"not a 5xx", rest("/cosmwasm/wasm/v1/contract/osmo1abc/smart/eyJ9"), heuristic.AnalysisResult{Reason: "internal_error"}, false},
		{"comet_bft, same path shape", domain.NewPayload(nil, domain.RPCTypeCometBFT, "").WithHTTP("/cosmos/tx/v1beta1/txs/block/1", "GET"), node5xx, false},
		{"json_rpc method", domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_call"), node5xx, false},
	} {
		got, ok := p.RefineVerdict(tc.p, tc.in)
		if ok != tc.want {
			t.Errorf("%s: refined = %v, want %v", tc.name, ok, tc.want)
			continue
		}
		if !ok {
			if got != tc.in {
				t.Errorf("%s: an unrefined verdict must be returned as it was", tc.name)
			}
			continue
		}
		if got.Reason != ReasonQuery5xx || got.Attribution != heuristic.AttrBlockchain || got.ShouldRetry || got.ShouldPenalize || got.ShouldCircuitBreak || got.MethodBlocking {
			t.Errorf("%s: refined verdict %+v", tc.name, got)
		}
	}
}
