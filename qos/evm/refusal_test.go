package evm

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
)

func rpc(method, params string) domain.Payload {
	return domain.NewPayload([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)), domain.RPCTypeJSONRPC, method)
}

// verdict is what the analyzer makes of a node's -32000 answer.
func verdict(message string) heuristic.AnalysisResult {
	return heuristic.Analyze([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":%q}}`, message)), 200, domain.RPCTypeJSONRPC)
}

func TestRefusalVerdict(t *testing.T) {
	const head = 235_168_774
	hex := func(n uint64) string { return fmt.Sprintf("%q", fmt.Sprintf("0x%x", n)) }
	for _, tc := range []struct {
		name    string
		payload domain.Payload
		message string
		refused bool
	}{
		{"sei: a single getLogs block 100 behind, \"pruned\"",
			rpc("eth_getLogs", `[{"fromBlock":`+hex(head-100)+`,"toBlock":`+hex(head-100)+`}]`), "requested height has been pruned", true},
		{"gnosis wording", rpc("debug_traceBlockByNumber", `[`+hex(head-100)+`,{}]`), "No state available for block", true},
		{"op wording, eth_call at latest", rpc("eth_call", `[{"to":"0x1"},"latest"]`), "no state found for block", true},
		{"a block 50,000 behind is a real prune",
			rpc("eth_getLogs", `[{"fromBlock":`+hex(head-50_000)+`,"toBlock":`+hex(head-50_000)+`}]`), "requested height has been pruned", false},
		{"a 100-block range refused is a policy, not a lie",
			rpc("eth_getLogs", `[{"fromBlock":`+hex(head-200)+`,"toBlock":`+hex(head-100)+`}]`), "historical state is not available", false},
		{"by hash: no block number to judge",
			rpc("eth_getLogs", `[{"blockHash":"0xabc"}]`), "requested height has been pruned", false},
		{"not a missing-state answer", rpc("eth_getLogs", `[{"fromBlock":"latest"}]`), "execution reverted", false},
	} {
		got, ok := RefusalVerdict(tc.payload, verdict(tc.message), head)
		if ok != tc.refused || (ok && (got.Reason != heuristic.ReasonRefusedRecent || got.Attribution != heuristic.AttrSupplier || !got.ShouldRetry || got.PenaltySeverity != heuristic.SeverityMajor)) {
			t.Errorf("%s: refused=%v %+v, want refused=%v", tc.name, ok, got, tc.refused)
		}
	}
	if _, ok := RefusalVerdict(rpc("eth_getLogs", `[{"fromBlock":"latest"}]`), verdict("has been pruned"), 0); ok {
		t.Error("with no head nothing is judged")
	}
}

func TestRequestedBlocks(t *testing.T) {
	const head = 1000
	for _, tc := range []struct {
		method, params string
		from, to       uint64
		ok             bool
	}{
		{"eth_getLogs", `[{"fromBlock":"0x3de","toBlock":"0x3e7"}]`, 990, 999, true},
		{"eth_getLogs", `[{"fromBlock":"0x3de"}]`, 990, head, true},
		{"eth_getLogs", `[{"blockHash":"0xabc"}]`, 0, 0, false},
		{"eth_getBalance", `["0x1","0x3e7"]`, 999, 999, true},
		{"eth_getBlockReceipts", `["latest"]`, head, head, true},
		{"eth_chainId", `[]`, 0, 0, false},
	} {
		f, to, ok := requestedBlocks(tc.method, gjson.GetBytes(rpc(tc.method, tc.params).Bytes(), "params"), head)
		if ok != tc.ok || f != tc.from || to != tc.to {
			t.Errorf("%s %s: %d-%d %v, want %d-%d %v", tc.method, tc.params, f, to, ok, tc.from, tc.to, tc.ok)
		}
	}
}
