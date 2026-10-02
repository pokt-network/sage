package evm

import (
	"fmt"
	"strings"
	"testing"
	"time"

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
		{"op wording, eth_call at a block 100 behind", rpc("eth_call", `[{"to":"0x1"},`+hex(head-100)+`]`), "no state found for block", true},
		{"latest: the node may simply be a block behind", rpc("eth_call", `[{"to":"0x1"},"latest"]`), "missing trie node", false},
		{"two blocks behind: a node that is behind cannot have it yet",
			rpc("eth_getLogs", `[{"fromBlock":`+hex(head-2)+`,"toBlock":`+hex(head-2)+`}]`), "no state found for block", false},
		{"ahead of the head", rpc("eth_getBalance", `["0x1",`+hex(head+1)+`]`), "no state found for block", false},
		{"one block behind", rpc("eth_getBalance", `["0x1",`+hex(head-1)+`]`), "no state found for block", false},
		{"\"historical state is not available\" is an honest wording", rpc("eth_getBalance", `["0x1",`+hex(head-100)+`]`), "historical state is not available", false},
		{"twenty behind is judged", rpc("eth_getBalance", `["0x1",`+hex(head-20)+`]`), "no state found for block", true},
		{"missing trie node: an honest geth after a restart", rpc("eth_call", `[{"to":"0x1"},`+hex(head-100)+`]`), "missing trie node", false},
		{"a block 50,000 behind is a real prune",
			rpc("eth_getLogs", `[{"fromBlock":`+hex(head-50_000)+`,"toBlock":`+hex(head-50_000)+`}]`), "requested height has been pruned", false},
		{"a 100-block range refused is a policy, not a lie",
			rpc("eth_getLogs", `[{"fromBlock":`+hex(head-200)+`,"toBlock":`+hex(head-100)+`}]`), "historical state is not available", false},
		{"by hash: no block number to judge",
			rpc("eth_getLogs", `[{"blockHash":"0xabc"}]`), "requested height has been pruned", false},
		{"not a missing-state answer", rpc("eth_getLogs", `[{"fromBlock":"latest"}]`), "execution reverted", false},
	} {
		got, ok, _ := RefusalVerdict(tc.payload, verdict(tc.message), head, nil)
		if ok != tc.refused || (ok && (got.Reason != heuristic.ReasonRefusedRecent || got.Attribution != heuristic.AttrSupplier || !got.ShouldRetry || got.PenaltySeverity != heuristic.SeverityMajor)) {
			t.Errorf("%s: refused=%v %+v, want refused=%v", tc.name, ok, got, tc.refused)
		}
	}
	if _, ok, _ := RefusalVerdict(rpc("eth_getLogs", `[{"fromBlock":"latest"}]`), verdict("has been pruned"), 0, nil); ok {
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

// The detail names the method, the blocks, the depth and the node's words.
func TestRefusalVerdict_Detail(t *testing.T) {
	const head = 1000
	req := rpc("eth_getLogs", `[{"fromBlock":"0x384","toBlock":"0x384"}]`) // 900
	got, ok, _ := RefusalVerdict(req, verdict("no state found for block"), head, nil)
	if !ok {
		t.Fatal("block 900 at head 1000 is a refusal")
	}
	for _, want := range []string{"eth_getLogs", "900-900", "100-100 behind the head 1000", "no state found for block"} {
		if !strings.Contains(got.Details, want) {
			t.Errorf("detail %q lacks %q", got.Details, want)
		}
	}
}

// On a fast chain the window is time, not blocks: sei makes 2.5 blocks a
// second, so 30 seconds is 75 blocks and ten minutes 1,500. The owner said
// "pruned" at every depth from 20 seconds to 70 minutes on 2026-10-01; an
// honest node traced all of them.
func TestRefusalVerdict_TimeWindowOnAFastChain(t *testing.T) {
	const head = 235_178_858
	rate := func(d time.Duration) uint64 { return uint64(2.5 * d.Seconds()) }
	for _, tc := range []struct {
		back    uint64
		refused bool
	}{{50, false}, {100, true}, {300, true}, {1000, true}, {10_000, false}} {
		b := fmt.Sprintf("%q", fmt.Sprintf("0x%x", head-tc.back))
		req := rpc("eth_getLogs", `[{"fromBlock":`+b+`,"toBlock":`+b+`}]`)
		if _, ok, _ := RefusalVerdict(req, verdict("requested height has been pruned"), head, rate); ok != tc.refused {
			t.Errorf("head-%d: refused=%v, want %v", tc.back, ok, tc.refused)
		}
	}
}

// Honest state-missing wordings and capabilities are never refusals, however
// recent the block.
func TestRefusalVerdict_HonestWordings(t *testing.T) {
	const head = 1000
	req := rpc("eth_call", `[{"to":"0x1"},"0x384"]`) // 900
	for _, msg := range []string{"missing trie node abc", "metadata is not found, 900", "this API is not supported by lite fullnode"} {
		if _, ok, _ := RefusalVerdict(req, verdict(msg), head, nil); ok {
			t.Errorf("%q judged a refusal", msg)
		}
	}
}

// A state method's window stops at 128 blocks however slow the chain: nodes
// keep that many states, and say "gone" truthfully past them.
func TestRefusalVerdict_StateMethodsStopAt128(t *testing.T) {
	const head = 1_000_000
	rate := func(d time.Duration) uint64 { return uint64(4 * d.Seconds()) } // a 250ms chain
	call := func(back uint64) domain.Payload {
		return rpc("eth_getBalance", fmt.Sprintf(`["0x1","0x%x"]`, head-back))
	}
	logs := func(back uint64) domain.Payload {
		b := fmt.Sprintf("%q", fmt.Sprintf("0x%x", head-back))
		return rpc("eth_getLogs", `[{"fromBlock":`+b+`,"toBlock":`+b+`}]`)
	}
	for _, tc := range []struct {
		name    string
		payload domain.Payload
		refused bool
	}{
		{"state 150 back", call(150), false},
		{"state 125 back", call(125), true},
		{"logs 2,000 back (8 min)", logs(2000), true},
		{"logs 3,000 back (12.5 min)", logs(3000), false},
	} {
		if _, ok, _ := RefusalVerdict(tc.payload, verdict("no state found for block"), head, rate); ok != tc.refused {
			t.Errorf("%s: refused=%v, want %v", tc.name, ok, tc.refused)
		}
	}
}

// A per-block claim on a history method is judged over any span, by its
// oldest block; policy wordings, softer claims and state methods keep the
// span rule. The owner refused eth_getLogs ranges on base and op this way.
func TestRefusalVerdict_PerBlockClaimsOverRanges(t *testing.T) {
	const head = 1_000_000
	rate := func(d time.Duration) uint64 { return uint64(0.5 * d.Seconds()) } // base: 2s blocks
	logs := func(fromBack, toBack uint64) domain.Payload {
		return rpc("eth_getLogs", fmt.Sprintf(`[{"fromBlock":"0x%x","toBlock":"0x%x"}]`, head-fromBack, head-toBack))
	}
	toLatest := rpc("eth_getLogs", fmt.Sprintf(`[{"fromBlock":"0x%x","toBlock":"latest"}]`, head-150))
	for _, tc := range []struct {
		name    string
		payload domain.Payload
		message string
		refused bool
	}{
		{"owner: 150-block range up to the head", toLatest, "no state found for block", true},
		{"owner: 100 to 20 back", logs(100, 20), "no state available for block", true},
		{"policy wording on the same range", toLatest, "historical state is not available", false},
		{"range-too-large policy", toLatest, "query returned more than 10000 results; range too large", false},
		{"address-filter policy", toLatest, "requires an 'address' filter", false},
		{"a softer claim over a wide range keeps the span rule", logs(150, 20), "block is pruned", false},
		{"oldest block 30 minutes back: may be a real prune", logs(900, 20), "no state found for block", false},
		{"oldest block 10s back: too new", logs(5, 0), "no state found for block", false},
		{"a state method keeps its own rules", rpc("eth_getBalance", fmt.Sprintf(`["0x1","0x%x"]`, head-150)), "no state found for block", false},
	} {
		if _, ok, _ := RefusalVerdict(tc.payload, verdict(tc.message), head, rate); ok != tc.refused {
			t.Errorf("%s: refused=%v, want %v", tc.name, ok, tc.refused)
		}
	}
}
