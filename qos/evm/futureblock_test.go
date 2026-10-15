package evm

import (
	"fmt"
	"testing"

	"github.com/pokt-network/sage/heuristic"
)

// A node's "header not found" about a block the chain has not made is the
// answer every node gives; about a block just made it is a node a block
// behind, and another may have it. The request's block against the head
// tells them apart, not the wording.
func TestFutureBlockVerdict(t *testing.T) {
	const head = 515_099_166
	hex := func(n uint64) string { return fmt.Sprintf("%q", fmt.Sprintf("0x%x", n)) }
	beyond := func(n uint64) bool { return n > head+10 }
	for _, tc := range []struct {
		name    string
		method  string
		params  string
		message string
		future  bool
	}{
		{"eth_call well past the head", "eth_call", `[{"to":"0x1"},` + hex(head+100_000) + `]`, "header not found", true},
		{"eth_getBalance past the head, another wording", "eth_getBalance", `["0x1",` + hex(head+11) + `]`, "historical state is not available", true},
		{"eth_getLogs range starting past the head", "eth_getLogs", `[{"fromBlock":` + hex(head+50) + `,"toBlock":` + hex(head+60) + `}]`, "unknown block", true},
		{"one block ahead: a node behind the pool", "eth_call", `[{"to":"0x1"},` + hex(head+1) + `]`, "header not found", false},
		{"a block the chain has", "eth_getBalance", `["0x1",` + hex(head-5) + `]`, "header not found", false},
		{"latest", "eth_call", `[{"to":"0x1"},"latest"]`, "header not found", false},
		{"by hash: no number to judge", "eth_getLogs", `[{"blockHash":"0xabc"}]`, "unknown block", false},
		{"not a blockchain_error", "eth_call", `[{"to":"0x1"},` + hex(head+100_000) + `]`, "execution reverted", false},
	} {
		got, ok := futureBlockVerdict(rpc(tc.method, tc.params), verdict(tc.message), head, beyond)
		if ok != tc.future {
			t.Errorf("%s: future=%v (%s), want %v", tc.name, ok, got.Reason, tc.future)
			continue
		}
		if ok && (got.Reason != heuristic.ReasonFutureBlock || !got.ShouldRetry || got.ShouldPenalize || got.Attribution != heuristic.AttrBlockchain) {
			t.Errorf("%s: %+v, want future_block, still retried, unscored, the chain's", tc.name, got)
		}
	}
	if _, ok := futureBlockVerdict(rpc("eth_call", `[{"to":"0x1"},`+hex(head+100_000)+`]`), verdict("header not found"), 0, beyond); ok {
		t.Error("with no head nothing is judged")
	}
}
