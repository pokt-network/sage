package evm

import (
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
)

// A node keeps the state of at least the last nearHeadBlocks blocks and the
// history (logs, receipts, bodies) of far more, so an answer claiming either
// is gone for a block that recent is not a prune. On mainnet (2026-10-01) one
// owner answered heavy calls about blocks a few minutes old, in 15-25ms, with
// "requested height has been pruned" (sei), "No state available for block"
// (gnosis) and "no state found for block" (op), where other operators served
// them: refusals worded as prunes, which fell to the unscored -32000 default
// or graded as the chain's answer, and were paid either way.
//
// Only short spans count. A node may refuse a wide eth_getLogs range on
// policy, in the same words ("historical state is not available" for an
// unfiltered 100-block range, served block by block); that is a limit, not a
// lie, and is left as it was.
const refusalMaxSpan = 10

// methodsWithLeadingBlock name the block in their first parameter.
var methodsWithLeadingBlock = map[string]bool{
	"debug_traceBlockByNumber":      true,
	"eth_getBlockReceipts":          true,
	"trace_block":                   true,
	"trace_replayBlockTransactions": true,
}

// RefusalVerdict re-attributes a missing-state answer to a request whose every
// block is within nearHeadBlocks of head, over a span of at most
// refusalMaxSpan, as the supplier refusing (heuristic.RefusedRecent). head 0
// (no consensus yet) judges nothing. Shared by every plugin that serves an EVM
// face.
func RefusalVerdict(payload domain.Payload, result heuristic.AnalysisResult, head uint64) (heuristic.AnalysisResult, bool) {
	if head == 0 || result.Attribution == heuristic.AttrSupplier || !heuristic.ReportsMissingHistoricalState(result.Details) {
		return result, false
	}
	from, to, ok := requestedBlocks(payload.Method(), gjson.GetBytes(payload.Bytes(), "params"), head)
	if !ok || to < from || to-from > refusalMaxSpan || from+nearHeadBlocks < head {
		return result, false
	}
	return heuristic.RefusedRecent(fmt.Sprintf("claims block %d is gone, %d behind the head: %s", from, head-min(from, head), result.Details)), true
}

// requestedBlocks reads the lowest and highest block a request names,
// resolving tags against head. ok is false for a request that names no block,
// or names one only by hash.
func requestedBlocks(method string, params gjson.Result, head uint64) (from, to uint64, ok bool) {
	block := func(v gjson.Result) (uint64, bool) {
		switch tag := v.String(); {
		case !v.Exists() || recentStateBlockTags[tag]:
			return head, true
		case tag == blockTagEarliest:
			return 0, true
		default:
			n, err := parseHexUint64(tag)
			return n, err == nil
		}
	}
	args := params.Array()
	switch {
	case method == "eth_getLogs":
		if len(args) == 0 || args[0].Get("blockHash").Exists() {
			return 0, 0, false
		}
		f, okF := block(args[0].Get("fromBlock"))
		t, okT := block(args[0].Get("toBlock"))
		return f, t, okF && okT
	case methodsWithLeadingBlock[method]:
		if len(args) == 0 || args[0].Type != gjson.String {
			return 0, 0, false
		}
		n, ok := block(args[0])
		return n, n, ok
	case methodsWithBlockParam[method]:
		s := findLastStringParam(args)
		if s == "" {
			return 0, 0, false
		}
		n, ok := block(gjson.Parse(`"` + s + `"`))
		return n, n, ok
	}
	return 0, 0, false
}
