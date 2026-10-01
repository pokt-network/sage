package evm

import (
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
)

// A block a few minutes old is one no node has discarded: geth-style nodes
// keep the last 128 states and every node keeps far more history, and on
// mainnet sei (2026-10-01) an honest node traced blocks 70 minutes back. An
// answer claiming such a block is pruned is a refusal. That day one owner
// answered heavy calls about blocks 20 seconds to 70 minutes old, in
// 15-25ms, with "requested height has been pruned" (sei), "No state available
// for block" (gnosis) and "no state found for block" (op), where other
// operators served them. Those fell to the unscored -32000 default or graded
// as the chain's answer, and were paid either way.
//
// Only short spans count. A node may refuse a wide eth_getLogs range on
// policy, in the same words ("historical state is not available" for an
// unfiltered 100-block range, served block by block); that is a limit, not a
// lie, and is left as it was.
const refusalMaxSpan = 10

// The window a request's blocks must fall in, behind the perceived head, in
// blocks and in the chain's own time; the larger of each applies. Newer than
// the floor, a node a few blocks behind may not have the block yet and says
// so in the same words (the first version judged those and charged honest
// parties within minutes of rolling). Older than the ceiling, "pruned" may be
// true. In time because block counts mean nothing across chains: 128 blocks
// is 25 minutes on Ethereum and 51 seconds on sei.
const (
	refusalMinBlocks = 16
	refusalMinAge    = 30 * time.Second
	refusalMaxBlocks = nearHeadBlocks
	refusalMaxAge    = 10 * time.Minute
)

// refusalClaims are the wordings that claim a block's state or history was
// discarded. Narrower than heuristic.ReportsMissingHistoricalState, on
// purpose: geth's "missing trie node" and "metadata is not found" are true
// for anything past its 128 in-memory states and for recent blocks after a
// restart; "lite fullnode", "api is not supported" and the indexing wordings
// are capabilities. None of those is a claim a node can be caught lying with.
var refusalClaims = []string{
	"has been pruned",
	"is pruned",
	"pruned history",
	"no state available for block",
	"no state found for block",
	"state not available",
	"historical state",
	"height is not available",
}

// methodsWithLeadingBlock name the block in their first parameter.
var methodsWithLeadingBlock = map[string]bool{
	"debug_traceBlockByNumber":      true,
	"eth_getBlockReceipts":          true,
	"trace_block":                   true,
	"trace_replayBlockTransactions": true,
}

// RefusalVerdict re-attributes an answer claiming a block was discarded
// (refusalClaims), to a request whose blocks all fall inside the refusal window
// behind head over a span of at most refusalMaxSpan, as the supplier refusing
// (heuristic.RefusedRecent). blocksIn converts the window's ages to blocks at
// the chain's rate (0 while unknown, leaving the block bounds). nodeHead is
// the answering host's own last reported height, 0 when unknown: a block at
// or past it is one the host may not have yet, whatever the perceived head
// says. head 0 (no consensus yet) judges nothing.
func RefusalVerdict(payload domain.Payload, result heuristic.AnalysisResult, head, nodeHead uint64, blocksIn func(time.Duration) uint64) (heuristic.AnalysisResult, bool) {
	if head == 0 || result.Attribution == heuristic.AttrSupplier || !claimsDiscarded(result.Details) {
		return result, false
	}
	minBack, maxBack := uint64(refusalMinBlocks), uint64(refusalMaxBlocks)
	if blocksIn != nil {
		minBack, maxBack = max(minBack, blocksIn(refusalMinAge)), max(maxBack, blocksIn(refusalMaxAge))
	}
	method := payload.Method()
	from, to, ok := requestedBlocks(method, gjson.GetBytes(payload.Bytes(), "params"), head)
	if !ok || to < from || to-from > refusalMaxSpan || from+maxBack < head || to+minBack > head ||
		(nodeHead > 0 && to >= nodeHead) {
		return result, false
	}
	return heuristic.RefusedRecent(fmt.Sprintf("%s blocks %d-%d, %d-%d behind the head %d: %s", method, from, to, head-to, head-from, head, result.Details)), true
}

// claimsDiscarded reports whether a verdict's details carry one of
// refusalClaims.
func claimsDiscarded(details string) bool {
	lower := strings.ToLower(details)
	for _, w := range refusalClaims {
		if strings.Contains(lower, w) {
			return true
		}
	}
	return false
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
