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
// policy (one operator does, for an unfiltered 100-block range it serves
// block by block); that is a limit, not a lie, and is left as it was.
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
// restart, and "historical state is not available" is how geth and erigon
// nodes honestly say the same, and how one operator refuses wide log ranges
// on policy; "lite fullnode", "api is not supported" and the indexing
// wordings are capabilities. None of those is a claim a node can be caught
// lying with.
var refusalClaims = []string{
	"has been pruned",
	"is pruned",
	"pruned history",
	"no state available for block",
	"no state found for block",
	"state not available",
	"height is not available",
}

// perBlockClaims name one block as gone. A node refusing a range on policy
// words it as a policy ("range too large", "requires an address filter",
// "historical state is not available"), never this way; on mainnet
// (2026-10-02) one owner answered eth_getLogs ranges hundreds of blocks wide,
// on base and op, with "no state found for block", and no other party used
// the wording on logs at all. For a history method such a claim is judged
// over any span, by its oldest block (see RefusalVerdict).
var perBlockClaims = []string{
	"no state found for block",
	"no state available for block",
	"requested height has been pruned",
}

// stateMethods read state, which geth-style nodes keep for only the last 128
// blocks; for them the window never reaches past refusalMaxBlocks, however
// fast the chain (where ten minutes would be more blocks than that). History
// (logs, receipts, bodies) is kept far longer, so the time ceiling applies to
// the rest. On mainnet (2026-10-01) an honest node
// answered eth_getBalance 128-178 blocks back on a fast chain with "historical
// state is not available", inside the ten-minute window.
var stateMethods = map[string]bool{
	"eth_getBalance":                true,
	"eth_call":                      true,
	"eth_estimateGas":               true,
	"eth_getCode":                   true,
	"eth_getStorageAt":              true,
	"eth_getTransactionCount":       true,
	"debug_traceBlockByNumber":      true,
	"trace_block":                   true,
	"trace_replayBlockTransactions": true,
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
// the chain's rate (0 while unknown, leaving the block bounds). head 0 (no
// consensus yet) judges nothing.
//
// skip names why a claim was not judged ("" when there was none to judge, or
// it was): for a plugin's debug log, to see what the rule leaves out.
//
// A per-block claim (perBlockClaims) on a history method is judged over any
// span, by its OLDEST block: a node may be unable to serve the newest blocks
// of a range touching the head, but it cannot truthfully claim a block
// minutes old is gone.
//
// The answering host's own reported head is deliberately not consulted. A
// cache reports a stale head too, minutes behind on sei, and every lie about
// a block newer than that was exempt; the floor already covers a node a few
// blocks behind.
func RefusalVerdict(payload domain.Payload, result heuristic.AnalysisResult, head uint64, blocksIn func(time.Duration) uint64) (refined heuristic.AnalysisResult, ok bool, skip string) {
	if head == 0 || result.Attribution == heuristic.AttrSupplier || !claimsAny(result.Details, refusalClaims) {
		return result, false, ""
	}
	method := payload.Method()
	perBlock := !stateMethods[method] && claimsAny(result.Details, perBlockClaims)
	minBack, maxBack := uint64(refusalMinBlocks), uint64(refusalMaxBlocks)
	if blocksIn != nil {
		minBack = max(minBack, blocksIn(refusalMinAge))
		if !stateMethods[method] {
			maxBack = max(maxBack, blocksIn(refusalMaxAge))
		}
	}
	from, to, named := requestedBlocks(method, gjson.GetBytes(payload.Bytes(), "params"), head)
	// The block the floor is measured on: the newest, or for a per-block
	// claim the oldest.
	floorBlock := to
	if perBlock {
		floorBlock = from
	}
	switch {
	case !named || to < from:
		return result, false, "no block number"
	case !perBlock && to-from > refusalMaxSpan:
		return result, false, "span"
	case floorBlock+minBack > head:
		return result, false, "too new"
	case from+maxBack < head:
		return result, false, "too old"
	}
	return heuristic.RefusedRecent(fmt.Sprintf("%s blocks %d-%d, %d-%d behind the head %d: %s", method, from, to, head-min(to, head), head-from, head, result.Details)), true, ""
}

// claimsAny reports whether a verdict's details carry one of wordings.
func claimsAny(details string, wordings []string) bool {
	lower := strings.ToLower(details)
	for _, w := range wordings {
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
