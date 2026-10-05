package heuristic

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// JSON-RPC error codes.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
	codeServerError    = -32000 // commonly used for execution errors
	codeExecReverted   = 3      // execution reverted (EIP-838)
)

// jsonRPCAnalysis holds parsed JSON-RPC response fields.
type jsonRPCAnalysis struct {
	hasResult    bool
	resultIsNull bool
	hasError     bool
	errorCode    int64
	errorMessage string
	// errorData is the error's `data` member, raw. CometBFT puts the reason
	// there and leaves `message` at the constant "Internal error", so a
	// classifier that reads `message` alone learns nothing from a CometBFT
	// node.
	errorData string
	hasID     bool
	idValue   string
}

// errorText is the text a classifier should match wordings against: the
// message, followed by the data member when there is one. Either field may be
// where a node or a proxy in front of it wrote the reason.
func (a jsonRPCAnalysis) errorText() string {
	if a.errorData == "" {
		return a.errorMessage
	}
	if a.errorMessage == "" {
		return a.errorData
	}
	return a.errorMessage + ": " + a.errorData
}

// parseJSONRPC parses a JSON-RPC response using gjson, operating directly on
// the byte slice — no string copy of the (potentially multi-MB) body.
// Returns the analysis and whether the body is valid JSON-RPC.
func parseJSONRPC(body []byte) (jsonRPCAnalysis, bool) {
	// Must be a JSON object.
	if !gjson.ValidBytes(body) {
		return jsonRPCAnalysis{}, false
	}

	parsed := gjson.ParseBytes(body)
	if parsed.Type != gjson.JSON {
		return jsonRPCAnalysis{}, false
	}

	// Check for jsonrpc field (standard JSON-RPC responses have this).
	jsonrpcField := parsed.Get("jsonrpc")

	// Check for result and error fields.
	resultField := parsed.Get("result")
	errorField := parsed.Get("error")

	// If no jsonrpc, result, or error field, it's not JSON-RPC.
	if !jsonrpcField.Exists() && !resultField.Exists() && !errorField.Exists() {
		return jsonRPCAnalysis{}, false
	}

	analysis := jsonRPCAnalysis{
		hasResult:    resultField.Exists(),
		resultIsNull: resultField.Exists() && resultField.Type == gjson.Null,
		hasError:     errorField.Exists() && errorField.Type != gjson.Null,
	}

	if analysis.hasError {
		// Scan only the error subtree, not the whole body again.
		analysis.errorCode = errorField.Get("code").Int()
		analysis.errorMessage = errorField.Get("message").String()
		analysis.errorData = errorField.Get("data").String()
	}

	idField := parsed.Get("id")
	if idField.Exists() {
		analysis.hasID = true
		analysis.idValue = idField.Raw
	}

	return analysis, true
}

// classifyJSONRPCError classifies a JSON-RPC error code and message.
func classifyJSONRPCError(code int64, message string) AnalysisResult {
	lowerMsg := strings.ToLower(message)

	switch {
	// Execution reverted — client's fault (bad call data or contract logic).
	case code == codeExecReverted:
		return AnalysisResult{
			ShouldRetry:        false,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrClient,
			Confidence:         0.95,
			Reason:             "execution_reverted",
			Details:            "execution reverted: " + message,
		}

	// Method not found — client asked for unsupported method.
	case code == codeMethodNotFound:
		return AnalysisResult{
			ShouldRetry:        false,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrClient,
			Confidence:         0.95,
			Reason:             ReasonMethodNotFound,
			Details:            "method not found: " + message,
			MethodBlocking:     true,
		}

	// Invalid params — client's fault.
	case code == codeInvalidParams:
		return AnalysisResult{
			ShouldRetry:        false,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrClient,
			Confidence:         0.95,
			Reason:             "invalid_params",
			Details:            "invalid params: " + message,
		}

	// Invalid request — client's fault.
	case code == codeInvalidRequest:
		return AnalysisResult{
			ShouldRetry:        false,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrClient,
			Confidence:         0.90,
			Reason:             "invalid_request",
			Details:            "invalid request: " + message,
		}

	// Parse error — the supplier couldn't parse what we sent, but could also be a broken supplier.
	case code == codeParseError:
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.70,
			Reason:             "parse_error",
			Details:            "parse error: " + message,
		}

	// Server error range (-32000 to -32099) — needs message inspection.
	case code == codeServerError || (code <= -32000 && code >= -32099):
		return classifyServerError(code, lowerMsg)

	// Internal error — inspect message for supplier vs blockchain attribution.
	case code == codeInternalError:
		return classifyInternalError(lowerMsg)

	// A code outside the spec saying the state is gone is still the node
	// answering: a proxy in front of pruned nodes answers 4444 "pruned history
	// unavailable" (mainnet base, 2026-09-26), and grading that by its code
	// penalized it for the truth and never marked it non-archival.
	case ReportsMissingHistoricalState(message):
		return classifyServerError(code, lowerMsg)

	// Unknown error codes — default handling.
	default:
		return AnalysisResult{
			ShouldRetry:     true,
			ShouldPenalize:  true,
			PenaltySeverity: SeverityMinor,
			Attribution:     AttrUnknown,
			Confidence:      0.50,
			Reason:          "unknown_error_code",
			Details:         "unknown JSON-RPC error code " + strconv.FormatInt(code, 10) + ": " + message,
		}
	}
}

// methodUnsupportedPatterns are wordings in which an endpoint reports that it
// does not serve the METHOD asked for — a namespace it was started without,
// an API a lite node does not expose. Unlike the historical-state wordings
// below, which are about one block, these are about the method, so they set
// MethodBlocking: the host should not receive that method again for a while.
var methodUnsupportedPatterns = []string{
	"api is not supported",
	"lite fullnode",
	"method not supported",
	"does not exist/is not available",
	"is not available on this node",
}

// methodUnsupported is the verdict on a node saying it does not serve the
// method: retried elsewhere, nobody scored, and the method kept away from the
// host for a while.
func methodUnsupported(details string) AnalysisResult {
	return AnalysisResult{
		ShouldRetry:    true,
		Attribution:    AttrBlockchain,
		Confidence:     0.85,
		Reason:         "method_unsupported",
		Details:        "method not served by this endpoint (" + details + ")",
		MethodBlocking: true,
	}
}

// reportsMethodUnsupported reports whether message is one of the
// methodUnsupportedPatterns wordings. It does not fold case: it expects an
// already-lowercased message, which is how every caller has it.
func reportsMethodUnsupported(lowerMsg string) bool {
	return ContainsAnyOf(lowerMsg, methodUnsupportedPatterns)
}

// capabilityLimitationPatterns are the wordings in which an endpoint reports
// that it does not retain the historical state a request asked for. They are
// listed once, here, because two callers need the same answer: this tier, which
// must attribute them to the chain rather than to the supplier, and the EVM
// plugin, which demotes an endpoint out of the archival pool on seeing one.
// PATH kept two catalogues and they drifted — a wording recognised by the
// analyzer but missing from the archival list left pruned nodes marked archival
// and still receiving the requests they had just failed.
//
// Every vendor words this differently, so the entries are deliberately short
// prefixes: geth hash-scheme says "missing trie node", geth path-scheme (PBSS)
// says "metadata is not found, <block>", erigon "state not available", and
// gnosis/polygon "historical state is not available" / "historical state <hash>".
var capabilityLimitationPatterns = []string{
	"missing trie node",
	"metadata is not found",
	"state not available",
	"historical state",
	"has been pruned",
	"no state available for block",
	"no state found for block",
	"is pruned",
	"pruned history",
	"height is not available",
	"haven't been fully indexed",
	"not been fully indexed",
	"lite fullnode",
	"api is not supported",
}

// ReportsMissingHistoricalState reports whether an error message is an endpoint
// saying it does not retain the state the request asked for, rather than a
// fault. Matching is case-insensitive on the caller's behalf.
//
// The distinction matters twice over: such an endpoint must not be penalized
// for answering honestly, and it must not keep receiving archival requests it
// has already proved it cannot serve.
func ReportsMissingHistoricalState(message string) bool {
	return ContainsAnyOf(strings.ToLower(message), capabilityLimitationPatterns)
}

// blockchainErrorPatterns are error wordings attributable to the chain rather
// than to the supplier serving it. The capability-limitation half is shared
// with the archival demotion path — a supplier that cannot serve
// historical/pruned state is a capability mismatch, so retry elsewhere but do
// not penalize. Built once rather than per call: this runs on every error
// response.
var blockchainErrorPatterns = append([]string{
	"block not found",
	"header not found",
	"unknown block",
	"transaction not found",
	"receipt not found",
	// Solana -32010 "<key> excluded from account secondary indexes; this RPC
	// method unavailable for key": the node was started without a secondary
	// account index for that program, so it cannot serve getProgramAccounts
	// for it while another operator serves the identical call from its index.
	// Node configuration, not a fault — and not historical state either, so
	// it lives here rather than in capabilityLimitationPatterns, which the EVM
	// archival demotion path also reads.
	"excluded from account secondary indexes",
	// Solana: a skipped slot has no block on any node, and a slot an RPC node
	// keeps no long-term storage for is its retention, not a fault.
	"was skipped",
	"block not available for slot",
	// Retention and limits one node has and another may not: retried,
	// nobody scored. Solana "transaction history is not available from this
	// node" and "block N cleaned up, does not exist on node"; polygon
	// "state 0x… is not available"; a node's own eth_getLogs range cap;
	// a Cosmos node behind the height asked for.
	"transaction history is not available",
	"cleaned up, does not exist",
	"state 0x",
	"block range exceeds",
	"must be less than or equal to",
}, capabilityLimitationPatterns...)

// heightAheadRe reads CometBFT's wording for a height past the node's head.
var heightAheadRe = regexp.MustCompile(`height (\d+) must be less than or equal to the current blockchain height (\d+)`)

// heightAheadRetry is how far past a node's head a requested height may be for
// another node to hold it already. ponytail: a fixed block count; scale by the
// chain's block time if a fast chain needs more.
const heightAheadRetry = 10

// heightJustAhead reports whether lowerMsg says the requested height is past
// the node's head by at most heightAheadRetry blocks.
func heightJustAhead(lowerMsg string) bool {
	if !strings.Contains(lowerMsg, "current blockchain height") {
		return false
	}
	m := heightAheadRe.FindStringSubmatch(lowerMsg)
	if m == nil {
		return false
	}
	asked, err1 := strconv.ParseInt(m[1], 10, 64)
	head, err2 := strconv.ParseInt(m[2], 10, 64)
	return err1 == nil && err2 == nil && asked > head && asked-head <= heightAheadRetry
}

// supplierInfraPatterns are the wordings of a supplier's own infrastructure
// failing, in either the -32000 range or -32603.
var supplierInfraPatterns = []string{
	"service unavailable",
	"bad gateway",
	"gateway timeout",
	"connection refused",
	"internal server error",
}

// clientErrorPatterns are -32000-range wordings about the request itself: a
// call that reverts, a transaction the chain would refuse. Every node answers
// them alike, so they are the client's answer: delivered, not retried, nobody
// scored. Many nodes give a revert code 3 (handled above); geth without
// revert data, and the Opera/Sonic clients always, say -32000 "execution
// reverted". On mainnet (2026-10-03) these fell to the unscored server_error
// default, which retries: on fantom 63 attempts a second went to other
// operators for an answer they all give, and every one read as a supplier
// fault on the dashboards.
var clientErrorPatterns = []string{
	// EVM execution
	"execution reverted",
	"out of gas",
	"gas required exceeds",
	"invalid opcode",
	"invalid jump destination",
	"stack underflow",
	"stack limit reached",
	"evm error: invalid", // reth/erigon: invalidfeopcode, invalidjump, …
	"execution unsuccessful",
	// transaction validity (geth core and txpool)
	"insufficient funds",
	"insufficient balance",
	"nonce too low",
	"nonce too high",
	"already known",
	"known transaction",
	"underpriced",
	"intrinsic gas too low",
	"exceeds block gas limit",
	"max fee per gas less than block base fee",
	"max priority fee per gas higher than max fee per gas",
	"tip higher than fee cap",
	"fee cap less than block base fee",
	"invalid sender",
	"transaction type not supported",
	"oversized data",
	"exceeds the configured cap",
	"only replay-protected",
	"rlp:",
	// request state the client holds or asked for wrongly
	"filter not found",
	"invalid block range params",
	// Solana
	"transaction simulation failed",
	"signature verification failure",
	"blockhash not found",
	"transaction version",
}

// supplierLagPatterns are a node saying it is behind or unwell: the supplier's
// state, retried elsewhere and scored minor, like its rate limit. Solana's
// -32005 "node is behind" / "node is unhealthy" and -32016 "minimum context
// slot has not been reached".
// A proxy in front of the supplier's nodes saying it has none to send to
// ("all suitable upstream rpc backends unavailable") is the same: the
// supplier's own capacity, not the request.
var supplierLagPatterns = []string{
	"node is behind",
	"node is unhealthy",
	"minimum context slot has not been reached",
	"upstream rpc backends",
}

// ReasonRateLimited is the verdict on a node's own rate limiter or admission
// limit answering instead of the node.
const ReasonRateLimited = "rate_limited"

// ReasonQuotaExceeded is the verdict on a rate limit that is a metered
// upstream plan's quota: the supplier is reselling a third-party provider's
// tier and has run through it.
const ReasonQuotaExceeded = "quota_exceeded"

// quotaPatterns are what set a quota apart from a busy node's rate limit: a
// metered unit, or the counters a metered provider's limiter reports. Matched
// against the message and the error's data together (errorText), which is
// where those counters are: mainnet robinhood, 2026-10-03, -32029 "rate limit
// exceeded" with data {"limit":60,"remaining":0,"unit":"cu_per_minute",
// "retry_after_ms":43000}. A busy node clears in a moment and stays minor; a
// spent quota answers the same way for the rest of its window, so it is
// graded major and feeds the failure rate.
var quotaPatterns = []string{
	"cu_per_",
	"compute units",
	"retry_after_ms",
	`"remaining":`,
}

// quotaLimitPatterns are the limit wordings a quota comes with. Without one,
// a quota marker alone is not a refusal ("too busy" is a queue, never a
// quota).
var quotaLimitPatterns = []string{"rate limit", "too many requests", "exceeded", "quota"}

// classifyServerError handles -32000 range errors which are commonly blockchain-specific.
func classifyServerError(code int64, lowerMsg string) AnalysisResult {
	if ContainsAnyOf(lowerMsg, clientErrorPatterns) {
		return AnalysisResult{
			Attribution: AttrClient,
			Confidence:  0.90,
			Reason:      "client_error",
			Details:     "client error (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
		}
	}
	if ContainsAnyOf(lowerMsg, supplierLagPatterns) {
		return AnalysisResult{
			ShouldRetry:     true,
			ShouldPenalize:  true,
			PenaltySeverity: SeverityMinor,
			Attribution:     AttrSupplier,
			Confidence:      0.85,
			Reason:          "node_behind",
			Details:         "node behind or unhealthy (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
		}
	}
	if ContainsAnyOf(lowerMsg, blockchainErrorPatterns) {
		result := AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrBlockchain,
			Confidence:         0.85,
			Reason:             "blockchain_error",
			Details:            "blockchain error (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
		}
		result.MethodBlocking = reportsMethodUnsupported(lowerMsg)
		return result
	}

	// Supplier-attributed errors at -32000.
	if ContainsAnyOf(lowerMsg, supplierInfraPatterns) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "supplier_server_error",
			Details:            "supplier server error (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
		}
	}

	// A metered plan's quota, relayed: see quotaPatterns. Checked before the
	// plain rate limit, whose wording it shares.
	if ContainsAnyOf(lowerMsg, quotaLimitPatterns) && ContainsAnyOf(lowerMsg, quotaPatterns) {
		return AnalysisResult{
			ShouldRetry:     true,
			ShouldPenalize:  true,
			PenaltySeverity: SeverityMajor,
			Attribution:     AttrSupplier,
			Confidence:      0.9,
			Reason:          ReasonQuotaExceeded,
			Details:         "supplier quota exceeded (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
		}
	}

	// The node's own rate limiter or admission limit: the supplier's
	// capacity, not the request. Scored like a relay miner's 429
	// (upstream_429), minor and retried. "server too busy" is a node whose
	// pending-request queue is full (mainnet sei, 2026-10-01: "rejecting new
	// request (pending: 803, threshold: 800)"); it fell to the unscored
	// default below.
	if ContainsAnyOf(lowerMsg, []string{"rate limit", "too many requests", "too busy"}) {
		return AnalysisResult{
			ShouldRetry:     true,
			ShouldPenalize:  true,
			PenaltySeverity: SeverityMinor,
			Attribution:     AttrSupplier,
			Confidence:      0.85,
			Reason:          ReasonRateLimited,
			Details:         "supplier rate limit (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
		}
	}

	// A method the node does not serve, in a wording that is not also a
	// blockchain-error pattern (those were handled above). The node answered
	// correctly about itself; it is not at fault, and it must not receive
	// that method again for a while.
	if reportsMethodUnsupported(lowerMsg) {
		return methodUnsupported("code " + strconv.FormatInt(code, 10) + ": " + lowerMsg)
	}

	// Default for the server error range: the node answered the request with
	// an error of its own, in a wording that is neither a known chain error
	// nor an infrastructure failure (both handled above). Retry, since another
	// node may answer, but do not score it: a node's own JSON-RPC answer is
	// not the supplier failing. On mainnet sei (2026-09-15) this branch
	// charged suppliers 8,400 minor errors an hour.
	return AnalysisResult{
		ShouldRetry: true,
		Attribution: AttrUnknown,
		Confidence:  0.60,
		Reason:      "server_error",
		Details:     "server error (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
	}
}

// classifyInternalError handles -32603 (internal error) with message-based classification.
func classifyInternalError(lowerMsg string) AnalysisResult {
	// Supplier infrastructure errors.
	if ContainsAnyOf(lowerMsg, supplierInfraPatterns) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "supplier_internal_error",
			Details:            "supplier internal error: " + lowerMsg,
		}
	}

	// A node a few blocks behind the height asked for: CometBFT answers a
	// block the node has not reached yet with -32603 "height N must be less
	// than or equal to the current blockchain height M". When N is within
	// heightAheadRetry of M, another node may already hold it, so it is
	// retried and nobody is scored. Delivered without a retry it was about
	// 600 an hour on mainnet osmosis (2026-10-05). A height far past the head
	// is the client's miss, which every node answers alike: passed through.
	if heightJustAhead(lowerMsg) {
		return AnalysisResult{
			ShouldRetry: true,
			Attribution: AttrBlockchain,
			Confidence:  0.85,
			Reason:      "blockchain_error",
			Details:     "node behind the height asked for: " + lowerMsg,
		}
	}

	// A wording the indicator table knows keeps that table's reason and
	// attribution. Supplier wordings (a timeout) keep their retry and
	// penalty. Blockchain wordings — a pruned height, a missing block, state
	// not available — are the node's answer and are passed through with no
	// retry and no penalty, under their own reason so they stay countable:
	// on a CometBFT node "height N is not available, lowest height is M" is
	// a pruned node answering an archival query. It was retried for a day
	// (5bae566): on the canary every retry on akash, shentu and persistence
	// exhausted, zero recovered, because the other operators prune too, and
	// the three services paid 31–42% more relays for it. Serving those
	// queries needs to know which endpoint holds which heights, which is
	// selection's job, not a retry's.
	if ind := matchIndicator([]byte(lowerMsg)); ind != nil {
		ind.Details = "internal error, " + ind.Details
		if ind.Attribution == AttrBlockchain {
			ind.ShouldRetry = false
		}
		return *ind
	}

	// Anything else is the node's own answer to the request, and it is
	// delivered as such: no retry, no penalty. CometBFT wraps every handler
	// error as -32603 with `message` fixed at "Internal error" and the reason
	// in `data` — "tx (…) not found", "transaction indexing is disabled", a
	// height above the chain head — so on a CometBFT service this code is the
	// ordinary shape of "your request could not be served", not a broken
	// node. Until 2026-09-13 this branch retried with a major penalty, which
	// on the canary scored shentu's only comet_bft operator to 0 for
	// answering client misses correctly and paid for every retry that then
	// received the same body. PATH passes a -32603 through unless the message
	// names supplier infrastructure (the patterns above); this matches it.
	return AnalysisResult{
		ShouldRetry:        false,
		ShouldCircuitBreak: false,
		ShouldPenalize:     false,
		PenaltySeverity:    SeverityNone,
		Attribution:        AttrBlockchain,
		Confidence:         0.60,
		Reason:             "internal_error",
		Details:            "node reported an internal error for this request: " + lowerMsg,
	}
}
