package heuristic

import (
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
	hasID        bool
	idValue      string
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
			Reason:             "method_not_found",
			Details:            "method not found: " + message,
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
	"state has been pruned",
	"block has been pruned",
	"is pruned",
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
	lower := strings.ToLower(message)
	for _, pattern := range capabilityLimitationPatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
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
}, capabilityLimitationPatterns...)

// classifyServerError handles -32000 range errors which are commonly blockchain-specific.
func classifyServerError(code int64, lowerMsg string) AnalysisResult {
	for _, pattern := range blockchainErrorPatterns {
		if strings.Contains(lowerMsg, pattern) {
			return AnalysisResult{
				ShouldRetry:        true,
				ShouldCircuitBreak: false,
				ShouldPenalize:     false,
				Attribution:        AttrBlockchain,
				Confidence:         0.85,
				Reason:             "blockchain_error",
				Details:            "blockchain error (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
			}
		}
	}

	// Supplier-attributed errors at -32000.
	supplierPatterns := []string{
		"service unavailable",
		"bad gateway",
		"gateway timeout",
		"connection refused",
		"internal server error",
	}
	for _, pattern := range supplierPatterns {
		if strings.Contains(lowerMsg, pattern) {
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
	}

	// Default for server error range: retry but only minor penalty.
	return AnalysisResult{
		ShouldRetry:     true,
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMinor,
		Attribution:     AttrUnknown,
		Confidence:      0.60,
		Reason:          "server_error",
		Details:         "server error (code " + strconv.FormatInt(code, 10) + "): " + lowerMsg,
	}
}

// classifyInternalError handles -32603 (internal error) with message-based classification.
func classifyInternalError(lowerMsg string) AnalysisResult {
	// Supplier infrastructure errors.
	supplierPatterns := []string{
		"service unavailable",
		"bad gateway",
		"gateway timeout",
		"connection refused",
		"internal server error",
	}
	for _, pattern := range supplierPatterns {
		if strings.Contains(lowerMsg, pattern) {
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
	}

	// Default for -32603: could be either side.
	return AnalysisResult{
		ShouldRetry:     true,
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMajor,
		Attribution:     AttrUnknown,
		Confidence:      0.55,
		Reason:          "internal_error",
		Details:         "internal error: " + lowerMsg,
	}
}
