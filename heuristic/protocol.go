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

// classifyServerError handles -32000 range errors which are commonly blockchain-specific.
func classifyServerError(code int64, lowerMsg string) AnalysisResult {
	// Blockchain-attributed errors: these are real blockchain conditions, not supplier faults.
	blockchainPatterns := []string{
		"block not found",
		"header not found",
		"missing trie node",
		"unknown block",
		"transaction not found",
		"receipt not found",
		"state not available",
		// Archival/capability-limitation: supplier can't serve historical/pruned
		// state. Retry elsewhere, but no penalty — it's a capability mismatch, not
		// a fault.
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
	for _, pattern := range blockchainPatterns {
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
