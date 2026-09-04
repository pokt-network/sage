package heuristic

import (
	"fmt"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// Analyze performs tiered analysis of a relay response to determine retry, circuit-break,
// and penalty decisions. The tiers short-circuit: if a tier produces a high-confidence
// result, later tiers are skipped.
//
// Tiers:
//
//	0: HTTP status code
//	1: Structural (empty body, HTML, plain text)
//	2: Protocol (JSON-RPC parsing and error code classification)
//	3: Indicator patterns (content matching)
func Analyze(response []byte, httpStatusCode int, rpcType domain.RPCType) AnalysisResult {
	// Over-servicing rejections must be checked before the HTTP status tier:
	// the signal arrives on both HTTP 200 (poktroll relay-miner) and HTTP 429
	// (HA relay-miner), and a 429 would otherwise be classified as a penalizable
	// rate limit. The supplier correctly enforced its per-session allocation —
	// do not penalize or circuit-break it.
	if isOverServiced(response) {
		return overServicedResult()
	}

	// Tier 0: HTTP status code.
	if result, done := analyzeTier0(httpStatusCode, response, rpcType); done {
		return result
	}

	// gRPC replies are framed protobuf, and every tier below reads the body as
	// text: IsPlainText returns true for anything not starting with '{', '['
	// or '<', so a perfectly good protobuf answer trips "plain_text_response",
	// gets retried across suppliers, and penalizes each one for answering
	// correctly. A gRPC call reports its own outcome in grpc-status, which the
	// transport has already turned into an error before reaching here.
	if rpcType == domain.RPCTypeGRPC {
		return analyzeGRPC(response)
	}

	// Tier 1: Structural checks.
	if result, done := analyzeTier1(response, httpStatusCode); done {
		return result
	}

	// Tier 2: Protocol-level analysis.
	if result, done := analyzeTier2(response, rpcType); done {
		return result
	}

	// Tier 3: Indicator pattern matching.
	if result := matchIndicator(response); result != nil {
		return *result
	}

	// No issues detected — treat as success.
	return successResult()
}

// analyzeTier0 checks HTTP status codes.
func analyzeTier0(statusCode int, response []byte, rpcType domain.RPCType) (AnalysisResult, bool) {
	switch {
	case statusCode >= 500:
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.90,
			Reason:             "http_5xx",
			Details:            fmt.Sprintf("HTTP %d server error", statusCode),
		}, true

	case statusCode == 429:
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false, // rate limit — not broken, just busy
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityMinor,
			Attribution:        AttrSupplier,
			Confidence:         0.95,
			Reason:             "http_429",
			Details:            "rate limited",
		}, true

	// A JSON-RPC server reports a client's mistake inside a JSON-RPC
	// envelope, whatever status it puts on it. A 4xx carrying anything else —
	// an HTML 404 page, an empty body — is the supplier's HTTP layer answering
	// for a backend that never saw the request: a misrouted vhost, a proxy
	// with no upstream. That is the supplier's, and another supplier will
	// answer; the host should not see this method again for a while.
	//
	// On a REST or CometBFT request only a 401 or 403 without JSON is read
	// the same way. The gateway signs every relay, so a client cannot be
	// refused access: a plain-text "Access Denied" is the supplier's front
	// door, not the chain's API. A non-JSON 404 stays the client's — it is
	// what asking for a path that does not exist looks like. Until
	// 2026-09-04 every REST 4xx was the client's, so a supplier answering
	// 403 to valid requests was graded by probes alone and client traffic
	// never moved its score — the canary's eth-beacon case.
	case statusCode >= 400 && statusCode < 500 && frontDoorRefusal(rpcType, statusCode) && !gjson.ValidBytes(response):
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityMinor,
			MethodBlocking:     true,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "http_4xx_page",
			Details:            fmt.Sprintf("HTTP %d with a non-JSON body on a %s request", statusCode, rpcType),
		}, true

	case statusCode >= 400 && statusCode < 500:
		return AnalysisResult{
			ShouldRetry:        false,
			ShouldCircuitBreak: false,
			ShouldPenalize:     false,
			Attribution:        AttrClient,
			Confidence:         0.85,
			Reason:             "http_4xx",
			Details:            fmt.Sprintf("HTTP %d client error", statusCode),
		}, true

	case statusCode >= 200 && statusCode < 300:
		// 2xx — continue to body analysis; status alone doesn't mean success.
		return AnalysisResult{}, false

	default:
		// Unexpected status codes (1xx, 3xx) — no strong signal.
		return AnalysisResult{}, false
	}
}

// isBodylessStatus reports whether an HTTP status is defined to carry no
// response body.
func isBodylessStatus(code int) bool {
	return code == http.StatusNoContent ||
		code == http.StatusResetContent ||
		code == http.StatusNotModified
}

// analyzeTier1 performs structural checks on the response body.
// analyzeGRPC is the whole of response analysis for gRPC. There is no text to
// inspect and no JSON-RPC envelope to classify — a unary reply is one
// length-prefixed frame, so the only thing the body can be wrong about is not
// being there at all.
func analyzeGRPC(body []byte) AnalysisResult {
	if IsEmpty(body) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityMajor,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "empty_response",
			Details:            "gRPC response carried no message frame",
		}
	}
	return successResult()
}

func analyzeTier1(body []byte, httpStatusCode int) (AnalysisResult, bool) {
	if IsEmpty(body) {
		// 204, 205 and 304 are defined to carry no body, so an empty payload on
		// one of them is the endpoint behaving correctly. Only a status that
		// promised content can have failed to deliver it — and that promise is
		// what the penalty below is for.
		if isBodylessStatus(httpStatusCode) {
			return successResult(), true
		}

		// Critical, not major: no RPC type SAGE forwards has a valid zero-length
		// response, and the relay is signed and settleable regardless of what
		// the supplier put in the body. That makes an empty payload a protocol
		// violation rather than a bad moment.
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.85,
			Reason:             "empty_response",
			Details:            "response body is empty",
		}, true
	}

	if IsHTML(body) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityCritical,
			Attribution:        AttrSupplier,
			Confidence:         0.90,
			Reason:             "html_response",
			Details:            "response is HTML (likely proxy error page)",
		}, true
	}

	if IsPlainText(body) {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: false,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityMajor,
			Attribution:        AttrSupplier,
			Confidence:         0.75,
			Reason:             "plain_text_response",
			Details:            "response is plain text (not JSON)",
		}, true
	}

	return AnalysisResult{}, false
}

// analyzeTier2 performs JSON-RPC protocol analysis.
func analyzeTier2(body []byte, rpcType domain.RPCType) (AnalysisResult, bool) {
	analysis, isJSONRPC := parseJSONRPC(body)
	if !isJSONRPC {
		// Not JSON-RPC — could be valid REST/CometBFT or genuinely broken.
		// For CometBFT, non-JSON-RPC responses are expected for some endpoints.
		if rpcType == domain.RPCTypeCometBFT || rpcType == domain.RPCTypeREST {
			return AnalysisResult{}, false
		}
		// For JSON-RPC type, a non-JSON-RPC body is suspicious but handled by Tier 1/3.
		return AnalysisResult{}, false
	}

	// Both result (non-null) AND error present — fabricated/corrupted response.
	if analysis.hasResult && !analysis.resultIsNull && analysis.hasError {
		return AnalysisResult{
			ShouldRetry:        true,
			ShouldCircuitBreak: true,
			ShouldPenalize:     true,
			PenaltySeverity:    SeverityFatal,
			Attribution:        AttrSupplier,
			Confidence:         0.95,
			Reason:             "fabricated_response",
			Details:            "response has both non-null result and error (fabricated)",
		}, true
	}

	// Error-only response (or result:null + error, which is a valid error-only pattern).
	if analysis.hasError {
		result := classifyJSONRPCError(analysis.errorCode, analysis.errorMessage)
		return result, true
	}

	// result:null without error — for CometBFT this can be normal.
	if analysis.resultIsNull && !analysis.hasError {
		if rpcType == domain.RPCTypeCometBFT {
			return successResult(), true
		}
		// For JSON-RPC, result:null is a valid response (e.g., eth_getTransactionReceipt
		// for a pending tx). Not an error.
		return successResult(), true
	}

	// Valid result present, no error — success.
	if analysis.hasResult && !analysis.hasError {
		// CometBFT awareness: empty object result {} is valid (e.g., health check).
		return successResult(), true
	}

	return AnalysisResult{}, false
}

// frontDoorRefusal reports whether a 4xx with no JSON body is the supplier's
// HTTP layer rather than the client's mistake: any such 4xx on JSON-RPC
// (the backend would have answered in an envelope), and an access refusal
// on REST or CometBFT, where a wrong path is legitimately a bare 404.
func frontDoorRefusal(rpcType domain.RPCType, statusCode int) bool {
	switch rpcType {
	case domain.RPCTypeJSONRPC:
		return true
	case domain.RPCTypeREST, domain.RPCTypeCometBFT:
		return statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden
	}
	return false
}
