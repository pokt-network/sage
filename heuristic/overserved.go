package heuristic

import "slices"

// overServedPatterns are wire signals that a supplier's relay-miner correctly
// rejected a relay because the application's per-(supplier, session) stake
// allocation is consumed. This is protocol-correct behavior — the supplier did
// the right thing — so it must NOT penalize reputation or circuit-break the
// domain. Two relay-miner implementations are covered:
//
//   - poktroll main: HTTP 200 + RelayMinerError{codespace="relayer_proxy", code=7}
//     and/or payload "offchain rate limit hit by relayer proxy".
//   - HA pocket-relay-miner: HTTP 429 + body
//     {"error":"session relay limit reached: claimable portion fully consumed"}.
//
// Detection is by response body because the signal arrives on both HTTP 200 and
// HTTP 429 — the status code alone is not a reliable discriminator.
var overServedPatterns = []string{
	"offchain rate limit hit by relayer proxy",
	"session relay limit reached",
	"claimable portion fully consumed",
}

// IsOverServiced reports whether the response body carries an over-servicing
// rejection signal. Scans only the first 2KB, consistent with matchIndicator.
func IsOverServiced(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	searchArea := body
	if len(searchArea) > 2048 {
		searchArea = searchArea[:2048]
	}
	return slices.ContainsFunc(overServedPatterns, func(p string) bool { return containsFold(searchArea, p) })
}

// ReasonOverServiced is the Reason for a protocol-correct over-servicing
// refusal (overServicedResult).
const ReasonOverServiced = "over_serviced"

// overServicedResult is the analysis outcome for a protocol-correct
// over-servicing rejection: retry on a different supplier (the per-session
// allocation is per (supplier, session), so another supplier may still serve),
// but never penalize or circuit-break the supplier that correctly enforced its
// allocation.
func overServicedResult() AnalysisResult {
	return AnalysisResult{
		ShouldRetry:        true,
		ShouldCircuitBreak: false,
		ShouldPenalize:     false,
		Attribution:        AttrClient, // not a supplier fault — expected protocol behavior, no action against the supplier
		Confidence:         0.95,
		Reason:             ReasonOverServiced,
		Details:            "supplier rejected: per-session relay allocation consumed (protocol-correct)",
	}
}

// MinerOverServiced reports whether a relay miner's own error report
// (RelayMinerError) is an over-servicing refusal: the poktroll miner's
// relayer_proxy code 7, or either miner's wording.
func MinerOverServiced(codespace string, code uint32, message string) bool {
	return (codespace == "relayer_proxy" && code == 7) || IsOverServiced([]byte(message))
}
