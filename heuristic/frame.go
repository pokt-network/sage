package heuristic

import (
	"github.com/pokt-network/sage/domain"
)

// AnalyzeFrame performs tiered analysis of a WebSocket frame body.
// It differs from Analyze in that there is no HTTP status code — WS frames
// arrive post-handshake and carry no per-message transport status. Tier 0
// (HTTP status classification) is therefore skipped; structural, protocol,
// and indicator checks run in the same order as Analyze.
//
// Callers are responsible for scaling signal severity appropriately for
// per-frame use (e.g., downgrading Fatal to Critical so a single bad frame
// in a long-lived subscription doesn't permanently sink an endpoint).
func AnalyzeFrame(body []byte, rpcType domain.RPCType) AnalysisResult {
	if result, done := analyzeTier1(body); done {
		return result
	}
	if result, done := analyzeTier2(body, rpcType); done {
		return result
	}
	if result := matchIndicator(body); result != nil {
		return *result
	}
	return successResult()
}
