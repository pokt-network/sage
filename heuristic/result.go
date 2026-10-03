package heuristic

import "fmt"

// ErrorAttribution identifies who is at fault for a failed response.
type ErrorAttribution int

// AttrUnknown is the zero value on purpose. An AnalysisResult built without
// naming an attribution used to mean AttrSupplier — penalise, and maybe
// break — which is the one default that must never be reached by accident.
// Every literal in this package sets the field; the zero value is for the
// one that will not.
const (
	// AttrUnknown means the cause is ambiguous — minor penalty at most.
	AttrUnknown ErrorAttribution = iota
	// AttrSupplier means the supplier is at fault — penalize and potentially circuit-break.
	AttrSupplier
	// AttrBlockchain means the blockchain itself had an issue — retry, but no penalty.
	AttrBlockchain
	// AttrClient means the client sent a bad request — no retry, no penalty.
	AttrClient
)

func (a ErrorAttribution) String() string {
	switch a {
	case AttrSupplier:
		return "supplier"
	case AttrBlockchain:
		return "blockchain"
	case AttrClient:
		return "client"
	case AttrUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// Severity constants for PenaltySeverity.
const (
	SeverityNone     = ""
	SeverityMinor    = "minor"
	SeverityMajor    = "major"
	SeverityCritical = "critical"
	SeverityFatal    = "fatal"
)

// AnalysisResult captures the outcome of analyzing a relay response.
// ShouldRetry and ShouldCircuitBreak are independent decisions:
// a response can warrant retry without circuit-breaking (e.g., 429 rate limit),
// or circuit-breaking without retry (e.g., persistent fabricated responses).
type AnalysisResult struct {
	// ShouldRetry indicates the request should be retried on a different endpoint.
	ShouldRetry bool
	// ShouldCircuitBreak indicates the supplier's domain should be circuit-broken.
	// This has a much higher bar than ShouldRetry.
	ShouldCircuitBreak bool
	// ShouldPenalize indicates the supplier's reputation should be penalized.
	ShouldPenalize bool
	// PenaltySeverity is the severity of the penalty: "minor", "major", "critical", "fatal".
	PenaltySeverity string
	// Attribution identifies who is at fault.
	Attribution ErrorAttribution
	// Confidence is how confident the analysis is (0.0 to 1.0).
	Confidence float64
	// Reason is a short machine-readable reason code.
	Reason string
	// Details provides human-readable context for debugging.
	Details string
	// MethodBlocking indicates the endpoint could not serve THIS METHOD — it
	// timed out after accepting the connection, or answered that the method
	// is not available on it — and should not receive that method again for
	// a while. It is deliberately not set for missing historical state (per
	// block, owned by archival tri-state) or Solana's per-key index exclusion
	// (per program), because those are not "cannot do this method".
	MethodBlocking bool
	// HeadLag is how many blocks behind the perceived head a stale_response
	// answer named; zero on every other verdict. Retry reads it to keep the
	// fresher of two stale answers.
	HeadLag uint64
}

// ReasonRefusedRecent is the verdict on an answer claiming the state or
// history of a block too recent for any node to have discarded: a refusal
// worded as a prune.
const ReasonRefusedRecent = "refused_recent"

// RefusedRecent is the verdict for such an answer: the supplier's, a major
// penalty, retried on another party. It is not a method block by itself; the
// method_block_refusal flag makes it one, as method_block_408 does for a 408.
func RefusedRecent(details string) AnalysisResult {
	return AnalysisResult{
		ShouldRetry:     true,
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMajor,
		Attribution:     AttrSupplier,
		Confidence:      0.9,
		Reason:          ReasonRefusedRecent,
		Details:         details,
	}
}

// ReasonStaleResponse is the verdict on an answer naming a chain head too far
// behind the perceived one (qos.HeadLagReader, featureflag.FlagStaleResponse).
const ReasonStaleResponse = "stale_response"

// StaleResponse is the verdict for an answer lag blocks behind the head: the
// supplier's (a node or a cache serving an old view), a major penalty so it
// feeds the failure rate, and retried on another party. Not a method block
// and not a circuit break: the same host answers other calls, and a fresh
// answer from it tomorrow is as good as anyone's.
func StaleResponse(lag uint64) AnalysisResult {
	return AnalysisResult{
		ShouldRetry:     true,
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMajor,
		Attribution:     AttrSupplier,
		Confidence:      0.9,
		Reason:          ReasonStaleResponse,
		Details:         fmt.Sprintf("head answer %d blocks behind the perceived head", lag),
		HeadLag:         lag,
	}
}

// ReasonInvalidResult is the verdict on an answer whose result no node
// produces for its method (qos.ResultValidator, featureflag.FlagInvalidResult).
const ReasonInvalidResult = "invalid_result"

// InvalidResult is the verdict for such an answer: the supplier's (a layer in
// front of the node rewrote it), a major penalty, retried on another party.
func InvalidResult(details string) AnalysisResult {
	return AnalysisResult{
		ShouldRetry:     true,
		ShouldPenalize:  true,
		PenaltySeverity: SeverityMajor,
		Attribution:     AttrSupplier,
		Confidence:      0.95,
		Reason:          ReasonInvalidResult,
		Details:         details,
	}
}

// ReasonSuccess is the Reason a verdict carries when the response passed every
// check. Callers ask IsSuccess rather than comparing against it.
const ReasonSuccess = "success"

// ReasonMethodNotFound is the Reason for a JSON-RPC -32601: the host does not
// serve the method. The analyzer does not retry it (a bogus method name must
// not bounce across the pool); the heuristic middleware does, once the
// method is known to be a real one (see relay/middleware.Heuristic).
const ReasonMethodNotFound = "method_not_found"

// ReasonConnectTimeout is the Reason for a dial our own deadline ended before
// the host accepted (heuristic.AnalyzeTransportError). With its full budget
// it is a host that cannot take a connection, graded like any connect
// failure; with a fraction of it, it measures the time other attempts spent,
// and the relay middleware takes the penalty and the breaker vote off.
const ReasonConnectTimeout = "transport_connect_timeout"

// IsSuccess reports whether the verdict is the analyzer passing the response.
//
// It exists because a caller cannot key on attribution alone to tell an
// answered request from a client error: the success verdict is AttrClient too
// (see successResult), so "client-attributed" covers both the request the
// client got wrong and the one the endpoint answered correctly.
func (r AnalysisResult) IsSuccess() bool {
	return r.Reason == ReasonSuccess
}

// successResult returns an AnalysisResult for a successful response.
func successResult() AnalysisResult {
	return AnalysisResult{
		Attribution: AttrClient, // not really "client's fault" — just means no action needed
		Confidence:  1.0,
		Reason:      ReasonSuccess,
		Details:     "valid response",
	}
}
