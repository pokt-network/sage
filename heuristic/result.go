package heuristic

// ErrorAttribution identifies who is at fault for a failed response.
type ErrorAttribution int

const (
	// AttrSupplier means the supplier is at fault — penalize and potentially circuit-break.
	AttrSupplier ErrorAttribution = iota
	// AttrBlockchain means the blockchain itself had an issue — retry, but no penalty.
	AttrBlockchain
	// AttrClient means the client sent a bad request — no retry, no penalty.
	AttrClient
	// AttrUnknown means the cause is ambiguous — minor penalty at most.
	AttrUnknown
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
}

// ReasonSuccess is the Reason a verdict carries when the response passed every
// check. Callers ask IsSuccess rather than comparing against it.
const ReasonSuccess = "success"

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
