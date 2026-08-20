package heuristic

// indicator represents a content pattern that hints at the cause of an error.
type indicator struct {
	pattern     string
	attribution ErrorAttribution
	shouldRetry bool
	severity    string
	reason      string
}

// indicators are Tier 3 content patterns checked when Tier 2 didn't give high confidence.
// Order matters: first match wins.
//
// The capability-limitation entries below duplicate capabilityLimitationPatterns
// (protocol.go) by design: Tier 2 matches a parsed JSON-RPC error message, this
// tier scans a body that never parsed. A wording added there belongs here too.
var indicators = []indicator{
	// Blockchain-attributed: the chain itself is having issues.
	{pattern: "missing trie node", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "missing_trie_node"},
	{pattern: "node is unhealthy", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "node_unhealthy"},
	{pattern: "block not found", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "block_not_found"},
	{pattern: "header not found", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "header_not_found"},
	{pattern: "state not available", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "state_not_available"},
	{pattern: "pruned state", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "pruned_state"},
	// geth's path-based state scheme (PBSS) words a pruned-state miss as
	// "metadata is not found, <block>" — no "prune" and no "trie" in it, so
	// every pattern above misses it and an honest answer reads as a fault.
	{pattern: "metadata is not found", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "pbss_pruned_state"},
	// Archival/capability-limitation variants: the supplier correctly reports it
	// can't serve historical/pruned state. Retry on an archival supplier, but do
	// NOT penalize or circuit-break — punishing a non-archival supplier for a
	// capability mismatch is wrong.
	{pattern: "historical state", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "historical_state"},
	{pattern: "state has been pruned", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "state_pruned"},
	{pattern: "block has been pruned", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "block_pruned"},
	{pattern: "is pruned", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "pruned"},
	{pattern: "height is not available", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "height_not_available"},
	{pattern: "haven't been fully indexed", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "not_indexed"},
	{pattern: "not been fully indexed", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "not_indexed"},
	// Capability limitation (e.g., Tron lite fullnodes that don't expose an API).
	{pattern: "lite fullnode", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "lite_fullnode"},
	{pattern: "api is not supported", attribution: AttrBlockchain, shouldRetry: true, severity: SeverityNone, reason: "api_not_supported"},

	// Supplier-attributed: the supplier's infrastructure is broken.
	{pattern: "connection refused", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "connection_refused"},
	{pattern: "connection reset", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "connection_reset"},
	{pattern: "timeout", attribution: AttrSupplier, shouldRetry: true, severity: SeverityMajor, reason: "timeout"},
	{pattern: "bad gateway", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "bad_gateway"},
	{pattern: "service unavailable", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "service_unavailable"},
	{pattern: "gateway timeout", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "gateway_timeout"},
	{pattern: "502 bad gateway", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "bad_gateway_502"},
	{pattern: "503 service unavailable", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "service_unavailable_503"},
	{pattern: "504 gateway timeout", attribution: AttrSupplier, shouldRetry: true, severity: SeverityCritical, reason: "gateway_timeout_504"},
}

// matchIndicator scans the response body for Tier 3 indicator patterns.
// Returns an AnalysisResult if a pattern matches, or nil if no match.
func matchIndicator(body []byte) *AnalysisResult {
	if len(body) == 0 {
		return nil
	}

	// Limit search to first 2KB to avoid scanning huge responses.
	searchArea := body
	if len(searchArea) > 2048 {
		searchArea = searchArea[:2048]
	}

	for _, ind := range indicators {
		if containsFold(searchArea, ind.pattern) {
			result := AnalysisResult{
				ShouldRetry:        ind.shouldRetry,
				ShouldCircuitBreak: ind.severity == SeverityCritical || ind.severity == SeverityFatal,
				ShouldPenalize:     ind.attribution == AttrSupplier,
				PenaltySeverity:    ind.severity,
				Attribution:        ind.attribution,
				Confidence:         0.60,
				Reason:             ind.reason,
				Details:            "indicator match: " + ind.pattern,
			}
			return &result
		}
	}

	return nil
}
