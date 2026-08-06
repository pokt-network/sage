// Package heuristic grades a relay response: was it good, whose fault if not,
// and what should the gateway do about it.
//
// [Analyze] is the entry point. It returns an [AnalysisResult] carrying three
// decisions the rest of the gateway acts on — retry, circuit-break, penalize —
// plus an [ErrorAttribution] and a confidence.
//
// # The decisions are independent
//
// [AnalysisResult.ShouldRetry] and [AnalysisResult.ShouldCircuitBreak] are
// separate fields because they answer separate questions, and collapsing them
// is a production bug reproduced from PATH. A 429 is worth retrying elsewhere
// and worth nothing else; a stream of fabricated responses is worth removing
// the domain over even when retrying is pointless. Retry alone must never
// escalate into a domain-wide lockout — circuit-breaking is explicit opt-in,
// per response.
//
// # Attribution protects innocent suppliers
//
// Not every bad response is the supplier's doing. "execution reverted" and
// "block not found" are the *chain* answering correctly about a bad request;
// penalizing the endpoint that faithfully relayed them would route traffic
// away from healthy suppliers for doing their job. Every result therefore
// carries an attribution — [AttrSupplier], [AttrBlockchain], [AttrClient], or
// [AttrUnknown] — and only supplier-attributed failures reach reputation.
// Over-servicing rejections are the sharpest case: a supplier enforcing its
// own per-session allocation is behaving correctly, arrives as either HTTP 200
// or HTTP 429 depending on the relay miner, and is checked before anything
// else so the 429 is never mistaken for a rate limit worth punishing.
//
// # Tiers, cheapest first
//
// Analysis is layered and short-circuits: HTTP status (tier 0), structural
// checks on the raw bytes (tier 1), JSON-RPC protocol parsing (tier 2), then
// content indicator matching (tier 3). A high-confidence answer at any tier
// skips the rest, so the common case — a valid response — costs almost
// nothing.
//
// Tier 1 may use byte-pattern matching, because it is asking structural
// questions ("is this HTML?", "is the body empty?") of bytes that may not be
// JSON at all. Tier 2 and above parse with gjson. Reaching for byte matching
// at those tiers to save an allocation is how a substring inside an unrelated
// string field ends up classified as an error.
//
// gRPC bypasses the text tiers entirely. A protobuf reply does not begin with
// '{', '[' or '<', so the structural tier would read every correct gRPC
// response as plain text, retry it across suppliers, and penalize each one.
// gRPC reports its own outcome in grpc-status, which the transport has already
// converted to an error before analysis runs.
package heuristic
