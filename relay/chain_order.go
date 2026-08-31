package relay

import (
	"fmt"
	"strings"
)

// Canonical middleware names. Use these constants when constructing a chain
// so the invariant validator can recognize each link.
const (
	MWShadow           = "shadow"
	MWTracing          = "tracing"
	MWTimeout          = "timeout"
	MWRequestID        = "request_id"
	MWClientIP         = "client_ip"
	MWMetrics          = "metrics"
	MWParse            = "parse"
	MWValidate         = "validate"
	MWCache            = "cache"
	MWBatch            = "batch"
	MWSingleflight     = "singleflight"
	MWObserve          = "observe"
	MWCrossValidate    = "cross_validate"
	MWRetry            = "retry"
	MWHedge            = "hedge"
	MWSupplierAffinity = "supplier_affinity"
	MWCircuitBreak     = "circuit_break"
	MWMethodBlocks     = "method_blocks"
	MWSelectEndpoint   = "select_endpoint"
	MWScore            = "score"
	MWDebugLog         = "debug_log"
	MWHeuristic        = "heuristic"
	MWSendRelay        = "send_relay"
)

// DefaultChainOrder is the middleware order used when a config does not set
// gateway_config.middleware_chain. It is the chain ARCHITECTURE.md documents,
// and every invariant in ValidateChainOrder holds for it (see the test).
//
// Returns a fresh slice per call so a caller cannot mutate the canonical order
// out from under the next one.
func DefaultChainOrder() []string {
	return []string{
		// --- Outermost (runs first) ---
		MWShadow,
		MWTracing,
		MWRequestID,
		MWClientIP,
		MWMetrics,
		MWParse,
		MWValidate,
		// After parse: the deadline is resolved per service, and the service
		// is what parse sets. Before cache/batch: the deadline must cover the
		// fan-out and every attempt inside it.
		MWTimeout,
		MWCache,
		MWBatch,
		MWSingleflight,
		MWObserve,
		MWCrossValidate,
		MWRetry,
		MWHedge,
		MWSupplierAffinity,
		MWCircuitBreak,
		MWMethodBlocks,
		MWSelectEndpoint,
		MWScore,
		MWDebugLog,
		MWHeuristic,
		MWSendRelay,
		// --- Innermost (runs last) ---
	}
}

// ValidateChainOrder enforces the load-bearing ordering invariants described
// in ARCHITECTURE.md. Ordering is outer-to-inner: earlier entries wrap later
// entries. A name not present in the list is treated as absent (the rules
// that reference it simply don't apply).
//
// Invariants:
//   - send_relay, if present, must be unique and the innermost (last) entry.
//   - parse must precede validate, cache, batch, singleflight,
//     cross_validate, select_endpoint, send_relay, heuristic
//     (they all read fields Parse sets: ServiceID, RPCType, Payloads).
//   - parse must precede timeout (timeout resolves its deadline per service,
//     which parse sets; before parse it resolves for service "" and no
//     per-service timeout_config or tuning override ever applies).
//   - timeout must precede batch and retry (the deadline has to cover the
//     fan-out and every attempt; inside either it would bound one sub-relay
//     or one attempt and let the request as a whole run on).
//   - select_endpoint must precede send_relay (SendRelay reads ctx.Endpoint).
//   - retry must precede select_endpoint (retry needs to re-select on each
//     attempt; if it were inside, rotation wouldn't work).
//   - hedge must precede select_endpoint (same reason: each racer picks its
//     own endpoint).
//   - circuit_break must precede select_endpoint (CB prunes domains before
//     selection; running after would let traffic reach broken endpoints).
//   - hedge must precede method_blocks (each hedge arm must honour and feed
//     method blocks, not just the primary attempt).
//   - method_blocks must precede select_endpoint (it prunes before
//     selection; running after would let traffic reach blocked methods).
//   - heuristic must precede send_relay (heuristic wraps the relay so it
//     can analyze ctx.Response).
//   - select_endpoint must precede score, and score must precede heuristic:
//     score records this attempt's endpoint with this attempt's verdict.
//   - retry, hedge and batch must all precede score — one attempt is one
//     scoring event, so a retried or hedged attempt that lost is still
//     scored, and a batch's per-attempt signals collapse in its sink.
//
// Duplicates of any canonical name are rejected — middlewares are singletons.
func ValidateChainOrder(names []string) error {
	// Index each canonical middleware. -1 means absent.
	idx := make(map[string]int, len(names))
	var dups []string
	for i, n := range names {
		if _, seen := idx[n]; seen {
			dups = append(dups, n)
			continue
		}
		idx[n] = i
	}
	if len(dups) > 0 {
		return fmt.Errorf("middleware chain has duplicates: %s", strings.Join(dups, ", "))
	}

	lookup := func(n string) int {
		if v, ok := idx[n]; ok {
			return v
		}
		return -1
	}

	// send_relay must be last.
	if sr := lookup(MWSendRelay); sr >= 0 && sr != len(names)-1 {
		return fmt.Errorf("middleware %q must be last (innermost); got position %d of %d", MWSendRelay, sr, len(names))
	}

	mustPrecede := []struct {
		before, after string
		reason        string
	}{
		{MWParse, MWValidate, "validate reads ServiceID set by parse"},
		{MWParse, MWCache, "cache keys on parsed method"},
		{MWParse, MWBatch, "batch decomposes parsed payloads"},
		{MWParse, MWSingleflight, "singleflight keys on parsed request"},
		{MWParse, MWCrossValidate, "cross_validate needs parsed payload"},
		{MWParse, MWSelectEndpoint, "select_endpoint reads ServiceID"},
		{MWParse, MWSendRelay, "send_relay needs parsed payload"},
		{MWParse, MWHeuristic, "heuristic keys on method"},

		{MWSelectEndpoint, MWSendRelay, "send_relay reads ctx.Endpoint from select_endpoint"},
		{MWHeuristic, MWSendRelay, "heuristic wraps send_relay to analyze the response"},

		{MWRetry, MWSelectEndpoint, "retry must run select_endpoint on each attempt; if it were inside, rotation would not work"},
		{MWHedge, MWSelectEndpoint, "each hedge racer must pick its own endpoint"},

		{MWCircuitBreak, MWSelectEndpoint, "circuit_break prunes broken domains before selection"},

		{MWParse, MWTimeout, "timeout resolves its deadline per service, which parse sets"},
		{MWTimeout, MWBatch, "the deadline must cover the batch fan-out"},
		{MWTimeout, MWRetry, "the deadline must cover every retry attempt"},

		{MWHedge, MWMethodBlocks, "each hedge arm must honour and feed method blocks"},
		{MWMethodBlocks, MWSelectEndpoint, "method_blocks prunes before selection"},

		// The fan-out rules come first so that a chain putting score outside
		// them is told the load-bearing thing — score missed an attempt —
		// rather than the select_endpoint rule such an order also trips.
		{MWRetry, MWScore, "one attempt is one scoring event; score must see every retry"},
		{MWHedge, MWScore, "each hedge arm scores its own attempt"},
		{MWBatch, MWScore, "batch collapses the per-attempt signals score adds to its sink"},
		{MWSelectEndpoint, MWScore, "score reads ctx.Endpoint, which select_endpoint sets for this attempt"},
		{MWScore, MWHeuristic, "score consumes the verdict heuristic produces for this attempt"},
	}

	for _, rule := range mustPrecede {
		a, b := lookup(rule.before), lookup(rule.after)
		if a < 0 || b < 0 {
			continue
		}
		if a >= b {
			return fmt.Errorf(
				"middleware chain order: %q must precede %q (%s); got %q at %d and %q at %d",
				rule.before, rule.after, rule.reason, rule.before, a, rule.after, b,
			)
		}
	}

	return nil
}
