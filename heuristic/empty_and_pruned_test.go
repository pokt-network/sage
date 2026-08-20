package heuristic

import (
	"net/http"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// TestAnalyze_EmptyBodyOnBodylessStatus pins the exemption that makes the
// critical weighting safe. 204, 205 and 304 carry no body by definition, so an
// empty payload on one of them is the endpoint being correct — harmless while
// an empty body cost a minor penalty, a critical penalty for correct behaviour
// once it is weighted as a protocol violation.
func TestAnalyze_EmptyBodyOnBodylessStatus(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusResetContent, http.StatusNotModified} {
		result := Analyze(nil, status, domain.RPCTypeJSONRPC)
		if result.ShouldPenalize {
			t.Fatalf("status %d: empty body must not be penalized", status)
		}
		if result.Reason != "success" {
			t.Fatalf("status %d: reason = %q, want success", status, result.Reason)
		}
	}
}

// TestAnalyze_EmptyBodyOnContentStatus is the other half: a status that
// promised content and delivered none. No RPC type SAGE forwards has a valid
// zero-length response, and the relay is signed and settleable regardless of
// what the supplier put in the body, so this is a protocol violation rather
// than a bad moment.
func TestAnalyze_EmptyBodyOnContentStatus(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusPartialContent} {
		result := Analyze([]byte("  "), status, domain.RPCTypeJSONRPC)
		if result.Reason != "empty_response" {
			t.Fatalf("status %d: reason = %q, want empty_response", status, result.Reason)
		}
		if !result.ShouldPenalize || result.PenaltySeverity != SeverityCritical {
			t.Fatalf("status %d: got penalize=%v severity=%q, want critical",
				status, result.ShouldPenalize, result.PenaltySeverity)
		}
	}
}

// TestAnalyze_PBSSPrunedState covers geth's path-based state scheme, which
// words a pruned-state miss as "metadata is not found, <block>" — no "prune"
// and no "trie" in it, so every existing pattern missed it and an endpoint
// honestly reporting it does not retain the state was penalized as a fault.
func TestAnalyze_PBSSPrunedState(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "tier 2: parsed JSON-RPC error",
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"metadata is not found, 0x14a5f1c"}}`,
		},
		{
			name: "tier 3: unparsed body carrying the same wording",
			body: `{"note":"upstream said metadata is not found, 0x14a5f1c"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Analyze([]byte(tt.body), http.StatusOK, domain.RPCTypeJSONRPC)
			if result.Attribution != AttrBlockchain {
				t.Fatalf("attribution = %v, want blockchain", result.Attribution)
			}
			if result.ShouldPenalize {
				t.Fatal("a capability limitation must not penalize the supplier")
			}
			if result.ShouldCircuitBreak {
				t.Fatal("a capability limitation must not circuit-break the domain")
			}
			if !result.ShouldRetry {
				t.Fatal("must retry elsewhere — another endpoint may retain the state")
			}
		})
	}
}

// TestReportsMissingHistoricalState covers the wordings the EVM archival
// demotion path shares with the analyzer. Table-driven because the
// discriminating detail is the exact string: PATH's two catalogues each missed
// a live wording by a single word ("state not available" does not match
// "historical state is not available").
func TestReportsMissingHistoricalState(t *testing.T) {
	missing := []string{
		"metadata is not found, 0x14a5f1c",
		"missing trie node 0e1f (path )",
		"historical state is not available",
		"historical state 0xabc",
		"state not available",
		"Missing trie node",
	}
	for _, msg := range missing {
		if !ReportsMissingHistoricalState(msg) {
			t.Fatalf("%q must read as missing historical state", msg)
		}
	}

	other := []string{
		"execution reverted",
		"rate limit exceeded",
		"invalid argument 0: hex string has length 39",
	}
	for _, msg := range other {
		if ReportsMissingHistoricalState(msg) {
			t.Fatalf("%q must not read as missing historical state", msg)
		}
	}
}
