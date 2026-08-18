package heuristic

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

func TestIsOverServiced(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"poktroll offchain rate limit", `{"error":"offchain rate limit hit by relayer proxy"}`, true},
		{"HA session relay limit", `{"error":"session relay limit reached: claimable portion fully consumed"}`, true},
		{"claimable portion phrase", `relay rejected: claimable portion fully consumed`, true},
		{"case insensitive", `OFFCHAIN RATE LIMIT HIT BY RELAYER PROXY`, true},
		{"generic backend rate limit is NOT over-serving", `{"error":"too many requests, slow down"}`, false},
		{"empty", ``, false},
		{"normal success", `{"jsonrpc":"2.0","result":"0x1","id":1}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOverServiced([]byte(tc.body)); got != tc.want {
				t.Fatalf("isOverServiced(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func TestAnalyze_OverServiced_NoPenalty(t *testing.T) {
	// HA relay-miner: HTTP 429 + over-serve body. Must NOT be penalized despite 429.
	t.Run("HA 429 over-serve", func(t *testing.T) {
		body := []byte(`{"error":"session relay limit reached: claimable portion fully consumed"}`)
		got := Analyze(body, 429, domain.RPCTypeJSONRPC)
		assertOverServiced(t, got)
	})

	// poktroll relay-miner: HTTP 200 + over-serve payload.
	t.Run("poktroll 200 over-serve", func(t *testing.T) {
		body := []byte(`{"error":"offchain rate limit hit by relayer proxy","code":7}`)
		got := Analyze(body, 200, domain.RPCTypeJSONRPC)
		assertOverServiced(t, got)
	})
}

func assertOverServiced(t *testing.T, got AnalysisResult) {
	t.Helper()
	if got.Reason != "over_serviced" {
		t.Fatalf("reason = %q, want over_serviced", got.Reason)
	}
	if got.ShouldPenalize {
		t.Fatal("over-serving must not penalize the supplier")
	}
	if got.ShouldCircuitBreak {
		t.Fatal("over-serving must not circuit-break the supplier")
	}
	if !got.ShouldRetry {
		t.Fatal("over-serving should retry on another supplier")
	}
}

// Regression guard: a generic backend rate limit (HTTP 429 without an
// over-serve signal) must still be penalized — only protocol-correct
// over-servicing is exempt.
func TestAnalyze_GenericRateLimit_StillPenalized(t *testing.T) {
	body := []byte(`{"error":"too many requests"}`)
	got := Analyze(body, 429, domain.RPCTypeJSONRPC)
	if got.Reason != "http_429" {
		t.Fatalf("reason = %q, want http_429", got.Reason)
	}
	if !got.ShouldPenalize {
		t.Fatal("generic rate limit should still penalize")
	}
}
