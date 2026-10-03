package heuristic

import (
	"testing"

	"github.com/pokt-network/sage/domain"
)

// Regressions for the 2026-10-03 correctness audit. Each test asserts the
// correct behaviour; on the code the audit read they fail.

// A relay miner's unsigned refusal that is about the client's request or the
// gateway's own relay says nothing about the supplier: every supplier would
// refuse the same bytes. SAGE's own response ceiling is already graded the
// client's (response_too_large); the miner's must not be charged instead.
func TestAudit_MinerRefusalsChargeTheSupplier(t *testing.T) {
	for _, tc := range []struct {
		name  string
		miner *domain.MinerError
	}{
		{"response over the miner's limit", &domain.MinerError{Codespace: "relayer_proxy", Code: 12, Message: "response limit exceed"}},
		{"request over the miner's limit", &domain.MinerError{Codespace: "relayer_proxy", Code: 13, Message: "request limit exceed"}},
		{"request body over the miner's max", &domain.MinerError{Codespace: "relayer_proxy", Code: 11, Message: "max body size exceeded"}},
		{"miner could not unmarshal our request", &domain.MinerError{Codespace: "relayer_proxy", Code: 14, Message: "failed to unmarshal relay request"}},
		{"invalid relay request", &domain.MinerError{Codespace: "relay_authenticator", Code: 4, Message: "invalid relay request"}},
	} {
		got := AnalyzeTransportError(domain.NewRelayError(domain.ErrEndpoint, "x", tc.miner, true), nil)
		if got.ShouldPenalize {
			t.Errorf("%s (%s %d): %s, penalize=%v %s attribution=%s; want not penalised",
				tc.name, tc.miner.Codespace, tc.miner.Code, got.Reason, got.ShouldPenalize, got.PenaltySeverity, got.Attribution)
		}
	}
}

// A REST 2xx with an empty or plain-text body can be the correct answer
// (Beacon GET /eth/v1/node/health is 200 with no body by spec). It must not
// be a supplier failure, let alone a breaker vote.
func TestAudit_RESTEmptyOrPlainTextIsSupplierFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"nil body", nil},
		{"empty body", []byte{}},
		{"plain text OK", []byte("OK")},
	} {
		r := Analyze(tc.body, 200, domain.RPCTypeREST)
		if r.ShouldPenalize || r.ShouldCircuitBreak || r.Attribution == AttrSupplier {
			t.Errorf("%s, 200, rest: %s penalize=%v severity=%s breaker=%v attribution=%s; want no supplier failure",
				tc.name, r.Reason, r.ShouldPenalize, r.PenaltySeverity, r.ShouldCircuitBreak, r.Attribution)
		}
	}
}
