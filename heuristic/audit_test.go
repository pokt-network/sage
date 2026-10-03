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
// (Beacon GET /eth/v1/node/health is 200 with no body by spec). Where a
// service turns rest_bodies_as_answers on, RESTBodyAnswer passes it, never a
// redirect. Off (the default) the structural rules still grade it, because
// on most REST faces nothing else would catch a backend answering 200 with
// nothing to every route.
func TestAudit_RESTEmptyOrPlainTextIsSupplierFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   []byte
		status int
		pass   bool
	}{
		{"nil body, 200", nil, 200, true},
		{"empty body, 204", []byte{}, 204, true},
		{"plain text OK, 200", []byte("OK"), 200, true},
		{"empty 301 redirect", nil, 301, false},
		{"JSON is not this rule's", []byte(`{"a":1}`), 200, false},
	} {
		if _, ok := RESTBodyAnswer(tc.body, tc.status); ok != tc.pass {
			t.Errorf("%s: RESTBodyAnswer ok=%v, want %v", tc.name, ok, tc.pass)
		}
	}
	if r := Analyze(nil, 200, domain.RPCTypeREST); r.Reason != "empty_response" {
		t.Errorf("default grading of an empty REST 200: %s, want empty_response", r.Reason)
	}
}
