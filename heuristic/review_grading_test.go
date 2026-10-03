package heuristic

import (
	"fmt"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/domain"
)

// Review of 5fd0d96..c3a3a7a: grading changes that can hide a broken
// supplier. Each test asserts the invariant; on c3a3a7a they fail.

// relayErr wraps cause the way protocol/shannon's SendRelay hands it over.
func relayErr(cause error) error {
	return domain.NewRelayError(domain.ErrEndpoint, "relay miner refused the relay", cause, true)
}

// poktroll's relayer_proxy 12 and 13 are the miner's own max_body_size
// (relay_builders.go:15, http_utils.go:127/143: sync.serverConfig.MaxBodySize)
// applied to the backend's response and to the gateway's request. That is
// one supplier's configuration, not the request: transport.go's own comment
// says "another supplier's miner may allow more". A supplier whose limit is
// set too low refuses every large request forever. Graded miner_request_refused
// it is neither scored nor steered away from: no penalty, no MethodBlocking,
// so the next large request is sent to it first again.
func TestReview_MinerBodyLimitRefusalIsNeitherScoredNorSteered(t *testing.T) {
	for _, code := range []uint32{12, 13} {
		v := AnalyzeTransportError(relayErr(&domain.MinerError{Codespace: "relayer_proxy", Code: code, Message: "limit exceed"}), nil)
		if !v.ShouldPenalize && !v.MethodBlocking {
			t.Errorf("relayer_proxy %d (the miner's own max_body_size): reason %q, penalize=false, method_blocking=false; "+
				"a supplier with a low limit keeps first attempts for every large request forever", code, v.Reason)
		}
	}
}

// The HA relay miner refuses a relay that fails its ValidateRelayRequest
// (supplier key not held by this miner, ring or session fetch failing, a
// session header that does not match) with HTTP 403 (pocket-relay-miner
// relayer/proxy.go:1321) and closes a WebSocket with 4001 for the same
// failure (relayer/websocket.go:1090). The two shapes of one condition must
// grade alike: on c3a3a7a the HTTP one is upstream_4xx (penalized) and the
// WebSocket one miner_request_refused (nothing), so a WebSocket face whose
// miner cannot validate is never scored, on the bridge (lossIsSuppliers) or
// by the probe (probeFailure).
func TestReview_HAMinerValidationFailureGradedAlikeOnHTTPAndWebSocket(t *testing.T) {
	httpVerdict := AnalyzeTransportError(relayErr(&domain.UpstreamStatusError{
		Status: 403,
		Body:   []byte("relay validation failed: supplier pokt1example is not allowed by this relayer"),
	}), nil)
	wsVerdict, ok := MinerRefusal(fmt.Errorf("read from endpoint: %w",
		&websocket.CloseError{Code: CloseMinerValidationFailed, Text: "relay validation failed"}))
	if !ok {
		t.Fatal("precondition: 4001 is a miner refusal")
	}
	if httpVerdict.ShouldPenalize != wsVerdict.ShouldPenalize {
		t.Fatalf("one HA miner condition, two grades: HTTP 403 %q penalize=%v, WebSocket 4001 %q penalize=%v",
			httpVerdict.Reason, httpVerdict.ShouldPenalize, wsVerdict.Reason, wsVerdict.ShouldPenalize)
	}
}

// A relay request the miner cannot unmarshal is answered by the HA miner with
// HTTP 400 "invalid relay request: body must be a valid RelayRequest protobuf"
// (pocket-relay-miner relayer/proxy.go:1004) and by the poktroll miner with
// relayer_proxy 14 (poktroll relay_builders.go:37). SAGE sends the same bytes
// to every supplier, so one supplier failing to parse them is that supplier's
// path mangling the body. The two miners' reports of it must grade alike.
func TestReview_UnmarshalRefusalGradedAlikeOnBothMiners(t *testing.T) {
	ha := AnalyzeTransportError(relayErr(&domain.UpstreamStatusError{
		Status: 400,
		Body:   []byte("invalid relay request: body must be a valid RelayRequest protobuf"),
	}), nil)
	poktroll := AnalyzeTransportError(relayErr(&domain.MinerError{
		Codespace: "relayer_proxy", Code: 14, Message: "failed to unmarshal relay request",
	}), nil)
	if ha.ShouldPenalize != poktroll.ShouldPenalize {
		t.Fatalf("one condition, two grades: HA miner 400 %q penalize=%v, poktroll relayer_proxy 14 %q penalize=%v",
			ha.Reason, ha.ShouldPenalize, poktroll.Reason, poktroll.ShouldPenalize)
	}
}

// The REST pass (analyzer.go: empty or plain-text REST body is success) sits
// after Tier 0, which claims 4xx and 5xx but leaves 1xx and 3xx to the body
// tiers. The HA relay miner does not follow a backend's redirect, it passes
// it through (pocket-relay-miner relayer/proxy.go:571-574, ErrUseLastResponse),
// so a REST backend URL that redirects every request (an http:// stake behind
// a load balancer that redirects to https://, with no body) answers every
// query with an empty 301. Before 9ab69fa that was empty_response (critical,
// breaker vote); now it is success.
func TestReview_EmptyRESTRedirectIsNotAnAnswer(t *testing.T) {
	for _, status := range []int{301, 302, 307, 308} {
		if v := Analyze(nil, status, domain.RPCTypeREST); v.IsSuccess() {
			t.Errorf("REST %d with an empty body graded %q: a redirect the gateway will not follow is not the answer to the client's query", status, v.Reason)
		}
	}
}
