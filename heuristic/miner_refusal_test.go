package heuristic

import (
	"fmt"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/domain"
)

// minerRefusalTable is every refusal a relay miner is known to send, with
// whether it is the supplier's to answer for. A new code goes here first:
// TestMinerRefusal_Table pins the heuristic's verdict and the HTTP path's,
// and TestMinerRefusal_WebSocketPathsAgree in protocol/shannon holds the
// WebSocket rebind and probe to the same table.
var minerRefusalTable = []struct {
	name      string
	err       error
	penalized bool
}{
	// poktroll relayer_proxy, pkg/relayer/proxy/errors.go.
	{"relayer_proxy 1 invalid session", minerErr("relayer_proxy", 1), false},
	{"relayer_proxy 2 services undefined", minerErr("relayer_proxy", 2), true},
	{"relayer_proxy 3 endpoint not handled", minerErr("relayer_proxy", 3), true},
	{"relayer_proxy 4 unsupported transport", minerErr("relayer_proxy", 4), true},
	{"relayer_proxy 5 internal error", minerErr("relayer_proxy", 5), true},
	{"relayer_proxy 6 unknown session", minerErr("relayer_proxy", 6), false},
	{"relayer_proxy 7 over-servicing", minerErr("relayer_proxy", 7), false},
	{"relayer_proxy 8 relay cost", minerErr("relayer_proxy", 8), true},
	{"relayer_proxy 9 backend unreachable", minerErr("relayer_proxy", 9), true},
	{"relayer_proxy 10 timeout", minerErr("relayer_proxy", 10), true},
	{"relayer_proxy 11 max body", minerErr("relayer_proxy", 11), true},
	{"relayer_proxy 12 response limit", minerErr("relayer_proxy", 12), true},
	{"relayer_proxy 13 request limit", minerErr("relayer_proxy", 13), true},
	{"relayer_proxy 14 unmarshal request", minerErr("relayer_proxy", 14), true},
	// poktroll relay_authenticator, pkg/relayer/relay_authenticator/errors.go.
	{"relay_authenticator 1 invalid session", minerErr("relay_authenticator", 1), false},
	{"relay_authenticator 2 supplier not in session", minerErr("relay_authenticator", 2), false},
	{"relay_authenticator 3 undefined signer", minerErr("relay_authenticator", 3), true},
	{"relay_authenticator 4 invalid request", minerErr("relay_authenticator", 4), true},
	{"relay_authenticator 5 invalid response", minerErr("relay_authenticator", 5), true},
	{"relay_authenticator 6 no key for supplier", minerErr("relay_authenticator", 6), true},
	// The HA miner's 429 and close codes, the poktroll miner's close reason.
	{"HA 429 over-servicing body", &domain.UpstreamStatusError{Status: 429, Body: []byte("session relay limit reached: claimable portion fully consumed")}, false},
	{"close 4000 session expired", &websocket.CloseError{Code: CloseMinerSessionExpired}, false},
	{"close 4001 validation failed", &websocket.CloseError{Code: CloseMinerValidationFailed, Text: "relay validation failed"}, true},
	{"close 4002 stake limit", &websocket.CloseError{Code: CloseMinerStakeLimit}, false},
	{"close 1000 over-servicing reason", &websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "offchain rate limit hit by relayer proxy"}, false},
}

func minerErr(codespace string, code uint32) error {
	return &domain.MinerError{Codespace: codespace, Code: code, Message: fmt.Sprintf("%s %d", codespace, code)}
}

func TestMinerRefusal_Table(t *testing.T) {
	for _, tc := range minerRefusalTable {
		wrapped := fmt.Errorf("relay: %w", tc.err)
		got, ok := MinerRefusal(wrapped)
		if !ok {
			t.Errorf("%s: not read as a miner refusal", tc.name)
			continue
		}
		if got.ShouldPenalize != tc.penalized || !got.ShouldRetry {
			t.Errorf("%s: %s penalize=%v retry=%v; want penalize=%v and a retry", tc.name, got.Reason, got.ShouldPenalize, got.ShouldRetry, tc.penalized)
		}
		// The HTTP path reaches the same verdict through AnalyzeTransportError.
		if http := AnalyzeTransportError(wrapped, nil); http.Reason != got.Reason || http.ShouldPenalize != got.ShouldPenalize {
			t.Errorf("%s: AnalyzeTransportError %s penalize=%v, MinerRefusal %s penalize=%v", tc.name, http.Reason, http.ShouldPenalize, got.Reason, got.ShouldPenalize)
		}
	}
}

// A close or a status that carries no miner verdict is left to the caller.
func TestMinerRefusal_NotARefusal(t *testing.T) {
	for _, err := range []error{
		&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "bye"},
		&websocket.CloseError{Code: websocket.CloseAbnormalClosure},
		&websocket.CloseError{Code: 4003, Text: "connection failed"},
		&domain.UpstreamStatusError{Status: 429, Body: []byte("too many requests")},
		&domain.UpstreamStatusError{Status: 502},
		fmt.Errorf("dial failed"),
	} {
		if v, ok := MinerRefusal(err); ok {
			t.Errorf("%v read as a miner refusal: %s", err, v.Reason)
		}
	}
}
