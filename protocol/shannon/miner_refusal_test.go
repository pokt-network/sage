package shannon

import (
	"fmt"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/websockets"
)

// The WebSocket rebind and probe grade a miner's refusal by the same table as
// the HTTP path (heuristic.MinerRefusal, pinned by its TestMinerRefusal_Table).
// One row per shape and per verdict, wrapped the way each path receives it.
func TestMinerRefusal_WebSocketPathsAgree(t *testing.T) {
	for _, err := range []error{
		&domain.MinerError{Codespace: "relayer_proxy", Code: 7, Message: "offchain rate limit hit by relayer proxy"},
		&domain.MinerError{Codespace: "relayer_proxy", Code: 6, Message: "unknown session"},
		&domain.MinerError{Codespace: "relayer_proxy", Code: 12, Message: "response limit exceed"},
		&domain.MinerError{Codespace: "relayer_proxy", Code: 9, Message: "supplier(s) not reachable"},
		&websocket.CloseError{Code: heuristic.CloseMinerSessionExpired},
		&websocket.CloseError{Code: heuristic.CloseMinerValidationFailed, Text: "relay validation failed"},
		&websocket.CloseError{Code: heuristic.CloseMinerStakeLimit},
		&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "offchain rate limit hit by relayer proxy"},
	} {
		want, ok := heuristic.MinerRefusal(err)
		if !ok {
			t.Fatalf("precondition: %v is a miner refusal", err)
		}
		loss := fmt.Errorf("%w: read from endpoint: %w", websockets.ErrBridgeConnectionFailed, err)
		if got := lossIsSuppliers(loss); got != want.ShouldPenalize {
			t.Errorf("%v: rebind loss charged = %v, table says penalize = %v", err, got, want.ShouldPenalize)
		}
		processed := fmt.Errorf("ws ProcessEndpointMessage: %w", err)
		if got := probeFailure(processed, wsProbeInvalid); (got == wsProbeInvalid) != want.ShouldPenalize {
			t.Errorf("%v: probe result %q, table says penalize = %v", err, got, want.ShouldPenalize)
		}
	}
}
