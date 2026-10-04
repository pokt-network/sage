package shannon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/websockets"
)

// WS-1. An HA relay miner refusing to over-service a WebSocket closes with a
// normal 1000 and says so in the reason, not with 4002. That is the same
// protocol-correct refusal the HTTP path scores as nothing (IsOverServiced),
// yet the rebind records it as a major ws_endpoint_lost against the supplier.
//
// TestLossIsSuppliers pins {1011, "internal"} as the supplier's, so a fix has
// to read the reason text, not treat code 1000 as innocent.
//
// The other half of the finding — the same close should mark the supplier
// over-served for the session, as 4002 does — is inline in Open's rebind
// closure and is not covered here: no test drives a successful Open (it needs
// a session, an app and a resolvable endpoint).
func TestAudit2_OverServicingCloseIsNotSuppliersLoss(t *testing.T) {
	cause := fmt.Errorf("%w: read from endpoint: %w", websockets.ErrBridgeConnectionFailed,
		&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "offchain rate limit hit by relayer proxy: websockets gateway message error"})
	if lossIsSuppliers(cause) {
		t.Fatalf("lossIsSuppliers(%v) = true; an over-servicing close is protocol-correct and must not cost the supplier a major error", cause)
	}
}

// WS-2. A stalled feed triggers a rebind, which replays the subscriptions to a
// new supplier. Until the replay ack lands, Heartbeat still reports the old
// supplier's last activity — already past the timeout — so the very next
// stall check calls the new supplier stalled before it could have answered:
// a second rebind and a major penalty for a supplier that did nothing wrong.
func TestAudit2_ReplayRestartsStallClock(t *testing.T) {
	const timeout = 50 * time.Millisecond
	subs := qos.NewSubscriptionRegistry(&evm.Plugin{})
	subs.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`))
	subs.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xheads"}`))
	time.Sleep(timeout + 20*time.Millisecond)
	if !stalled(subs, timeout) {
		t.Fatal("precondition: a newHeads feed silent past the timeout is stalled")
	}

	if frames := subs.Replay().Frames; len(frames) != 1 {
		t.Fatalf("precondition: ReplayFrames = %d frames, want 1", len(frames))
	}
	if stalled(subs, timeout) {
		t.Fatal("stalled right after the replay was sent: the new supplier is judged on the old one's silence")
	}
}

// WS-5. The relay miner reports "session expired" as a 410 envelope when a
// frame lands on a session that ended. While the bridge can still rebind onto
// the next session the client's connection survives, so the 410 body is noise
// to it — a JSON error with no id, in the middle of its subscription stream.
// PATH swallows it.
//
// Conflicts with TestWSProcessor_ControlFrameForwardsDecodedBodyUngraded,
// which pins the decoded body as forwarded; one of the two has to change.
func TestAudit2_SessionExpiredControlFrameNotForwarded(t *testing.T) {
	const body = `{"error":"session expired"}`
	p := &Protocol{
		fullNode: &mockRelayFullNode{validateResponse: &servicetypes.RelayResponse{Payload: envelopeBytes(t, 410, body)}},
		signer:   &countingSigner{}, bl: newBlacklist(), logger: newTestLogger(),
	}
	proc := newWSMessageProcessor(
		context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionEndBlockHeight: 200},
		"pokt1supplier", "endpoint-1", &apptypes.Application{Address: "pokt1app"}, nil,
	)

	out, _ := proc.ProcessEndpointMessage([]byte(`inbound-wire-bytes`))
	if len(out) != 0 {
		t.Fatalf("the miner's 410 session-expired body %q was forwarded to the client; want it swallowed while a rebind is possible", out)
	}
}
