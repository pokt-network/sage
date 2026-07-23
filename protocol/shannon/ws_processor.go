package shannon

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
)

// ErrSessionExpired is returned by wsMessageProcessor.ProcessClientMessage
// when the Shannon session backing the bridge has crossed its
// SessionEndBlockHeight. The bridge treats this as a terminal error and
// shuts down with CloseServiceRestart so the client reconnects.
var ErrSessionExpired = errors.New("shannon ws: session expired")

// frameCallback is invoked after each endpoint-originated frame is validated
// and unwrapped. Keeps the processor decoupled from reputation/observation/
// heuristic concerns — WSRelayer wires the callback with those hooks.
type frameCallback func(payload []byte, err error, latency time.Duration)

// wsMessageProcessor implements websockets.MessageProcessor for Shannon.
//
// The processor is instantiated once per WS bridge (i.e. per client session).
// The SessionHeader, supplier, and app are captured at construction and are
// stable for the bridge's lifetime — v1 closes the bridge at session
// boundaries instead of rotating the endpoint or refreshing the session.
type wsMessageProcessor struct {
	protocol      *Protocol
	ctx           context.Context
	sessionHeader *sessiontypes.SessionHeader
	supplierAddr  string
	app           *apptypes.Application

	// sessionActive gates ProcessClientMessage. Flipped to false by the
	// session-expiry watcher in WSRelayer so in-flight endpoint frames can
	// still drain but no new client frames are signed.
	sessionActive atomic.Bool

	// onEndpointFrame is invoked after ProcessEndpointMessage successfully
	// validates + unwraps a supplier frame. Nil-safe.
	onEndpointFrame frameCallback
}

// newWSMessageProcessor creates a processor ready to be handed to
// websockets.startBridge.
func newWSMessageProcessor(
	ctx context.Context,
	protocol *Protocol,
	sessionHeader *sessiontypes.SessionHeader,
	supplierAddr string,
	app *apptypes.Application,
	onEndpointFrame frameCallback,
) *wsMessageProcessor {
	p := &wsMessageProcessor{
		protocol:        protocol,
		ctx:             ctx,
		sessionHeader:   sessionHeader,
		supplierAddr:    supplierAddr,
		app:             app,
		onEndpointFrame: onEndpointFrame,
	}
	p.sessionActive.Store(true)
	return p
}

// ProcessClientMessage wraps the client's outgoing frame in a signed
// servicetypes.RelayRequest before forwarding to the supplier's relay miner.
//
// The RelayRequest's Payload is the raw client frame bytes — DO NOT wrap in
// an HTTP envelope. The poktroll relay miner's WS bridge
// (pkg/relayer/proxy/websockets/bridge.go:handleGatewayIncomingMessage)
// writes relayRequest.Payload verbatim to the backend WS connection — no
// DeserializeHTTPRequest, no envelope unwrap. The raw payload bytes are also
// hashed for onchain proof verification (bridge.go:UpdatePayloadHash), so
// the gateway and miner must agree bit-for-bit on the payload encoding.
// Wrapping in an HTTP envelope would cause proof validation to fail.
//
// The frame type (text vs binary) is carried by the gorilla websocket frame
// metadata, not by the RelayRequest payload — our bridge preserves the
// original msg.messageType automatically on both the outbound write to the
// supplier and the return write to the client.
func (p *wsMessageProcessor) ProcessClientMessage(data []byte) ([]byte, error) {
	if !p.sessionActive.Load() {
		return nil, ErrSessionExpired
	}

	unsigned := &servicetypes.RelayRequest{
		Meta: servicetypes.RelayRequestMetadata{
			SessionHeader:           p.sessionHeader,
			SupplierOperatorAddress: p.supplierAddr,
		},
		Payload: data,
	}

	signed, err := p.protocol.signer.signRelayRequest(p.ctx, unsigned, p.app)
	if err != nil {
		return nil, fmt.Errorf("ws ProcessClientMessage: sign: %w", err)
	}

	wire, err := signed.Marshal()
	if err != nil {
		return nil, fmt.Errorf("ws ProcessClientMessage: marshal: %w", err)
	}
	return wire, nil
}

// ProcessEndpointMessage validates the supplier's signed RelayResponse and
// returns the inner payload — raw frame bytes — for the bridge to forward
// to the client. The miner places the backend's raw WS frame directly into
// RelayResponse.Payload (see poktroll websockets/bridge.go line ~363), so
// no HTTP envelope decoding is needed or permitted here.
//
// Validation failure blacklists the supplier (same policy as the HTTP path)
// and returns an error; the bridge treats that as terminal and shuts down.
func (p *wsMessageProcessor) ProcessEndpointMessage(data []byte) ([]byte, error) {
	start := time.Now()

	relayResp, err := p.protocol.fullNode.ValidateRelayResponse(p.supplierAddr, data)
	if err != nil {
		p.protocol.bl.BlacklistSupplier(domain.ServiceID(p.sessionHeader.ServiceId), p.supplierAddr)
		if p.onEndpointFrame != nil {
			p.onEndpointFrame(nil, err, time.Since(start))
		}
		return nil, fmt.Errorf("ws ProcessEndpointMessage: validate: %w", err)
	}

	latency := time.Since(start)
	payload := relayResp.Payload
	if p.onEndpointFrame != nil {
		p.onEndpointFrame(payload, nil, latency)
	}
	return payload, nil
}
