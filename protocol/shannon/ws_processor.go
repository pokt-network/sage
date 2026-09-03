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
	"github.com/pokt-network/sage/qos"
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
	endpointAddr  domain.EndpointAddr
	app           *apptypes.Application

	// sessionActive gates ProcessClientMessage. Flipped to false by the
	// session-expiry watcher in WSRelayer so in-flight endpoint frames can
	// still drain but no new client frames are signed.
	sessionActive atomic.Bool

	// onEndpointFrame is invoked after ProcessEndpointMessage successfully
	// validates + unwraps a supplier frame. Nil-safe.
	onEndpointFrame frameCallback

	// subs tracks the connection's subscriptions from the frames that cross
	// it. Nil-safe; set by withSubscriptions.
	subs *qos.SubscriptionRegistry
}

// withSubscriptions attaches the subscription registry the frames feed.
func (p *wsMessageProcessor) withSubscriptions(subs *qos.SubscriptionRegistry) *wsMessageProcessor {
	p.subs = subs
	return p
}

// newWSMessageProcessor creates a processor ready to be handed to
// websockets.startBridge.
func newWSMessageProcessor(
	ctx context.Context,
	protocol *Protocol,
	sessionHeader *sessiontypes.SessionHeader,
	supplierAddr string,
	endpointAddr domain.EndpointAddr,
	app *apptypes.Application,
	onEndpointFrame frameCallback,
) *wsMessageProcessor {
	p := &wsMessageProcessor{
		protocol:        protocol,
		ctx:             ctx,
		sessionHeader:   sessionHeader,
		supplierAddr:    supplierAddr,
		endpointAddr:    endpointAddr,
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
	// Before signing: the registry reads the client's own JSON, not the
	// relay envelope, and may rewrite an unsubscribe to the id the current
	// supplier knows.
	data = p.subs.TranslateClientFrame(data)

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
// returns the inner payload for the bridge to forward to the client.
//
// The miner places a backend's raw WS frame directly into
// RelayResponse.Payload (see poktroll websockets/bridge.go line ~363), so a
// data frame needs no decoding — but its OWN control and error responses come
// through the same field as a POKTHTTPResponse envelope, and those do. See
// extractEndpointFrameBody, which decodes only what is provably an envelope
// and returns everything else untouched.
//
// Validation failure goes through the same policy as the HTTP path
// (Protocol.handleValidationFailure: blacklist only what the supplier is
// answerable for) and returns an error; the bridge treats that as terminal and
// shuts down.
func (p *wsMessageProcessor) ProcessEndpointMessage(data []byte) ([]byte, error) {
	start := time.Now()
	serviceID := domain.ServiceID(p.sessionHeader.ServiceId)

	relayResp, err := p.protocol.fullNode.ValidateRelayResponse(p.supplierAddr, data)

	// Read the miner's error report before branching on err — see
	// Protocol.trackRelayMinerError.
	p.protocol.trackRelayMinerError(serviceID, p.endpointAddr, p.supplierAddr, relayResp)

	if err != nil {
		validationErr := p.protocol.handleValidationFailure(
			serviceID, p.endpointAddr, p.supplierAddr, err, "transport", "websocket",
		)
		if p.onEndpointFrame != nil {
			p.onEndpointFrame(nil, validationErr, time.Since(start))
		}
		return nil, fmt.Errorf("ws ProcessEndpointMessage: validate: %w", validationErr)
	}

	latency := time.Since(start)
	payload, status := extractEndpointFrameBody(relayResp.Payload)

	// A non-2xx status is the miner reporting a condition, not the backend
	// answering. Forward the DECODED body — the client gets readable JSON
	// instead of a protobuf blob, and the endpoint's close frame follows —
	// but hand the callback ErrEndpointControlFrame so nothing is graded: a
	// session expiry is a session-boundary event, and rewarding the supplier
	// for it (the bug this replaces) and penalising it are both wrong.
	//
	// Not returned as an error: the bridge treats a returned error as
	// terminal, and this body is exactly what the client should see.
	if status < 200 || status >= 300 {
		p.protocol.logger.Warn("ws: endpoint returned a non-2xx response, forwarding the decoded body without grading it",
			"service_id", serviceID,
			"endpoint_addr", p.endpointAddr,
			"http_status", status,
			"body", string(payload),
		)
		if p.onEndpointFrame != nil {
			p.onEndpointFrame(payload, ErrEndpointControlFrame, latency)
		}
		return payload, nil
	}

	if p.onEndpointFrame != nil {
		p.onEndpointFrame(payload, nil, latency)
	}
	// After validation, before the client: a replay ack is consumed here
	// (nil, nil — the bridge forwards nothing), a notification may be
	// rewritten to the subscription id the client holds.
	out, forward := p.subs.TranslateEndpointFrame(payload)
	if !forward {
		return nil, nil
	}
	return out, nil
}
