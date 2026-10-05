package shannon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/websockets"
)

// ErrSessionExpired is returned by wsMessageProcessor.ProcessClientMessage
// when the Shannon session backing the bridge has crossed its
// SessionEndBlockHeight. The bridge treats this as a terminal error and
// shuts down with CloseServiceRestart so the client reconnects.
var ErrSessionExpired = errors.New("shannon ws: session expired")

// errReissue ends a supplier's tenure on a bridge when it refused a client
// request for its rate limit or quota, or answered it with a stale head: the
// bridge rebinds, and the request goes again to the next supplier
// (wsMessageProcessor.reissue).
var errReissue = errors.New("supplier refused or answered stale a request that goes again to the next supplier")

// errSupplierBehind ends a supplier's tenure on a bridge whose newHeads it
// pushes behind the chain head (wsMessageProcessor.staleHead).
var errSupplierBehind = errors.New("supplier pushes heads behind the chain head")

// frameCallback is invoked after each endpoint-originated frame is validated
// and unwrapped. Keeps the processor decoupled from reputation/observation/
// heuristic concerns — WSRelayer wires the callback with those hooks.
type frameCallback func(payload []byte, err error, latency time.Duration)

// wsMessageProcessor implements websockets.MessageProcessor for Shannon.
//
// The processor is instantiated once per supplier tenure: a bridge starts with
// one, and every rebind (a lost supplier, a session rollover) builds a new one
// for the supplier and session it moves to. The SessionHeader, supplier, and
// app are captured at construction and never change after it.
type wsMessageProcessor struct {
	// evidence marks a debug probe's processor: a response that fails
	// verification is reported, not blacklisted (debug.go).
	evidence bool
	samples  *wsNotificationSamples
	// heads and consensusHead feed the head signals (ws_heads.go); nil
	// for a probe.
	heads         *wsHeadTracker
	consensusHead func() uint64
	// topicMu guards topics: frames are counted on the bridge goroutine and
	// read at release by the rebind handler or the close.
	topicMu       sync.Mutex
	topics        map[string]int64
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
	// it. Nil-safe; shared by every processor of one bridge.
	subs *qos.SubscriptionRegistry

	// canRebind reports whether the bridge would meet an endpoint loss with
	// a rebind. Nil (a probe, or before the bridge is up) means no, and a
	// rate limit then reaches the client; see reissue.
	canRebind func() bool

	// staleness grades head answers and newHeads pushes against the chain
	// head (ws_stale.go); nil when the plugin measures no head. staleHeads
	// is this supplier's run of stale pushes; bridge loop only.
	staleness  *wsStaleness
	staleHeads int

	// duplicates counts this supplier's repeated notifications
	// (ws_duplicates.go); nil for a probe.
	duplicates *wsDuplicates

	// answered counts client requests this supplier answered, taken by the
	// connection's next success signal (wsSuccessGate.weight).
	answered atomic.Int64

	// metrics, owner and operator attribute every frame to the supplier
	// that signed it; endpointFrames counts this supplier's frames to the
	// client for its tenure. Set by the relayer when it binds the supplier;
	// metrics is nil-safe.
	metrics        WSMetrics
	owner          string
	operator       string
	boundAt        time.Time
	endpointFrames atomic.Int64
}

// countTopic counts one notification this supplier pushed on a topic, capped
// at wsLedgerMaxTopics topics (the rest under otherTopic).
func (p *wsMessageProcessor) countTopic(topic string) {
	if topic == "" {
		return
	}
	if len(topic) > 64 {
		topic = topic[:64]
	}
	p.topicMu.Lock()
	if p.topics == nil {
		p.topics = make(map[string]int64, 2)
	}
	p.topics[topicSlot(p.topics, topic)]++
	p.topicMu.Unlock()
}

// topicCounts is a copy of the per-topic notification counts so far.
func (p *wsMessageProcessor) topicCounts() map[string]int64 {
	p.topicMu.Lock()
	defer p.topicMu.Unlock()
	out := make(map[string]int64, len(p.topics))
	for t, n := range p.topics {
		out[t] = n
	}
	return out
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
	if p.metrics != nil {
		p.metrics.SupplierFrame(domain.ServiceID(p.sessionHeader.ServiceId), p.operator, p.owner, websockets.SourceClient)
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

	// The miner's own unsigned refusal is not a forged frame: no blacklist
	// (see SendRelay). The bridge still ends; the rebind decides the rest.
	if minerErr := unsignedMinerError(relayResp, err); minerErr != nil {
		if p.onEndpointFrame != nil && !p.evidence {
			p.onEndpointFrame(nil, ErrEndpointControlFrame, time.Since(start))
		}
		return nil, fmt.Errorf("ws ProcessEndpointMessage: %w", minerErr)
	}
	if err != nil {
		if p.evidence {
			// A probe reports a failed verification; it does not act on it.
			return nil, fmt.Errorf("ws ProcessEndpointMessage: validate: %w", err)
		}
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
		// A 410 is the miner saying the session ended. The bridge rebinds
		// onto the next session when the miner's close follows (and closes
		// with 1012 if it cannot), so the client's connection survives and
		// the body is noise to it: a JSON error with no id in the middle of
		// its subscription stream. PATH swallows it too. Any other status may
		// be the answer to a request the client is waiting on, and is
		// forwarded.
		if status == http.StatusGone {
			if p.onEndpointFrame != nil {
				p.onEndpointFrame(payload, ErrEndpointControlFrame, latency)
			}
			return nil, nil
		}
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

	// How long the supplier took over a request the client is waiting on:
	// from the frame going out to its answer coming in.
	req, sent := p.subs.InFlight(payload)
	if !sent.IsZero() && p.metrics != nil {
		p.metrics.SupplierAnswer(serviceID, p.operator, time.Since(sent))
	}
	// A stale answer is graded here, not by the frame heuristic, which would
	// pass it: it is a well-formed answer, only an old one.
	stale := p.staleAnswer(req, payload)
	switch {
	case stale:
		p.staleness.penalize(reasonWSStaleResponse)
	case p.onEndpointFrame != nil:
		p.onEndpointFrame(payload, nil, latency)
	}
	if p.reissue(payload, stale) {
		return nil, fmt.Errorf("ws ProcessEndpointMessage: %w", errReissue)
	}
	// A request answered, and answered well: what the next success signal
	// stands for in the failure rate. Not a stale answer, nor an error one.
	if !sent.IsZero() && !stale && !qos.JSONRPCHasError(payload) {
		p.answered.Add(1)
	}
	// After validation, before the client: a replay ack is consumed here
	// (nil, nil — the bridge forwards nothing), a notification may be
	// rewritten to the subscription id the client holds.
	out, forward, note := p.subs.TranslateEndpointFrame(payload)
	p.endpointFrames.Add(1)
	if p.metrics != nil {
		p.metrics.SupplierFrame(serviceID, p.operator, p.owner, websockets.SourceEndpoint)
		p.metrics.SupplierNotification(serviceID, p.operator, p.owner, note)
	}
	if note.Kind == qos.NotificationOK || note.Kind == qos.NotificationDuplicate {
		p.samples.observe(serviceID, p.operator, p.owner, note.Topic, payload)
		p.countTopic(note.Topic)
		if p.duplicates.observe(note.Kind == qos.NotificationDuplicate, time.Now()) {
			return nil, nil
		}
	}
	if note.Kind == qos.NotificationOK && note.Topic == "newHeads" && p.observeHead(serviceID, payload) {
		return nil, fmt.Errorf("ws ProcessEndpointMessage: %w", errSupplierBehind)
	}
	if !forward {
		return nil, nil
	}
	return out, nil
}

// EndpointSentBinaryJSON implements websockets.BinaryJSONObserver: this
// supplier framed a JSON answer as binary. Counted per supplier, whatever
// frame type the client is answered in, so the operator whose stack sends
// binary can be told.
func (p *wsMessageProcessor) EndpointSentBinaryJSON() {
	if p.metrics != nil {
		p.metrics.SupplierRetyped(domain.ServiceID(p.sessionHeader.ServiceId), p.operator, p.owner)
	}
}

var _ websockets.BinaryJSONObserver = (*wsMessageProcessor)(nil)

// reissue reports whether payload is a rate limit, a spent quota or (stale)
// a stale head answering a client request still in flight, now queued to go
// again to the next supplier after a rebind (qos.SubscriptionRegistry.Reissue).
// The caller drops the frame and returns errReissue, which the bridge meets
// with that rebind. The client is waiting on an answer this supplier will not
// give: on mainnet robinhood (2026-10-03) a metered plan's -32029 went to the
// client as the answer, and (2026-10-04) a head 3,985 blocks old.
//
// Graded already, by onEndpointFrame or as stale. Not when the bridge has no
// rebind left: then it would close, and the answer is better than a closed
// connection.
func (p *wsMessageProcessor) reissue(payload []byte, stale bool) bool {
	if p.canRebind == nil {
		return false
	}
	reason := "stale"
	if !stale {
		if !bytes.Contains(payload, []byte(`"error"`)) {
			return false
		}
		reason = heuristic.AnalyzeFrame(payload, domain.RPCTypeWebSocket).Reason
		if reason != heuristic.ReasonRateLimited && reason != heuristic.ReasonQuotaExceeded {
			return false
		}
	}
	if !p.canRebind() || !p.subs.Reissue(payload) {
		return false
	}
	if p.metrics != nil {
		p.metrics.SupplierReissued(domain.ServiceID(p.sessionHeader.ServiceId), p.operator, p.owner, reason, 1)
	}
	return true
}
