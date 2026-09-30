package shannon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/websockets"
)

// DebugEvent is one frame a probe subscription received.
type DebugEvent struct {
	// TMS is milliseconds since the subscribe was sent.
	TMS int64 `json:"t_ms"`
	// Kind is "notification", "response" (the subscribe's answer, or another
	// answer carrying an id) or "other".
	Kind         string `json:"kind"`
	Subscription string `json:"subscription,omitempty"`
	// What the notification names, when it names something: a head's number
	// and hash; a log's block, transaction and index; a pending transaction.
	BlockNumber *uint64 `json:"block_number,omitempty"`
	BlockHash   string  `json:"block_hash,omitempty"`
	TxHash      string  `json:"tx_hash,omitempty"`
	LogIndex    *uint64 `json:"log_index,omitempty"`
	// Payload is the frame's JSON as the backend sent it, when include_payload
	// was asked for.
	Payload string `json:"payload,omitempty"`
	// Signed is the RelayResponse frame the supplier signed, base64 when
	// marshalled, when include_signed was asked for.
	Signed []byte `json:"signed_b64,omitempty"`
}

// DebugSubscribeResult is one probe subscription's record.
type DebugSubscribeResult struct {
	ServiceID string        `json:"service_id"`
	Target    DebugTarget   `json:"target"`
	Session   *DebugSession `json:"session,omitempty"`
	StartedAt time.Time     `json:"started_at"`
	// DurationMS is how long frames were read.
	DurationMS int64 `json:"duration_ms"`
	// SubscribeAck is the answer to the subscribe, verbatim.
	SubscribeAck string `json:"subscribe_ack,omitempty"`
	// Stop is why reading ended: duration, max_events, session_ended,
	// closed (the peer closed; see CloseCode), cancelled or error.
	Stop      string       `json:"stop"`
	CloseCode int          `json:"close_code,omitempty"`
	Error     string       `json:"error,omitempty"`
	Events    []DebugEvent `json:"events"`
}

// DebugSubscribeOptions shapes one probe subscription.
type DebugSubscribeOptions struct {
	Duration       time.Duration
	MaxEvents      int
	IncludePayload bool
	IncludeSigned  bool
}

// debugReadSlice is how often a probe subscription checks for the end of its
// session.
const debugReadSlice = time.Second

// DebugSubscribe opens a WebSocket to exactly the target, sends the subscribe,
// and records every frame until the duration, the event cap or the end of the
// session it was signed for — it does not rebind: the question is what THIS
// supplier sends. It runs outside the bridge, so no client ledger, reputation
// or metric sees it. See debug.go.
func (r *WSRelayer) DebugSubscribe(ctx context.Context, serviceID domain.ServiceID, target string, subscribe []byte, opts DebugSubscribeOptions) (DebugSubscribeResult, error) {
	res := DebugSubscribeResult{ServiceID: string(serviceID), Events: []DebugEvent{}}
	method := gjson.GetBytes(subscribe, "method").String()
	if method == "" || len(subscribe) > debugMaxPayload {
		return res, fmt.Errorf("%w: a JSON-RPC subscribe with a method, at most %d bytes", protocol.ErrDebugBadRequest, debugMaxPayload)
	}
	if opts.Duration <= 0 || opts.Duration > debugMaxDuration {
		opts.Duration = debugMaxDuration
	}
	if opts.MaxEvents <= 0 || opts.MaxEvents > debugMaxEvents {
		opts.MaxEvents = debugMaxEvents
	}
	p := r.deps.Protocol
	release, ok := p.debugLimits.takeSubscription()
	if !ok {
		return res, fmt.Errorf("%w: at most %d probe subscriptions at once", protocol.ErrDebugBusy, debugMaxSubscriptions)
	}
	defer release()
	r.deps.Logger.Warn("admin: debug ws subscribe", "service_id", serviceID, "target", target,
		"method", method, "duration", opts.Duration)

	if target == debugTargetReference {
		return r.debugReferenceSubscribe(ctx, serviceID, subscribe, opts, res)
	}
	addr, ep, err := p.resolveDebugTarget(ctx, serviceID, domain.RPCTypeWebSocket, target)
	if err != nil {
		return res, err
	}
	res.Target = DebugTarget{Endpoint: string(addr), Supplier: ep.Supplier(), Operator: addr.Operator(), Owner: ep.Owner()}
	session := ep.Session()
	if session == nil || session.Header == nil {
		return res, fmt.Errorf("%w: no session for %s", protocol.ErrDebugTargetNotFound, addr)
	}
	h := session.Header
	res.Session = &DebugSession{SessionID: h.SessionId, Application: h.ApplicationAddress,
		StartHeight: h.SessionStartBlockHeight, EndHeight: h.SessionEndBlockHeight}
	url, err := ep.GetURL(domain.RPCTypeWebSocket)
	if err != nil {
		return res, fmt.Errorf("%w: %s serves no websocket", protocol.ErrDebugTargetNotFound, addr)
	}
	proc, wire, conn, err := r.dialSigned(ctx, serviceID, h, ep, addr, url, subscribe)
	if err != nil {
		res.Stop, res.Error = "error", err.Error()
		return res, nil
	}
	defer conn.Close()
	proc.evidence = true
	endHeight := h.SessionEndBlockHeight
	sessionEnded := func() bool {
		height := p.LatestBlockHeight()
		return height > 0 && height > endHeight
	}
	decode := func(data []byte) ([]byte, []byte, error) {
		payload, err := proc.ProcessEndpointMessage(data)
		return payload, data, err
	}
	return debugRead(ctx, r.deps.Logger, conn, wire, subscribe, decode, sessionEnded, opts, res), nil
}

// debugReferenceSubscribe subscribes on the configured reference, unsigned.
func (r *WSRelayer) debugReferenceSubscribe(ctx context.Context, serviceID domain.ServiceID, subscribe []byte, opts DebugSubscribeOptions, res DebugSubscribeResult) (DebugSubscribeResult, error) {
	url := r.deps.Protocol.debugRefWS[serviceID]
	if url == "" {
		return res, fmt.Errorf("%w: no debug_reference_ws_url for %s", protocol.ErrDebugTargetNotFound, serviceID)
	}
	res.Target = DebugTarget{Endpoint: debugTargetReference}
	conn, err := websockets.ConnectEndpoint(probeLogger, url, nil)
	if err != nil {
		// The error text can carry the URL, and the URL can carry a key.
		res.Stop, res.Error = "error", "reference dial failed"
		return res, nil
	}
	defer conn.Close()
	plain := func(data []byte) ([]byte, []byte, error) { return data, nil, nil }
	return debugRead(ctx, r.deps.Logger, conn, subscribe, subscribe, plain, func() bool { return false }, opts, res), nil
}

// debugRead sends the subscribe and records frames until a stop condition.
// decode turns a wire frame into the backend's JSON and the signed envelope
// (nil for an unsigned reference). Frames are read on their own goroutine:
// gorilla fails a connection for good on a read deadline, so the stop checks
// cannot run between short reads. The caller's Close ends that goroutine.
func debugRead(
	ctx context.Context,
	logger *slog.Logger,
	conn *websocket.Conn,
	wire, subscribe []byte,
	decode func([]byte) (payload, signed []byte, err error),
	sessionEnded func() bool,
	opts DebugSubscribeOptions,
	res DebugSubscribeResult,
) DebugSubscribeResult {
	subID := gjson.GetBytes(subscribe, "id").Raw
	start := time.Now()
	res.StartedAt = start.UTC()
	finish := func(stop string) DebugSubscribeResult {
		res.Stop = stop
		res.DurationMS = time.Since(start).Milliseconds()
		return res
	}
	_ = conn.SetWriteDeadline(start.Add(10 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, wire); err != nil {
		res.Error = "sending the subscribe failed"
		return finish("error")
	}

	type frame struct {
		data []byte
		err  error
	}
	frames := make(chan frame)
	done := make(chan struct{})
	defer close(done)
	safego.Go(logger, "websocket.debug.read", func() {
		for {
			_, data, err := conn.ReadMessage()
			select {
			case frames <- frame{data, err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	})

	timeout := time.NewTimer(opts.Duration)
	defer timeout.Stop()
	tick := time.NewTicker(debugReadSlice)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return finish("cancelled")
		case <-timeout.C:
			return finish("duration")
		case <-tick.C:
			if sessionEnded() {
				return finish("session_ended")
			}
		case f := <-frames:
			if f.err != nil {
				var ce *websocket.CloseError
				if errors.As(f.err, &ce) {
					res.CloseCode = ce.Code
				} else {
					res.Error = "read failed"
				}
				return finish("closed")
			}
			payload, signed, err := decode(f.data)
			if err != nil {
				res.Error = err.Error()
				return finish("error")
			}
			ev := debugEventOf(payload)
			ev.TMS = time.Since(start).Milliseconds()
			if ev.Kind == "response" && res.SubscribeAck == "" && gjson.GetBytes(payload, "id").Raw == subID {
				res.SubscribeAck = string(payload)
			}
			if opts.IncludePayload {
				ev.Payload = string(payload)
			}
			if opts.IncludeSigned {
				ev.Signed = signed
			}
			res.Events = append(res.Events, ev)
			if len(res.Events) >= opts.MaxEvents {
				return finish("max_events")
			}
		}
	}
}

// debugEventOf reads what a frame names: the subscription and, for a
// notification, the block, transaction and log it is about.
func debugEventOf(payload []byte) DebugEvent {
	j := gjson.ParseBytes(payload)
	switch {
	case j.Get("params.subscription").Exists():
		ev := DebugEvent{Kind: "notification", Subscription: j.Get("params.subscription").String()}
		res := j.Get("params.result")
		if res.Type == gjson.String {
			ev.TxHash = res.String()
			return ev
		}
		if n, ok := hexUint(res.Get("number")); ok {
			ev.BlockNumber = &n
			ev.BlockHash = res.Get("hash").String()
		}
		if n, ok := hexUint(res.Get("blockNumber")); ok {
			ev.BlockNumber = &n
			ev.BlockHash = res.Get("blockHash").String()
		}
		if t := res.Get("transactionHash"); t.Exists() {
			ev.TxHash = t.String()
		} else if t := res.Get("hash"); t.Exists() && !res.Get("number").Exists() {
			ev.TxHash = t.String()
		}
		if n, ok := hexUint(res.Get("logIndex")); ok {
			ev.LogIndex = &n
		}
		return ev
	case j.Get("id").Exists():
		return DebugEvent{Kind: "response"}
	}
	return DebugEvent{Kind: "other"}
}

// hexUint parses a 0x-prefixed quantity.
func hexUint(v gjson.Result) (uint64, bool) {
	if v.Type != gjson.String || !strings.HasPrefix(v.String(), "0x") {
		return 0, false
	}
	n, err := strconv.ParseUint(v.String()[2:], 16, 64)
	return n, err == nil
}
