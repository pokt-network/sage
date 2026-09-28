package shannon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol"
)

// Pinned-supplier probe.
//
// Every question about whether a supplier serves what it claims — a feed
// richer than its peers', an HTTP face that fails where its WebSocket face
// does not — needs the same request sent at the same moment to that supplier,
// to its peers and to a reference, and the answers compared. A relay miner
// only accepts a request signed for a session it is in, so that can only be
// done from inside the gateway. These are the admin probe's two halves: one
// signed relay, and one WebSocket subscription held for a bounded time, each
// to exactly the target named and to nothing else.
//
// A probe gathers evidence, so it changes nothing: it runs outside the
// middleware chain (no retry, hedge, scoring, metrics or client ledger), and a
// response that fails verification is reported rather than blacklisted. It
// keeps the signed bytes, which let anyone re-verify later that the supplier
// served exactly that content in that session.
const (
	// debugRelayInterval spaces probe relays: 10 a second, fleet-wide per pod.
	debugRelayInterval = 100 * time.Millisecond
	// debugMaxSubscriptions caps concurrent probe subscriptions per pod.
	debugMaxSubscriptions = 5
	// DebugMaxDuration and DebugMaxEvents bound one probe subscription.
	DebugMaxDuration = 300 * time.Second
	DebugMaxEvents   = 20000
	// DebugMaxPayload bounds a probe request body.
	DebugMaxPayload = 64 << 10
	// debugTargetReference names the configured reference endpoint.
	debugTargetReference = "reference"
	// debugReferenceTimeout bounds a reference relay: the admin server sets
	// no timeouts, so a hung reference would otherwise hold its caller.
	debugReferenceTimeout = 30 * time.Second
)

// DebugSession is the session a probe was signed for.
type DebugSession struct {
	SessionID   string `json:"session_id"`
	Application string `json:"application"`
	StartHeight int64  `json:"start_height"`
	EndHeight   int64  `json:"end_height"`
}

// DebugTarget is who a probe went to.
type DebugTarget struct {
	// Endpoint is the registration probed, or "reference".
	Endpoint string `json:"endpoint"`
	Supplier string `json:"supplier,omitempty"`
	Operator string `json:"operator,omitempty"`
	Owner    string `json:"owner,omitempty"`
}

// DebugRelayResult is one probe relay's outcome. Error is set when the relay
// did not produce a verified answer; whatever was collected before that is
// still reported.
type DebugRelayResult struct {
	ServiceID  string        `json:"service_id"`
	RPCType    string        `json:"rpc_type"`
	Target     DebugTarget   `json:"target"`
	Session    *DebugSession `json:"session,omitempty"`
	HTTPStatus int           `json:"http_status"`
	LatencyMS  int64         `json:"latency_ms"`
	// Body is the backend's answer as the relay carried it.
	Body  string `json:"body,omitempty"`
	Error string `json:"error,omitempty"`
	// SignedRequest and SignedResponse are the wire bytes, base64 when
	// marshalled: the RelayRequest SAGE signed and the RelayResponse the
	// supplier signed over the payload and session header.
	SignedRequest  []byte `json:"signed_request_b64,omitempty"`
	SignedResponse []byte `json:"signed_response_b64,omitempty"`
}

// debugLimits holds the probe's rate and concurrency caps.
type debugLimits struct {
	mu   sync.Mutex
	next time.Time
	subs chan struct{}
}

func newDebugLimits() *debugLimits {
	return &debugLimits{subs: make(chan struct{}, debugMaxSubscriptions)}
}

// takeRelay admits one probe relay, or reports the rate cap.
func (l *debugLimits) takeRelay(now time.Time) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Before(l.next) {
		return false
	}
	l.next = now.Add(debugRelayInterval)
	return true
}

// takeSubscription admits one probe subscription; release gives the slot back.
func (l *debugLimits) takeSubscription() (release func(), ok bool) {
	if l == nil {
		return func() {}, true
	}
	select {
	case l.subs <- struct{}{}:
		return func() { <-l.subs }, true
	default:
		return nil, false
	}
}

// SetDebugReferences installs the per-service reference endpoints a probe may
// name as target "reference": plain HTTP and WebSocket URLs, unsigned. Call
// once at wire time. The URLs are never logged or returned.
func (p *Protocol) SetDebugReferences(http, ws map[domain.ServiceID]string) {
	p.debugRefHTTP, p.debugRefWS = http, ws
}

// resolveDebugTarget finds the registration a probe target names in the
// service's current session: an endpoint address, a URL a registration serves
// the RPC type from, an operator (registrable domain), a supplier (operator
// account) address or an owner address.
// Only current registrations are candidates — a target is a key to match,
// never an address to dial — and among several matches one is picked at
// random. No match is ErrDebugTargetNotFound, naming the operators present.
func (p *Protocol) resolveDebugTarget(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType, target string) (domain.EndpointAddr, *endpoint, error) {
	eps, err := p.RegisteredEndpoints(ctx, serviceID, rpcType)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", protocol.ErrDebugTargetNotFound, err)
	}
	present := map[string]bool{}
	var matches domain.EndpointAddrList
	for _, addr := range eps {
		ep, ok := p.sessions.lookupEndpoint(addr)
		if !ok {
			continue
		}
		present[addr.Operator()] = true
		url, _ := p.endpointURL(serviceID, ep, rpcType)
		if string(addr) == target || url == target || addr.Operator() == target || ep.Supplier() == target || ep.Owner() == target {
			matches = append(matches, addr)
		}
	}
	if len(matches) == 0 {
		ops := make([]string, 0, len(present))
		for op := range present {
			ops = append(ops, op)
		}
		sort.Strings(ops)
		return "", nil, fmt.Errorf("%w: %q on %s %s; operators in the session: %s",
			protocol.ErrDebugTargetNotFound, target, serviceID, rpcType, strings.Join(ops, ", "))
	}
	addr := matches[time.Now().UnixNano()%int64(len(matches))]
	ep, _ := p.sessions.lookupEndpoint(addr)
	return addr, ep, nil
}

// DebugRelay sends one relay to exactly the target and reports it with its
// signed bytes. See the file comment; the admin route is its only caller.
func (p *Protocol) DebugRelay(ctx context.Context, serviceID domain.ServiceID, target string, rpcType domain.RPCType, body []byte) (DebugRelayResult, error) {
	res := DebugRelayResult{ServiceID: string(serviceID), RPCType: string(rpcType)}
	method := gjson.GetBytes(body, "method").String()
	if method == "" || len(body) > DebugMaxPayload {
		return res, fmt.Errorf("%w: a JSON-RPC request with a method, at most %d bytes", protocol.ErrDebugBadRequest, DebugMaxPayload)
	}
	if !p.debugLimits.takeRelay(time.Now()) {
		return res, fmt.Errorf("%w: at most %d relays a second", protocol.ErrDebugBusy, int(time.Second/debugRelayInterval))
	}
	p.logger.Warn("admin: debug relay", "service_id", serviceID, "target", target, "rpc_type", rpcType, "method", method)

	if target == debugTargetReference {
		return p.debugReferenceRelay(ctx, serviceID, body, res)
	}
	addr, ep, err := p.resolveDebugTarget(ctx, serviceID, rpcType, target)
	if err != nil {
		return res, err
	}
	res.Target = DebugTarget{Endpoint: string(addr), Supplier: ep.Supplier(), Operator: addr.Operator(), Owner: ep.Owner()}

	var ev relayEvidence
	start := time.Now()
	resp, err := p.sendRelay(ctx, serviceID, addr, domain.NewPayload(body, rpcType, method), &ev)
	res.LatencyMS = time.Since(start).Milliseconds()
	res.SignedRequest, res.SignedResponse, res.HTTPStatus = ev.Request, ev.Response, ev.HTTPStatus
	if h := ev.Session; h != nil {
		res.Session = &DebugSession{SessionID: h.SessionId, Application: h.ApplicationAddress,
			StartHeight: h.SessionStartBlockHeight, EndHeight: h.SessionEndBlockHeight}
	}
	if err != nil {
		res.Error = err.Error()
		return res, nil
	}
	res.HTTPStatus, res.Body = resp.HTTPStatusCode, string(resp.Body)
	return res, nil
}

// debugReferenceRelay posts the request to the configured reference, unsigned.
func (p *Protocol) debugReferenceRelay(ctx context.Context, serviceID domain.ServiceID, body []byte, res DebugRelayResult) (DebugRelayResult, error) {
	url := p.debugRefHTTP[serviceID]
	if url == "" {
		return res, fmt.Errorf("%w: no debug_reference_url for %s", protocol.ErrDebugTargetNotFound, serviceID)
	}
	res.Target = DebugTarget{Endpoint: debugTargetReference}
	ctx, cancel := context.WithTimeout(ctx, debugReferenceTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		res.Error = "reference request could not be built"
		return res, nil
	}
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		// The error text can carry the URL, and the URL can carry a key.
		res.Error = "reference request failed"
		return res, nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	res.HTTPStatus, res.Body = resp.StatusCode, string(b)
	return res, nil
}
