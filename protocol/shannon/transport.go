package shannon

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"

	"github.com/pokt-network/sage/domain"
)

// rpcTypeToShared maps domain RPC types to poktroll shared types for the Rpc-Type header.
// The relay miner uses this header to select the correct backend service config.
var rpcTypeToShared = map[domain.RPCType]sharedtypes.RPCType{
	domain.RPCTypeJSONRPC:   sharedtypes.RPCType_JSON_RPC,
	domain.RPCTypeREST:      sharedtypes.RPCType_REST,
	domain.RPCTypeCometBFT:  sharedtypes.RPCType_COMET_BFT,
	domain.RPCTypeWebSocket: sharedtypes.RPCType_WEBSOCKET,
	domain.RPCTypeGRPC:      sharedtypes.RPCType_GRPC,
}

// rpcTypeHeaderValue is the wire value of the Rpc-Type header/metadata: the
// numeric poktroll enum. Empty when the type has no mapping, which callers
// treat as "send no header" rather than sending UNKNOWN_RPC.
func rpcTypeHeaderValue(rpcType domain.RPCType) string {
	st, ok := rpcTypeToShared[rpcType]
	if !ok || st == sharedtypes.RPCType_UNKNOWN_RPC {
		return ""
	}
	return strconv.Itoa(int(st))
}

// payloadContentType is the media type the supplier's backend should see. JSON
// is the default because every other SAGE transport is JSON; a gRPC relay
// carries protobuf and must say so, or the miner's backend-type heuristic reads
// it as an ordinary HTTP call.
func payloadContentType(payload domain.Payload) string {
	if ct := payload.ContentType(); ct != "" {
		return ct
	}
	return "application/json"
}

// payloadHTTPMethod is the verb the supplier's backend should see. POST is the
// default because every JSON-RPC chain uses it; only REST/CometBFT set a verb.
func payloadHTTPMethod(payload domain.Payload) string {
	if m := payload.HTTPMethod(); m != "" {
		return m
	}
	return http.MethodPost
}

// payloadURL joins the supplier's staked URL with the payload's request URI.
// The staked URL is an origin ("https://host"), so a path on it is unexpected;
// if one is present it is kept as a prefix rather than silently dropped.
func payloadURL(supplierURL string, payload domain.Payload) string {
	path := payload.Path()
	if path == "" || path == "/" {
		return supplierURL
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimSuffix(supplierURL, "/") + path
}

// newRelayClient is the client every relay goes out on.
//
// It never follows a redirect, as PATH never did: the 3xx comes back as the
// response, which SendRelay grades as a retryable endpoint error. A relay is
// always a POST, so a 301/302/303 could not have worked anyway (Go re-sends it
// as a bodyless GET), and a 307/308 re-sends the signed relay, body and all, to
// wherever the supplier points — including this pod's own loopback, where the
// admin API may take a POST with no token. A supplier could reset its own
// reputation that way.
func newRelayClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		Transport:     newRelayTransport(),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// newRelayTransport is http.DefaultTransport with its pool sized for a gateway.
//
// The default keeps 2 idle connections per host and 100 in all, so at mainnet
// rates a busy supplier's connections were closed as fast as they came back
// and nearly every relay paid a fresh TLS handshake: a 2026-09-22 profile put
// 31% of CPU in client handshakes, 24% in verifying supplier certificates.
// The pool sizes are PATH's. The session cache lets a connection that does
// have to be opened resume instead of verifying the chain again.
func newRelayTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 10000
	t.MaxIdleConnsPerHost = 500
	t.TLSClientConfig = &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(1024)}
	return t
}

// sendHTTP sends an HTTP POST request to the given URL with the provided body.
// It sets the Rpc-Type header so the relay miner can route to the correct backend.
func (p *Protocol) sendHTTP(ctx context.Context, url string, body []byte, rpcType domain.RPCType) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("sendHTTP: failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if v := rpcTypeHeaderValue(rpcType); v != "" {
		req.Header.Set("Rpc-Type", v)
	}

	resp, err := doTraced(p.httpClient, req)
	if err != nil {
		return nil, fmt.Errorf("sendHTTP: request failed: %w", err)
	}
	return resp, nil
}

// doTraced is client.Do with one fact attached to a failure: whether a
// connection to the host was ever obtained. An error with none is wrapped in
// domain.ConnectError.
//
// The fact has to be observed, not inferred. net/http runs the dial under the
// request context, so when Client.Timeout fires against a host that drops
// SYNs the error is a url.Error around an http timeout — the identical shape a
// host that accepted the connection and never answered produces. Only the
// GotConn hook separates the two, and the difference is a dead host (circuit
// breaker) versus a slow method (method block).
//
// GotConn fires for a reused pooled connection too, which is right: the host
// was reachable. It fires only after the TLS handshake, so a handshake failure
// counts as no connection, matching the other connect-level shapes.
func doTraced(client *http.Client, req *http.Request) (*http.Response, error) {
	var connected atomic.Bool
	trace := &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := client.Do(req)
	if err != nil && !connected.Load() {
		return nil, &domain.ConnectError{Cause: err}
	}
	return resp, err
}
