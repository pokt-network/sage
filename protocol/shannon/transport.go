package shannon

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"

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
