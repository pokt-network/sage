package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// jsonRPCInvalidRequest is the JSON-RPC 2.0 code for a request the server
// refuses to process: malformed, addressed to nothing it serves, or carrying
// a type it does not accept. Every rejection SAGE makes before a relay is
// attempted carries it, as PATH's do, so a client that branches on the code
// sees the same number from either gateway.
const jsonRPCInvalidRequest = -32600

// rejectRequest answers a request SAGE refuses to relay and returns the error
// the chain should stop on. It is the one place a pre-relay rejection is
// rendered — parse, validate and batch all go through it — so a client meets
// one shape for a client mistake instead of three.
//
// The shape follows the request: a JSON-RPC request gets a JSON-RPC error
// envelope with its own id echoed (null for a batch, whose members have many),
// and anything else gets {"error": message}. data, when non-nil, is attached
// to the error either way; it is what tells a caller *what* is allowed, not
// only that its request was not.
//
// body is the raw request, needed because a rejection can happen before any
// payload exists (a malformed body, an unsupported RPC-Type header) and the
// envelope's id has to come from somewhere. Nil is fine.
func rejectRequest(ctx *relay.Context, body []byte, status int, kind domain.ErrorKind, message string, cause error, data map[string]any) error {
	ctx.Err = domain.NewRelayError(kind, message, cause, false)
	if ctx.Writer == nil {
		return ctx.Err
	}
	ctx.Writer.SetHeader("Content-Type", "application/json")
	ctx.Writer.SetStatusCode(status)
	_ = ctx.Writer.Write(rejectionBody(ctx, body, message, data))
	return ctx.Err
}

// rejectionBody renders the rejection for the request's framing.
func rejectionBody(ctx *relay.Context, body []byte, message string, data map[string]any) []byte {
	if !isJSONRPCShaped(ctx, body) {
		out := map[string]any{"error": message}
		if data != nil {
			out["data"] = data
		}
		b, _ := json.Marshal(out)
		return b
	}
	errObj := map[string]any{"code": jsonRPCInvalidRequest, "message": message}
	if data != nil {
		errObj["data"] = data
	}
	b, err := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   map[string]any  `json:"error"`
	}{JSONRPC: "2.0", ID: jsonRPCIDOf(ctx, body), Error: errObj})
	if err != nil {
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}}`)
	}
	return b
}

// isJSONRPCShaped reports whether the request is JSON-RPC: by the type Parse
// settled on when it has, and by the body's first byte when it has not yet —
// a rejection can come before detection, and a client that sent a JSON-RPC
// object deserves a JSON-RPC answer even then.
func isJSONRPCShaped(ctx *relay.Context, body []byte) bool {
	switch ctx.RPCType {
	case domain.RPCTypeJSONRPC:
		return true
	case domain.RPCTypeREST, domain.RPCTypeCometBFT, domain.RPCTypeGRPC, domain.RPCTypeWebSocket:
		return false
	}
	if ctx.HTTPRequest != nil && ctx.HTTPRequest.Method != http.MethodPost {
		return false
	}
	trimmed := bytes.TrimSpace(body)
	return len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[')
}

// jsonRPCIDOf finds the id to echo: the single payload's when parsing got that
// far, the top-level object's from the raw body otherwise, and null for a
// batch or anything without one.
func jsonRPCIDOf(ctx *relay.Context, body []byte) json.RawMessage {
	if len(ctx.Payloads) == 1 {
		return ctx.Payloads[0].JSONRPCID()
	}
	if len(ctx.Payloads) > 1 {
		return json.RawMessage("null")
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		if id := gjson.GetBytes(trimmed, "id"); id.Exists() {
			return json.RawMessage(id.Raw)
		}
	}
	return json.RawMessage("null")
}
