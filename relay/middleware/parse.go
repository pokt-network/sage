// Package middleware provides composable relay middleware for the SAGE gateway.
package middleware

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// serviceIDHeader is the HTTP header used to specify the target service.
const serviceIDHeader = "Target-Service-Id"

// cometBFTPaths are known CometBFT RPC endpoint path prefixes and exact paths.
var cometBFTPaths = []string{
	"/status",
	"/health",
	"/block",
	"/block_results",
	"/blockchain",
	"/broadcast_tx",
	"/commit",
	"/consensus_state",
	"/dump_consensus_state",
	"/genesis",
	"/net_info",
	"/num_unconfirmed_txs",
	"/tx",
	"/tx_search",
	"/unconfirmed_txs",
	"/validators",
	"/abci_info",
	"/abci_query",
}

// cosmosPaths are known Cosmos REST API path prefixes.
var cosmosPaths = []string{
	"/cosmos/",
	"/ibc/",
}

// Parse returns a middleware that reads the Target-Service-Id header, looks up
// the QoS plugin, detects the RPC type, and calls plugin.ParseRequest to
// extract payloads. It sets ctx.ServiceID, ctx.RPCType, ctx.Plugin, and
// ctx.Payloads.
func Parse(registry *qos.Registry) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Extract service ID.
			serviceID := ctx.HTTPRequest.Header.Get(serviceIDHeader)
			if serviceID == "" {
				ctx.Err = domain.NewRelayError(
					domain.ErrValidation,
					"missing Target-Service-Id header",
					nil,
					false,
				)
				if ctx.Writer != nil {
					ctx.Writer.SetStatusCode(http.StatusBadRequest)
					_ = ctx.Writer.Write([]byte(`{"error":"missing Target-Service-Id header"}`))
				}
				return ctx.Err
			}
			ctx.ServiceID = domain.ServiceID(serviceID)

			// Look up QoS plugin.
			plugin := registry.Get(ctx.ServiceID)
			ctx.Plugin = plugin

			// Read the body once, bounded; the same slice is shared by RPC type
			// detection and plugin parsing (plugins must not re-read req.Body).
			body, err := readBody(ctx.HTTPRequest)
			if err != nil {
				ctx.Err = domain.NewRelayError(
					domain.ErrValidation,
					"failed to read request body",
					err,
					false,
				)
				if ctx.Writer != nil {
					ctx.Writer.SetStatusCode(http.StatusBadRequest)
					_ = ctx.Writer.Write([]byte(`{"error":"failed to read request body"}`))
				}
				return ctx.Err
			}

			// Detect RPC type.
			ctx.RPCType = detectRPCType(ctx.HTTPRequest, body)

			// Parse request via plugin if available.
			if plugin != nil {
				payloads, err := plugin.ParseRequest(ctx.Ctx, ctx.HTTPRequest, body, ctx.RPCType)
				if err != nil {
					ctx.Err = domain.NewRelayError(
						domain.ErrValidation,
						"failed to parse request",
						err,
						false,
					)
					if ctx.Writer != nil {
						ctx.Writer.SetStatusCode(http.StatusBadRequest)
						_ = ctx.Writer.Write([]byte(`{"error":"failed to parse request"}`))
					}
					return ctx.Err
				}
				ctx.Payloads = payloads
			} else {
				// No plugin: wrap the raw body as a single payload, keeping the
				// path and verb so non-JSON-RPC requests still reach the right
				// backend route.
				ctx.Payloads = []domain.Payload{
					domain.NewPayload(body, ctx.RPCType, "").
						WithHTTP(ctx.HTTPRequest.URL.RequestURI(), ctx.HTTPRequest.Method),
				}
			}

			return next.HandleRelay(ctx)
		})
	}
}

// detectRPCType determines the RPC type from an HTTP request and its
// already-read body.
func detectRPCType(req *http.Request, body []byte) domain.RPCType {
	// WebSocket upgrade.
	if isWebSocketUpgrade(req) {
		return domain.RPCTypeWebSocket
	}

	// gRPC announces itself by media type. Checked before the path tables
	// because a gRPC method path (/cosmos.bank.v1beta1.Query/Params) resembles
	// nothing in them — note "/cosmos." is not the "/cosmos/" REST prefix.
	if isGRPCContentType(req.Header.Get("Content-Type")) {
		return domain.RPCTypeGRPC
	}

	path := req.URL.Path

	// CometBFT paths.
	for _, p := range cometBFTPaths {
		if path == p || strings.HasPrefix(path, p+"/") {
			return domain.RPCTypeCometBFT
		}
	}

	// Cosmos REST paths.
	for _, p := range cosmosPaths {
		if strings.HasPrefix(path, p) {
			return domain.RPCTypeREST
		}
	}

	// For POST requests, inspect the body.
	if req.Method == http.MethodPost {
		trimmed := bytes.TrimSpace(body)
		if len(trimmed) > 0 {
			first := trimmed[0]
			if first == '{' || first == '[' {
				// Check for jsonrpc field.
				if bytes.Contains(trimmed, []byte(`"jsonrpc"`)) {
					return domain.RPCTypeJSONRPC
				}
				// Batch (array) without explicit jsonrpc key — still JSON-RPC batch.
				if first == '[' {
					return domain.RPCTypeJSONRPC
				}
			}
		}
		return domain.RPCTypeJSONRPC
	}

	return domain.RPCTypeJSONRPC
}

// isGRPCContentType reports whether a media type is gRPC in any framing:
// native "application/grpc" and the gRPC-Web variants share the prefix. Which
// framing SAGE then uses toward the supplier is a transport decision, not this
// one — see protocol/shannon/grpc.go.
func isGRPCContentType(contentType string) bool {
	// Strip any parameters ("application/grpc-web+proto; charset=utf-8").
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = contentType[:i]
	}
	contentType = strings.ToLower(strings.TrimSpace(contentType))

	const prefix = "application/grpc"
	if !strings.HasPrefix(contentType, prefix) {
		return false
	}
	// Bare "application/grpc", an encoding suffix ("+proto", "+json"), or the
	// gRPC-Web family ("-web", "-web-text"). A type that merely begins with the
	// same letters is a different media type, not a gRPC framing.
	rest := contentType[len(prefix):]
	return rest == "" || rest[0] == '+' || strings.HasPrefix(rest, "-web")
}

// isWebSocketUpgrade returns true if the request is a WebSocket upgrade.
func isWebSocketUpgrade(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade")
}

// maxBodyBytes bounds request bodies to avoid unbounded allocation. Typical
// JSON-RPC requests are well under 1 KiB; anything past 1 MiB is pathological.
const maxBodyBytes = 1 << 20 // 1 MiB

// readDeclaredLength reads a body whose length Content-Length already states,
// into a buffer sized to exactly that. Returns nil — not an error — when the
// header offers nothing usable, meaning the caller should read until EOF
// instead.
//
// It exists because bytes.Buffer.ReadFrom insists on bytes.MinRead (512) bytes
// of headroom before every read. A JSON-RPC request is an order of magnitude
// smaller than that, so the padding, not the body, was the allocation — on
// every relay.
//
// Content-Length is a claim, not a guarantee. net/http stops a server request
// body at the declared length, but this is reachable with any *http.Request, so
// a short body is returned at its real length and a long one is read to the end
// rather than silently truncated.
func readDeclaredLength(contentLength int64, r io.Reader) ([]byte, error) {
	if contentLength <= 0 || contentLength > maxBodyBytes {
		return nil, nil
	}

	data := make([]byte, contentLength)
	read, err := io.ReadFull(r, data)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	data = data[:read]

	// One byte past the promise. Stack-allocated, so the common answer — there
	// is nothing more — costs nothing.
	var probe [1]byte
	switch n, err := r.Read(probe[:]); {
	case n > 0:
		// The body is longer than it claimed. Hand the already-read prefix back
		// to the buffer path so the size limit still applies to the whole thing.
		rest, restErr := io.ReadAll(r)
		if restErr != nil {
			return nil, restErr
		}
		return append(append(data, probe[0]), rest...), nil
	case err != nil && !errors.Is(err, io.EOF):
		return nil, err
	}

	return data, nil
}

// readBody reads the full request body (bounded by maxBodyBytes) and replaces
// the Body so it can be read again by downstream handlers.
func readBody(req *http.Request) ([]byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	// One byte past the limit, so a body that is exactly at the cap is told
	// apart from one that exceeds it.
	limited := io.LimitReader(req.Body, maxBodyBytes+1)

	data, err := readDeclaredLength(req.ContentLength, limited)
	if err != nil {
		return nil, err
	}
	if data == nil {
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(limited); err != nil {
			return nil, err
		}
		data = buf.Bytes()
	}
	if len(data) > maxBodyBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxBodyBytes)
	}
	req.Body = io.NopCloser(bytes.NewReader(data))
	return data, nil
}
