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
// Parse is ParseWithServices with no rpc_types resolver, so an unrecognised
// path defaults to JSON-RPC (the pre-multi-surface behaviour).
func Parse(registry *qos.Registry) relay.Middleware {
	return ParseWithServices(registry, nil)
}

// ParseWithServices returns the parse middleware. rpcTypes, when non-nil,
// reports the RPC types a service declares in config, so a request to a
// REST-capable service on a path that is not a JSON-RPC entry point is
// classified REST rather than defaulting to JSON-RPC. Nil keeps the old
// default.
func ParseWithServices(registry *qos.Registry, rpcTypes func(domain.ServiceID) []string) relay.Middleware {
	return ParseWithOptions(registry, ParseOptions{RPCTypes: rpcTypes})
}

// ParseOptions carries what Parse needs from config. Every field has a
// working zero value so the two older constructors stay valid.
type ParseOptions struct {
	// RPCTypes reports the RPC types a service declares in config; nil means
	// nothing is known and an unrecognised path defaults to JSON-RPC.
	RPCTypes func(domain.ServiceID) []string
	// MaxBodyBytes caps the request body. Zero takes DefaultMaxBodyBytes.
	MaxBodyBytes int64
}

// rpcTypeHeader is the request header a client uses to say which surface it
// is addressing, ahead of any detection. PATH reads the same header.
const rpcTypeHeader = "RPC-Type"

// ParseWithOptions returns the parse middleware with every knob supplied.
func ParseWithOptions(registry *qos.Registry, opts ParseOptions) relay.Middleware {
	rpcTypes := opts.RPCTypes
	maxBody := opts.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = DefaultMaxBodyBytes
	}
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			// Extract service ID.
			serviceID := ctx.HTTPRequest.Header.Get(serviceIDHeader)
			if serviceID == "" {
				return rejectRequest(ctx, nil, http.StatusBadRequest, domain.ErrValidation,
					"missing "+serviceIDHeader+" header", nil, nil)
			}
			ctx.ServiceID = domain.ServiceID(serviceID)

			// Look up QoS plugin.
			plugin := registry.Get(ctx.ServiceID)
			ctx.Plugin = plugin

			// Read the body once, bounded; the same slice is shared by RPC type
			// detection and plugin parsing (plugins must not re-read req.Body).
			body, err := readBody(ctx.HTTPRequest, maxBody)
			if err != nil {
				if errors.Is(err, errBodyTooLarge) {
					return rejectRequest(ctx, nil, http.StatusRequestEntityTooLarge, domain.ErrValidation,
						fmt.Sprintf("request body exceeds %d bytes", maxBody), err, nil)
				}
				return rejectRequest(ctx, nil, http.StatusBadRequest, domain.ErrValidation,
					"failed to read request body", err, nil)
			}

			// The client's own declaration wins over detection: a REST call
			// whose body happens to look like JSON-RPC is still REST if the
			// caller says so. An unknown value is refused rather than ignored —
			// silently detecting instead would hand the request to the wrong
			// surface and make the header look honoured.
			if declared := ctx.HTTPRequest.Header.Get(rpcTypeHeader); declared != "" {
				rt, ok := domain.ParseRPCType(declared)
				if !ok {
					return rejectRequest(ctx, body, http.StatusBadRequest, domain.ErrValidation,
						fmt.Sprintf("invalid %s header value %q", rpcTypeHeader, declared), nil,
						map[string]any{"allowed_rpc_types": domain.AllRPCTypes()})
				}
				ctx.RPCType = rt
			} else {
				ctx.RPCType = detectRPCType(ctx.HTTPRequest, body, serviceDeclaresREST(rpcTypes, ctx.ServiceID))
			}

			// Parse request via plugin if available.
			if plugin != nil {
				payloads, err := plugin.ParseRequest(ctx.Ctx, ctx.HTTPRequest, body, ctx.RPCType)
				if err != nil {
					// The plugin's reason is SAGE-written ("batch array is
					// empty", "missing method") and is the one thing the
					// client needs to fix its request; it used to reach only
					// the log.
					return rejectRequest(ctx, body, http.StatusBadRequest, domain.ErrValidation,
						"invalid request: "+domain.ClientMessage(err), err, nil)
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

// jsonRPCEntryPaths are the paths at which a request is JSON-RPC even on a
// REST-capable service. Everything else on such a service is that chain's REST
// (or CometBFT, caught earlier). "/" is the conventional JSON-RPC endpoint;
// "/jsonrpc" is TRON's, and harmless elsewhere.
var jsonRPCEntryPaths = map[string]bool{"/": true, "": true, "/jsonrpc": true}

// detectRPCType determines the RPC type from an HTTP request and its
// already-read body. serviceDeclaresREST is whether the target service lists
// "rest" among its RPC types; when it does, a path-addressed request that is
// not a JSON-RPC entry point is that chain's native REST surface.
//
// The default for an unrecognised request used to be JSON-RPC unconditionally,
// which is an EVM assumption: it misrouted every chain-native REST namespace
// (TRON /wallet, Pocket /poktroll) to JSON-RPC suppliers. See
// docs/design/specs/2026-08-31-rpc-type-classification-design.md.
func detectRPCType(req *http.Request, body []byte, serviceDeclaresREST bool) domain.RPCType {
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

	// A JSON-RPC envelope in the body is self-identifying and wins on any
	// chain, even a REST-capable one: an EVM-compatible Cosmos chain answers
	// JSON-RPC at its root.
	if req.Method == http.MethodPost {
		trimmed := bytes.TrimSpace(body)
		if len(trimmed) > 0 {
			if trimmed[0] == '{' && bytes.Contains(trimmed, []byte(`"jsonrpc"`)) {
				return domain.RPCTypeJSONRPC
			}
			// A batch array is a JSON-RPC batch.
			if trimmed[0] == '[' {
				return domain.RPCTypeJSONRPC
			}
		}
	}

	path := req.URL.Path

	// CometBFT paths — a distinct surface with well-known paths, checked
	// before the REST default so a Cosmos chain's /status is not called REST.
	for _, p := range cometBFTPaths {
		if path == p || strings.HasPrefix(path, p+"/") {
			return domain.RPCTypeCometBFT
		}
	}

	// A REST-capable service, addressed by a path that is not a JSON-RPC entry
	// point, is that chain's REST surface — /cosmos/, /ibc/, /wallet/,
	// /poktroll/, and anything else, without a per-chain table.
	if serviceDeclaresREST && !jsonRPCEntryPaths[path] {
		return domain.RPCTypeREST
	}

	// The standard Cosmos REST prefixes still classify REST even when the
	// service's declared types are unknown (nil resolver): a request that
	// unmistakably names the Cosmos REST gateway should not become JSON-RPC.
	for _, p := range cosmosPaths {
		if strings.HasPrefix(path, p) {
			return domain.RPCTypeREST
		}
	}

	return domain.RPCTypeJSONRPC
}

// serviceDeclaresREST reports whether the service lists "rest" among its RPC
// types. A nil resolver reports false, which keeps the JSON-RPC default.
func serviceDeclaresREST(rpcTypes func(domain.ServiceID) []string, svc domain.ServiceID) bool {
	if rpcTypes == nil {
		return false
	}
	for _, t := range rpcTypes(svc) {
		if t == "rest" {
			return true
		}
	}
	return false
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

// DefaultMaxBodyBytes bounds request bodies when config sets no cap. It is
// PATH's default, so a batch that PATH accepts is accepted here; the 1 MiB
// this used to be turned large batches into 400s that only SAGE produced.
// Config: router_config.max_request_body_bytes.
const DefaultMaxBodyBytes int64 = 75 << 20 // 75 MiB

// errBodyTooLarge is the cause when a body exceeds the cap, so the rejection
// can be a 413 rather than the 400 a body that failed to read gets.
var errBodyTooLarge = errors.New("request body too large")

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
func readDeclaredLength(contentLength int64, r io.Reader, maxBodyBytes int64) ([]byte, error) {
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
func readBody(req *http.Request, maxBodyBytes int64) ([]byte, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	// One byte past the limit, so a body that is exactly at the cap is told
	// apart from one that exceeds it.
	limited := io.LimitReader(req.Body, maxBodyBytes+1)

	data, err := readDeclaredLength(req.ContentLength, limited, maxBodyBytes)
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
	if int64(len(data)) > maxBodyBytes {
		return nil, fmt.Errorf("%w: exceeds %d bytes", errBodyTooLarge, maxBodyBytes)
	}
	req.Body = io.NopCloser(bytes.NewReader(data))
	return data, nil
}
