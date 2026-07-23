package cosmos

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// cometBFTMethods is the set of JSON-RPC method names that map to the CometBFT RPC protocol.
var cometBFTMethods = map[string]bool{
	"status":               true,
	"health":               true,
	"block":                true,
	"block_results":        true,
	"blockchain":           true,
	"commit":               true,
	"validators":           true,
	"genesis":              true,
	"consensus_state":      true,
	"dump_consensus_state": true,
	"net_info":             true,
	"abci_info":            true,
	"abci_query":           true,
}

// cometBFTPathPrefixes lists URL path prefixes that indicate a CometBFT RPC request.
var cometBFTPathPrefixes = []string{
	"/status",
	"/health",
	"/block",
	"/blockchain",
	"/commit",
	"/validators",
	"/genesis",
	"/consensus_state",
	"/dump_consensus_state",
	"/net_info",
	"/abci_info",
	"/abci_query",
}

// cosmosPaths are well-known Cosmos REST API path prefixes (gRPC-gateway).
var cosmosPaths = []string{
	"/cosmos/",
	"/ibc/",
	"/osmosis/",
	"/noble/",
}

// isCometBFTPath returns true if the URL path maps to a CometBFT RPC endpoint.
func isCometBFTPath(path string) bool {
	for _, prefix := range cometBFTPathPrefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") || strings.HasPrefix(path, prefix+"?") {
			return true
		}
	}
	return false
}

// isCosmosRESTPath returns true if the URL path maps to a Cosmos REST (gRPC-gateway) endpoint.
func isCosmosRESTPath(path string) bool {
	for _, prefix := range cosmosPaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// isCometBFTMethod returns true if the JSON-RPC method name is a CometBFT method.
func isCometBFTMethod(method string) bool {
	return cometBFTMethods[strings.ToLower(method)]
}

// parseRequest inspects the request path/method and the pre-read body, and
// determines the RPC type and method. It returns a single Payload.
//
// Detection order:
//  1. If the path matches a known Cosmos REST path → RPCTypeREST
//  2. If the path matches a known CometBFT path (GET or POST) → RPCTypeCometBFT
//  3. If POST with a JSON-RPC body:
//     a. If the method is a CometBFT method → RPCTypeCometBFT
//     b. Otherwise → RPCTypeJSONRPC
//  4. Fall back to the caller-supplied rpcType hint.
func parseRequest(req *http.Request, body []byte, hintRPCType domain.RPCType) (domain.Payload, error) {
	path := req.URL.Path

	// gRPC is identified by media type upstream, not by path: a method path
	// like /cosmos.bank.v1beta1.Query/Params starts with "/cosmos." and would
	// otherwise fall through to the REST branch below.
	//
	// The backend is a native gRPC server, so it is told "application/grpc"
	// regardless of the framing the client used — for a unary call the request
	// body is byte-identical between gRPC and gRPC-Web (they differ only in
	// trailers, which requests do not carry).
	if hintRPCType == domain.RPCTypeGRPC {
		return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeGRPC, grpcMethodFromPath(path)), req).
			WithContentType("application/grpc"), nil
	}

	// Cosmos REST paths take priority.
	if isCosmosRESTPath(path) {
		return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeREST, ""), req), nil
	}

	// CometBFT path detection (covers GET and POST to known paths).
	if isCometBFTPath(path) {
		return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeCometBFT, ""), req), nil
	}

	// For POST requests, try to parse as JSON-RPC.
	if req.Method == http.MethodPost {
		if len(body) > 0 && gjson.ValidBytes(body) {
			method := gjson.GetBytes(body, "method").String()
			if method != "" {
				if isCometBFTMethod(method) {
					return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeCometBFT, method), req), nil
				}
				return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeJSONRPC, method), req), nil
			}
		}

		// Non-JSON-RPC POST (e.g., REST POST body).
		rpcType := hintRPCType
		if rpcType == domain.RPCTypeUnknown || rpcType == "" {
			rpcType = domain.RPCTypeREST
		}
		return withRequestHTTP(domain.NewPayload(body, rpcType, ""), req), nil
	}

	// GET request not matching any known path — treat as REST.
	return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeREST, ""), req), nil
}

// grpcMethodFromPath turns a gRPC path into the method name SAGE records for
// metrics and reputation: "/cosmos.bank.v1beta1.Query/Params" → "Query/Params".
// A path not in that shape yields "", the same "unknown method" the JSON-RPC
// parsers produce, rather than an error.
func grpcMethodFromPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/")
	slash := strings.LastIndexByte(trimmed, '/')
	if slash <= 0 || slash == len(trimmed)-1 {
		return ""
	}
	service, method := trimmed[:slash], trimmed[slash+1:]
	if dot := strings.LastIndexByte(service, '.'); dot >= 0 {
		service = service[dot+1:]
	}
	return service + "/" + method
}

// withRequestHTTP copies the incoming request's path (query string included)
// and verb onto the payload so the relay miner replays them against its
// backend. Without it every REST/CometBFT relay lands on the backend's root.
func withRequestHTTP(p domain.Payload, req *http.Request) domain.Payload {
	if req == nil || req.URL == nil {
		return p
	}
	return p.WithHTTP(req.URL.RequestURI(), req.Method)
}

// parseBlockHeight attempts to extract a block height from a response body.
// It handles two formats:
//
//  1. CometBFT JSON-RPC sync_info format:
//     {"result":{"sync_info":{"latest_block_height":"12345"}}}
//
//  2. Cosmos REST format:
//     {"height":"12345"}
func parseBlockHeight(response []byte) (uint64, error) {
	if len(response) == 0 {
		return 0, fmt.Errorf("cosmos: empty response")
	}

	// Try CometBFT format first.
	cometHeight := gjson.GetBytes(response, "result.sync_info.latest_block_height")
	if cometHeight.Exists() {
		return parseDecimalString(cometHeight.String(), "comet_bft latest_block_height")
	}

	// Try Cosmos REST format.
	restHeight := gjson.GetBytes(response, "height")
	if restHeight.Exists() {
		return parseDecimalString(restHeight.String(), "rest height")
	}

	return 0, fmt.Errorf("cosmos: no block height found in response")
}

// parseChainID extracts the chain identifier from a CometBFT /status response:
//
//	{"result":{"node_info":{"network":"cosmoshub-4"}}}
//
// Returns ("", false) when the response carries no chain identifier at all.
// That is the common case, not a failure: ExtractData sees every sampled relay
// response, and only /status reports the network. Absent means "this response
// cannot tell us", which is different from "this endpoint is on the wrong
// chain", and only the latter is worth acting on.
//
// Unlike EVM, there is nothing to normalize. A CometBFT network is an opaque
// name — "cosmoshub-4" — with no encoding, padding or casing to see through.
func parseChainID(response []byte) (string, bool) {
	if len(response) == 0 {
		return "", false
	}
	network := gjson.GetBytes(response, "result.node_info.network")
	if !network.Exists() || network.Type != gjson.String {
		return "", false
	}
	id := network.String()
	if id == "" {
		return "", false
	}
	return id, true
}

// parseDecimalString converts a string representing a decimal integer to uint64.
func parseDecimalString(s, field string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("cosmos: %s is empty", field)
	}
	var v uint64
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return 0, fmt.Errorf("cosmos: %s %q is not a valid decimal integer: %w", field, s, err)
	}
	return v, nil
}

// cometBFTStatusPayload builds the health check payload for a CometBFT /status request.
func cometBFTStatusPayload() domain.Payload {
	// CometBFT /status is a GET request with no body.
	return domain.NewPayload(nil, domain.RPCTypeCometBFT, "status").
		WithHTTP("/status", http.MethodGet)
}

// buildCometBFTJSONRPCPayload builds a JSON-RPC status payload for endpoints that
// only accept JSON-RPC POST (i.e., CometBFT JSON-RPC POST mode).
func buildCometBFTJSONRPCPayload() (domain.Payload, error) {
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "status",
		"params":  []interface{}{},
		"id":      1,
	})
	if err != nil {
		return domain.Payload{}, fmt.Errorf("cosmos: marshalling status payload: %w", err)
	}
	return domain.NewPayload(body, domain.RPCTypeCometBFT, "status"), nil
}

// isRPCTypeSupported returns true if rpcType is in the supported set.
func isRPCTypeSupported(rpcType domain.RPCType, supported []domain.RPCType) bool {
	for _, s := range supported {
		if s == rpcType {
			return true
		}
	}
	return false
}
