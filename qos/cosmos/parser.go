package cosmos

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// cometBFTMethods is the CometBFT RPC catalogue: every method the node
// answers, as a JSON-RPC method name and, with a leading slash, as an HTTP
// path (GET /status is {"method":"status"}). Exact names rather than
// prefixes, because NormalizeMethod uses this as the bounded set behind a
// metric label. Reference: https://docs.cometbft.com/v1.0/rpc/
var cometBFTMethods = map[string]bool{
	// node and network
	"status":          true,
	"health":          true,
	"net_info":        true,
	"genesis":         true,
	"genesis_chunked": true,
	// blocks and headers
	"block":                true,
	"block_by_hash":        true,
	"block_results":        true,
	"block_search":         true,
	"blockchain":           true,
	"commit":               true,
	"header":               true,
	"header_by_hash":       true,
	"validators":           true,
	"consensus_params":     true,
	"consensus_state":      true,
	"dump_consensus_state": true,
	// transactions
	"tx":                  true,
	"tx_search":           true,
	"unconfirmed_txs":     true,
	"num_unconfirmed_txs": true,
	"broadcast_tx_sync":   true,
	"broadcast_tx_async":  true,
	"broadcast_tx_commit": true,
	"broadcast_evidence":  true,
	"check_tx":            true,
	// abci
	"abci_info":  true,
	"abci_query": true,
	// websocket subscriptions, which also arrive as plain JSON-RPC
	"subscribe":       true,
	"unsubscribe":     true,
	"unsubscribe_all": true,
}

// cosmosPaths are well-known Cosmos REST API path prefixes (gRPC-gateway).
var cosmosPaths = []string{
	"/cosmos/",
	"/ibc/",
	"/osmosis/",
	"/noble/",
}

// isCometBFTPath reports whether the URL path is a CometBFT RPC endpoint:
// its first segment is a catalogued method. One catalogue serves the JSON-RPC
// and the HTTP face, so the two cannot drift apart.
func isCometBFTPath(path string) bool {
	seg := strings.TrimPrefix(path, "/")
	if i := strings.IndexAny(seg, "/?"); i >= 0 {
		seg = seg[:i]
	}
	return seg != "" && cometBFTMethods[strings.ToLower(seg)]
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

// classifyRPCType is the plugin's answer to which type a request is relayed
// as; see qos.RPCTypeClassifier. The Cosmos plugin fronts up to four
// surfaces, and CometBFT is itself two faces of one node, so the answer
// depends on what the service declares in rpc_types:
//
//  1. gRPC, by media type — a method path like /cosmos.bank.v1beta1.Query/Params
//     starts with "/cosmos." and would otherwise read as REST.
//  2. A Cosmos REST path (/cosmos/, /ibc/, ...) is rest.
//  3. A CometBFT path (GET /status, POST /block) is the node's HTTP face:
//     comet_bft when declared, else rest — on Pocket a supplier stakes rest
//     for exactly that face, with no comet_bft stake.
//  4. A JSON body with a method is the JSON-RPC face: a CometBFT method is
//     comet_bft when declared, else json_rpc — the same supplier stakes
//     json_rpc for this face; any other method is json_rpc (the EVM surface
//     of an EVM-enabled chain, or the client's mistake).
//  5. Anything else keeps what generic detection said, or is rest.
//
// When comet_bft is declared it is preferred for both faces, because it is
// the surface the request names. A service whose suppliers do not stake it
// should not declare it; with json_rpc and rest declared instead, both faces
// reach the pools that can serve them. An undeclared result is returned as
// is, so that ParseRequest refuses it with the declared list.
func classifyRPCType(req *http.Request, body []byte, detected domain.RPCType, supported []domain.RPCType) domain.RPCType {
	declares := func(t domain.RPCType) bool { return isRPCTypeSupported(t, supported) }
	faceType := func(alt domain.RPCType) domain.RPCType {
		if declares(domain.RPCTypeCometBFT) || !declares(alt) {
			return domain.RPCTypeCometBFT
		}
		return alt
	}

	if detected == domain.RPCTypeGRPC || strings.HasPrefix(req.Header.Get("Content-Type"), "application/grpc") {
		return domain.RPCTypeGRPC
	}
	path := req.URL.Path
	if isCosmosRESTPath(path) {
		return domain.RPCTypeREST
	}
	if isCometBFTPath(path) {
		return faceType(domain.RPCTypeREST)
	}
	if req.Method == http.MethodPost && len(body) > 0 && gjson.ValidBytes(body) {
		if method := gjson.GetBytes(body, "method").String(); method != "" {
			if isCometBFTMethod(method) {
				return faceType(domain.RPCTypeJSONRPC)
			}
			return domain.RPCTypeJSONRPC
		}
	}
	if detected != domain.RPCTypeUnknown && detected != "" {
		return detected
	}
	return domain.RPCTypeREST
}

// parseRequest builds the single Payload for a request. rpcType is the type
// Parse settled on — the plugin's own classification unless the client's
// RPC-Type header overrode it — and the payload carries it unchanged, so
// what was validated and pooled is what is sent. RPCTypeUnknown (a caller
// with no Parse in front of it) classifies here instead.
//
// The method is read from a JSON body when there is one, whatever the type:
// it is what NormalizeMethod and the reputation key see, and a CometBFT
// request relayed as json_rpc on a service that declares no comet_bft is
// still "status" to both.
func parseRequest(req *http.Request, body []byte, rpcType domain.RPCType, supported []domain.RPCType) (domain.Payload, error) {
	if rpcType == domain.RPCTypeUnknown || rpcType == "" {
		rpcType = classifyRPCType(req, body, domain.RPCTypeUnknown, supported)
	}
	path := req.URL.Path
	if rpcType == domain.RPCTypeGRPC {
		// The backend is a native gRPC server, so it is told "application/grpc"
		// regardless of the framing the client used — for a unary call the
		// request body is byte-identical between gRPC and gRPC-Web (they
		// differ only in trailers, which requests do not carry).
		return withRequestHTTP(domain.NewPayload(body, domain.RPCTypeGRPC, grpcMethodFromPath(path)), req).
			WithContentType("application/grpc"), nil
	}
	method := ""
	if req.Method == http.MethodPost && len(body) > 0 && gjson.ValidBytes(body) {
		method = gjson.GetBytes(body, "method").String()
	}
	return withRequestHTTP(domain.NewPayload(body, rpcType, method), req), nil
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

// isRPCTypeSupported returns true if rpcType is in the supported set.
func isRPCTypeSupported(rpcType domain.RPCType, supported []domain.RPCType) bool {
	for _, s := range supported {
		if s == rpcType {
			return true
		}
	}
	return false
}
