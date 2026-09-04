package domain

import "strings"

// RPCType identifies the RPC protocol of a request.
type RPCType string

// The RPC protocols a request can arrive over. SAGE is told which one it is
// handling — by config and by the request's shape — rather than sniffing it
// from the payload, and the value is immutable through the middleware chain
// once Parse has set it.
const (
	RPCTypeJSONRPC   RPCType = "json_rpc"
	RPCTypeREST      RPCType = "rest"
	RPCTypeCometBFT  RPCType = "comet_bft"
	RPCTypeWebSocket RPCType = "websocket"

	// RPCTypeGRPC is relayed as a gRPC call to the relay miner's relay service,
	// not as an HTTP POST of the signed request — the miner routes those two
	// down different paths, and only the gRPC one reaches the HTTP/2 client it
	// uses to talk to gRPC backends. The framing on that hop (native gRPC or
	// gRPC-Web) depends on what the supplier's front door accepts; see
	// protocol/shannon/grpc.go.
	RPCTypeGRPC RPCType = "grpc"

	RPCTypeUnknown RPCType = "unknown"
)

// AllRPCTypes returns every RPC type a request can carry, excluding Unknown.
// Used where a caller must act on an endpoint across every protocol it serves
// — e.g. resetting an endpoint's reputation, which is scored per RPC type.
func AllRPCTypes() []RPCType {
	return []RPCType{
		RPCTypeJSONRPC,
		RPCTypeREST,
		RPCTypeCometBFT,
		RPCTypeWebSocket,
		RPCTypeGRPC,
	}
}

// ParseRPCType resolves the spelling a client or config uses for an RPC type
// — "json_rpc", "REST", "comet_bft" — to the RPCType it names. Matching is
// case-insensitive because PATH's RPC-Type header is documented in upper case
// while its config keys are lower case, and a client copying either must not
// be refused. Unknown spellings, including "unknown" itself, report false.
func ParseRPCType(s string) (RPCType, bool) {
	for _, t := range AllRPCTypes() {
		if strings.EqualFold(s, string(t)) {
			return t, true
		}
	}
	return RPCTypeUnknown, false
}
