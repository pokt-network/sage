package domain

// RPCType identifies the RPC protocol of a request.
type RPCType string

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
