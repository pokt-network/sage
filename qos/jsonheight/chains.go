package jsonheight

import "github.com/pokt-network/sage/domain"

// The chains SAGE serves through this plugin, each declared as the two facts
// that differ: what to ask, and where the answer is.
//
// Every probe payload here must be verified against live suppliers before it
// is trusted. A wrong payload does not fail quietly — the probe runs, the
// supplier answers with an error or nothing the path matches, and the plugin
// grades a healthy endpoint down every cycle for refusing a request it never
// agreed to serve. Verification means one relay THROUGH SAGE or PATH, never a
// direct call to a supplier URL, which bypasses signing and proves nothing
// about what a relay would do.
var (
	// NEAR reports the head of the final chain in a JSON-RPC `block` call.
	// Clients ask the same question with the same method, so client traffic
	// contributes heights too.
	NEAR = Chain{
		Name:              "near",
		CheckName:         "near_block",
		Probe:             domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"block","params":{"finality":"final"}}`), domain.RPCTypeJSONRPC, "block"),
		HeightPath:        "result.header.height",
		RequestMethodPath: "method",
		HeightMethod:      "block",
	}

	// Sui counts checkpoints rather than blocks, and returns the sequence
	// number as a decimal STRING — which is why heightFrom accepts one.
	Sui = Chain{
		Name:              "sui",
		CheckName:         "sui_getLatestCheckpointSequenceNumber",
		Probe:             domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"sui_getLatestCheckpointSequenceNumber","params":[]}`), domain.RPCTypeJSONRPC, "sui_getLatestCheckpointSequenceNumber"),
		HeightPath:        "result",
		RequestMethodPath: "method",
		HeightMethod:      "sui_getLatestCheckpointSequenceNumber",
	}

	// The Ethereum beacon chain is REST, and its head is a slot rather than a
	// block number. Slots advance on a fixed schedule whether or not a block
	// is produced, which makes them a better staleness signal than block
	// numbers, not a worse one.
	//
	// No RequestMethodPath: a REST request names its method in the path, not
	// the body, so a client asking for the head is not recognisable here and
	// heights come from the probe alone.
	EthBeacon = Chain{
		Name:       "eth-beacon",
		CheckName:  "beacon_head_header",
		Probe:      domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/eth/v1/beacon/headers/head", "GET"),
		HeightPath: "data.header.message.slot",
	}

	// Radix reports a state version rather than a height. It counts up and it
	// is what "how far behind is this node" means there, which is all the
	// filter needs.
	Radix = Chain{
		Name:       "radix",
		CheckName:  "radix_gateway_status",
		Probe:      domain.NewPayload([]byte(`{}`), domain.RPCTypeREST, "").WithHTTP("/status/gateway-status", "POST"),
		HeightPath: "ledger_state.state_version",
	}
)

// byServiceType maps a configured service type to its chain declaration.
var byServiceType = map[domain.ServiceType]Chain{
	domain.ServiceTypeNEAR:      NEAR,
	domain.ServiceTypeSui:       Sui,
	domain.ServiceTypeEthBeacon: EthBeacon,
	domain.ServiceTypeRadix:     Radix,
}

// ByServiceType returns the chain a service type declares, if any.
//
// domain.KnownServiceTypes is the list config reporting reads, and a type in
// one and not the other is a service that either claims QoS it does not get or
// gets QoS nobody is told about — so TestEveryDeclaredChainIsAKnownType pins
// the two together.
func ByServiceType(serviceType domain.ServiceType) (Chain, bool) {
	chain, ok := byServiceType[serviceType]
	return chain, ok
}

// Declared returns every chain this package serves.
func Declared() map[domain.ServiceType]Chain { return byServiceType }
