package domain

// ServiceID uniquely identifies a service (e.g., "eth", "poly", "solana").
type ServiceID string

// ServiceType categorizes the blockchain protocol.
type ServiceType string

// The blockchain protocol families SAGE has QoS plugins for. ServiceTypeGeneric
// is the fallback for a service with no chain-specific handling — it relays and
// scores, but understands nothing about the payload.
const (
	ServiceTypeEVM    ServiceType = "evm"
	ServiceTypeCosmos ServiceType = "cosmos"
	ServiceTypeSolana ServiceType = "solana"
	ServiceTypeTron   ServiceType = "tron"
	// The chains served by qos/jsonheight, which needs only a probe and the
	// path its height sits at.
	ServiceTypeNEAR      ServiceType = "near"
	ServiceTypeSui       ServiceType = "sui"
	ServiceTypeEthBeacon ServiceType = "eth-beacon"
	// ServiceTypeRadix is declared but NOT served: its probe is unverified and
	// deliberately unwired (see qos/jsonheight). A service naming it falls to
	// the passthrough and is reported as such at startup.
	ServiceTypeRadix   ServiceType = "radix"
	ServiceTypeGeneric ServiceType = "generic"
)
