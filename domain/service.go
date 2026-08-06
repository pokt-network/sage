package domain

// ServiceID uniquely identifies a service (e.g., "eth", "poly", "solana").
type ServiceID string

// ServiceType categorizes the blockchain protocol.
type ServiceType string

// The blockchain protocol families SAGE has QoS plugins for. ServiceTypeGeneric
// is the fallback for a service with no chain-specific handling — it relays and
// scores, but understands nothing about the payload.
const (
	ServiceTypeEVM     ServiceType = "evm"
	ServiceTypeCosmos  ServiceType = "cosmos"
	ServiceTypeSolana  ServiceType = "solana"
	ServiceTypeGeneric ServiceType = "generic"
)
