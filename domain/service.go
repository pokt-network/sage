package domain

// ServiceID uniquely identifies a service (e.g., "eth", "poly", "solana").
type ServiceID string

// ServiceType categorizes the blockchain protocol.
type ServiceType string

const (
	ServiceTypeEVM     ServiceType = "evm"
	ServiceTypeCosmos  ServiceType = "cosmos"
	ServiceTypeSolana  ServiceType = "solana"
	ServiceTypeGeneric ServiceType = "generic"
)
