package domain

// KnownServiceTypes are the service types that select a QoS plugin with
// chain-specific behaviour. Anything else is served by the passthrough.
//
// Listed here rather than in the wiring so config validation and plugin
// selection cannot disagree about what is recognised: a type absent from this
// list gets the passthrough, and the operator is told, which is the whole
// point of naming the set in one place.
func KnownServiceTypes() []ServiceType {
	return []ServiceType{
		ServiceTypeEVM, ServiceTypeCosmos, ServiceTypeSolana, ServiceTypeTron,
		ServiceTypeNEAR, ServiceTypeSui, ServiceTypeEthBeacon,
	}
}

// IsKnownServiceType reports whether a configured type selects a
// chain-specific QoS plugin.
func IsKnownServiceType(t ServiceType) bool {
	for _, known := range KnownServiceTypes() {
		if t == known {
			return true
		}
	}
	return false
}
