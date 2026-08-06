package featureflag

// Flag names. Every known flag is declared here and referenced by these
// constants everywhere else — middleware passes Flag* to IsEnabled rather than a
// string literal, so a wrong or unknown name fails to compile instead of quietly
// resolving to false.
const (
	FlagRetry               = "retry"
	FlagHedge               = "hedge"
	FlagCircuitBreaker      = "circuit_breaker"
	FlagSingleflight        = "singleflight"
	FlagCache               = "cache"
	FlagCrossValidation     = "cross_validation"
	FlagHeuristic           = "heuristic"
	FlagObservationPipeline = "observation_pipeline"
	FlagHealthChecks        = "health_checks"
	FlagTracing             = "tracing"
	FlagSupplierAffinity    = "supplier_affinity"
	FlagDebugLog            = "debug_log"
	FlagShadowMode          = "shadow_mode"
	FlagWebsocketRelays     = "websocket_relays"
	// FlagOperatorAwareSelection gates every place endpoint selection reasons
	// about operator identity (eTLD+1) rather than individual endpoints: the
	// per-operator concentration cap, and the retry/hedge preference for
	// reaching a different operator than the attempt that just failed. Off
	// restores per-endpoint-only behavior.
	FlagOperatorAwareSelection = "operator_aware_selection"
)

// DefaultFlags is the set of known flags and their default state. It is the ONE
// place a flag is defined: config carries only the overrides an operator set (a
// partial map), and both stores fall back here for every flag left unset.
//
// Adding a flag is therefore a one-line edit here (plus its Flag* constant
// above) and referencing that constant from the middleware — no config struct to
// grow, no map converter to keep in sync. A flag name absent from this map
// resolves to false, so this is also the map that decides whether a flag exists
// at all.
var DefaultFlags = map[string]bool{
	FlagRetry:               true,
	FlagHedge:               true,
	FlagCircuitBreaker:      true,
	FlagSingleflight:        true,
	FlagCache:               true,
	FlagCrossValidation:     true,
	FlagHeuristic:           true,
	FlagObservationPipeline: true,
	FlagHealthChecks:        true,
	FlagTracing:             false,
	FlagSupplierAffinity:    true,
	FlagDebugLog:            false,
	FlagShadowMode:          false,
	FlagWebsocketRelays:     true,

	FlagOperatorAwareSelection: true,
}

// IsKnownFlag reports whether name is a flag SAGE implements. Used to warn on a
// misspelled or PATH-only flag name at startup rather than accepting it silently
// (config carries flags as an open map, so nothing else would catch a typo).
func IsKnownFlag(name string) bool {
	_, ok := DefaultFlags[name]
	return ok
}
