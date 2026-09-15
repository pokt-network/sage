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
	// FlagLatencyTieBreak gates the latency tie-break inside the winning
	// score tier: with it on, a faster host is picked more often than a
	// slower one of the same tier, weighted by the per-key traffic latency
	// EWMA; off restores a uniform pick within the tier. Latency never
	// moves a score either way (docs/scoring.md §7.2).
	FlagLatencyTieBreak = "latency_tiebreak"
	// FlagMethodBlocks gates the method_blocks middleware: per-host,
	// per-method memory that stops sending a method to a host that recently
	// timed out on it or said it does not serve it, without affecting any
	// other method on that host. Off passes every relay through unpruned.
	FlagMethodBlocks = "method_blocks"
	// FlagRequestSampler gates the Observe middleware's call into the
	// traffic.Sampler: recording each relay's payloads for request-shape
	// diversity (see package traffic). Off means every relay skips the
	// sampler entirely — the admin request-sample routes and gauges then
	// report nothing for that service, not zeros.
	FlagRequestSampler = "request_sampler"
	// FlagScoringV2 gates per-attempt reputation scoring: the score middleware
	// records one signal per attempt (batch collapses to one per endpoint) and
	// Observe records nothing. Off restores the pre-v2 path where Observe
	// records once per client request. Observe, score and batch each read the
	// flag per request, so a flip while a request is in flight can count that
	// one request on both paths or neither — an admin action, not a traffic
	// pattern. See docs/scoring.md.
	FlagScoringV2 = "scoring_v2"
	// FlagTrafficInformedProbing gates skipping a health check against a
	// backend that client traffic has already graded this cycle. Every client
	// attempt records a reputation signal, so a busy backend is graded
	// continuously and its probe buys a second copy of the same fact — on the
	// mainnet canary at 1% traffic, 48.66% of probes in a ten-minute window
	// went to backends traffic had graded in that same window. Off by default:
	// the saving and the risk both scale with traffic share, and a probe is
	// the only observation source that bypasses sampling, so this needs
	// measuring at whatever share it runs at rather than assuming the canary's
	// numbers hold. Only ever skips once the pod is warm.
	FlagTrafficInformedProbing = "traffic_informed_probing"
	// FlagPeerProbeSkip gates skipping a health check that another SAGE
	// instance ran recently (active_health_checks.peer_probe_stream). On by
	// default, because the config key is the decision to read a peer at all;
	// this is the live off switch, globally or for one service. Off, this
	// instance probes that service itself again from the next cycle while it
	// keeps applying the peer's results.
	FlagPeerProbeSkip = "peer_probe_skip"
	// FlagAutoDrain lets the auto-drain engine (package autodrain) set drains.
	// Off by default: until it is on for a service the engine only evaluates.
	// docs/auto-drain.md.
	FlagAutoDrain = "auto_drain"
	// FlagAutoDrainShadow keeps the engine evaluating, recording and counting
	// its decisions without setting a drain, even where auto_drain is on. On by
	// default, so every instance collects shadow decisions from day one.
	FlagAutoDrainShadow = "auto_drain_shadow"
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
	FlagLatencyTieBreak:        true,
	FlagMethodBlocks:           true,
	FlagRequestSampler:         true,
	FlagScoringV2:              true,

	FlagTrafficInformedProbing: false,
	FlagPeerProbeSkip:          true,
	FlagAutoDrain:              false,
	FlagAutoDrainShadow:        true,
}

// IsKnownFlag reports whether name is a flag SAGE implements. Used to warn on a
// misspelled or PATH-only flag name at startup rather than accepting it silently
// (config carries flags as an open map, so nothing else would catch a typo).
func IsKnownFlag(name string) bool {
	_, ok := DefaultFlags[name]
	return ok
}
