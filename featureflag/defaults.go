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
	FlagHeuristic           = "heuristic"
	FlagObservationPipeline = "observation_pipeline"
	FlagHealthChecks        = "health_checks"
	FlagTracing             = "tracing"
	FlagSupplierAffinity    = "supplier_affinity"
	FlagDebugLog            = "debug_log"
	FlagShadowMode          = "shadow_mode"
	FlagWebsocketRelays     = "websocket_relays"
	// FlagWebsocketProbes gates WebSocket recovery probes: once a minute
	// each pod dials the WebSocket endpoints of a service whose reputation
	// is below full, sends one signed request and grades the answer. A
	// WebSocket key only earns score from connections, and selection gives
	// connections only to the top tier, so without probes a demoted key has
	// no way back. A probe success moves only a key traffic is not already
	// grading. Off: no probes, and a demoted WebSocket key stays demoted.
	FlagWebsocketProbes = "websocket_probes"
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
	// FlagPenalize408 lets a supplier's HTTP 408 cost it a major error. On by
	// default since 2026-09-15; the same change was reverted on 2026-09-02
	// for concentrating traffic, so this is its live undo, globally or per
	// service. Off, a 408 is still retried, just not scored.
	FlagPenalize408 = "penalize_408"
	// FlagCircuitBreakUpstream lets a supplier's relay-miner timeout (HTTP
	// 408) or 5xx count toward the circuit breaker's failure-rate gate, so a
	// host failing a fifth of its relays within 30s is taken out of selection
	// for a while instead of being sent traffic through the whole outage.
	// Without it the gate only sees connect failures and malformed answers,
	// and an operator whose relay miners answer 408/5xx in bursts keeps its
	// share until scoring demotes each key one by one. The gate is per
	// hostname, so an operator's hosts trip one by one; an operator-wide trip
	// is the upgrade if that proves slow. Off by default: a host that answers
	// 408 to most relays all the time is removed at every TTL expiry,
	// escalating to 30 minutes, which is a stronger policy than scoring.
	FlagCircuitBreakUpstream = "circuit_break_upstream"
	// FlagMethodBlock408 lets a supplier's HTTP 408 keep that method away
	// from that host for a while (method_blocks' client TTL), while the host
	// keeps receiving every other method. A relay miner passes a backend's
	// 408 through inside a signed, claimable relay, and on mainnet
	// (2026-09-28) one owner answered an instant 408 to eth_getLogs,
	// eth_getBlockReceipts and eth_estimateGas every time while serving
	// cheap calls: each refusal was paid, and so was the retry that answered
	// it. The marks never escalate to a host-wide block — a host refusing
	// some methods is not a dead host. Off by default.
	FlagMethodBlock408 = "method_block_408"
	// FlagCosmosEVMHeight lets a Cosmos service learn block heights from its
	// EVM face: an eth_blockNumber probe on its json_rpc stakes, and the
	// eth_blockNumber answers in its traffic. For chains whose EVM block
	// number IS the Cosmos height (sei, Ethermint chains such as kava) and
	// only for those — elsewhere the two numbers would poison one consensus.
	// On mainnet sei (until 2026-09-29) the only height probe was CometBFT
	// /status, which the json_rpc stakes cannot answer, and the few comet_bft
	// stakes supplied none, so the service had no height for at least a week
	// and its height filter passed every host. Off by default; enable
	// per service.
	FlagCosmosEVMHeight = "cosmos_evm_height"
	// FlagWSShareCap keeps any one party (the owner when its domain is
	// dedicated, else the operator) under half of a service's WebSocket
	// traffic on a pod, measured in supplier frames a second rather than
	// connections: on mainnet (2026-09-29) one owner carried two thirds of
	// base's WebSocket traffic on about two connections, which a
	// connection-count spread never sees. Applied where a connection picks
	// its supplier — at open and at every rebind, which includes the one each
	// session end takes — and only while at least two parties the service's
	// reputation vouches for are in the session; otherwise placement is as
	// before. Off by default.
	FlagWSShareCap = "ws_share_cap"
	// FlagStaleResponse grades an answer that names a chain head too far
	// behind the perceived one (eth_blockNumber, Solana getBlockHeight, …;
	// see sage_stale_answers_total) as stale_response: a major supplier
	// penalty, so it feeds the failure rate, and a retry on another party. A
	// response cache only looks fast: on mainnet (2026-09-30) one owner's
	// cache answered 69-79% of its head calls on base, robinhood, tron and
	// bsc stale while its reputation sat above the in-sync operators'. When
	// every retry is stale too, the freshest answer is delivered rather than
	// an error. Needs the heuristic flag. Off by default.
	FlagStaleResponse = "stale_response"
	// FlagStaleShare charges a party (the owner when its domain is dedicated,
	// else the operator) for the share of its chain-head answers that are
	// stale, on every one of its reputation keys in the service: 0 within 15
	// points of the service's cleanest party, linear to -40 at 45 points. The
	// share is measured whatever the flag says (sage_party_stale_share). On
	// mainnet (2026-09-30) one owner's cache answered 70-80% of its head calls
	// stale on tron, bera and arb-one while head calls were 1-2% of its
	// traffic there, so stale_response's per-answer hits, spread over dozens
	// of keys, healed between hits and never moved its share. Off by default.
	FlagStaleShare = "stale_share"
	// FlagStateCanary adds the state canary to an EVM service's health
	// checks: an eth_call to Multicall3 at "latest", byte-identical while in
	// use, whose answer names its block's timestamp (qos/evm/canary.go). A
	// timestamp more than two block times and ten seconds behind the clock is
	// a stale answer, counted toward the party's stale share like a stale head
	// answer. It finds a response cache keyed on the request body serving
	// state calls old: on mainnet (2026-10-01) one owner's cache answered a
	// fixed eth_call with the same block for over two minutes on bsc while a
	// fresh request was current. One relay per backend per cycle. Off by
	// default; not for TRON, whose JSON-RPC face has no Multicall3.
	FlagStateCanary = "state_canary"
	// FlagRelativeChronic measures a key's chronic-failure penalty from the
	// best failure rate in its (service, RPC type) pool rather than from zero,
	// so a timeout tail every operator shares does not floor all of them. On
	// by default since 2026-09-15; the live undo, globally or per service.
	FlagRelativeChronic = "relative_chronic"
	// FlagOperatorChronic charges a key its OPERATOR's chronic failure rate
	// rather than its own. The operator rate is a decayed count of attempts
	// and failures kept against (service, operator, RPC type) and persisted,
	// so it survives the session draw that replaces an operator's endpoints
	// every 20 blocks. Off restores the per-key rate.
	//
	// Off by default: it changes what every score in a pool is charged, so it
	// wants a read of its own before it decides traffic. See
	// reputation/opstats.go for the mainnet measurement it comes from.
	FlagOperatorChronic = "operator_chronic"
	// FlagQuorum gates the quorum middleware: a request carrying
	// Target-Quorum-Count or Target-Quorum-Mode is sent to several operators
	// at once and answered by majority or as every answer side by side. Off
	// by default because each such request costs several paid relays; turn
	// it on for a service only once the edge strips the headers from clients
	// that may not spend that.
	FlagQuorum = "quorum"
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
	FlagHeuristic:           true,
	FlagObservationPipeline: true,
	FlagHealthChecks:        true,
	FlagTracing:             false,
	FlagSupplierAffinity:    true,
	FlagDebugLog:            false,
	FlagShadowMode:          false,
	FlagWebsocketRelays:     true,
	FlagWebsocketProbes:     true,

	FlagOperatorAwareSelection: true,
	FlagLatencyTieBreak:        true,
	FlagMethodBlocks:           true,
	FlagRequestSampler:         true,
	FlagScoringV2:              true,

	FlagTrafficInformedProbing: false,
	FlagPeerProbeSkip:          true,
	FlagAutoDrain:              false,
	FlagAutoDrainShadow:        true,
	FlagPenalize408:            true,
	FlagCircuitBreakUpstream:   false,
	FlagMethodBlock408:         false,
	FlagCosmosEVMHeight:        false,
	FlagWSShareCap:             false,
	FlagStaleResponse:          false,
	FlagStaleShare:             false,
	FlagStateCanary:            false,
	FlagRelativeChronic:        true,
	FlagOperatorChronic:        false,
	FlagQuorum:                 false,
}

// IsKnownFlag reports whether name is a flag SAGE implements. Used to warn on a
// misspelled or PATH-only flag name at startup rather than accepting it silently
// (config carries flags as an open map, so nothing else would catch a typo).
func IsKnownFlag(name string) bool {
	_, ok := DefaultFlags[name]
	return ok
}
