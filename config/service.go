package config

import (
	"time"

	"github.com/pokt-network/sage/reputation"
)

// GatewayConfig holds the gateway-specific configuration.
// Supports both PATH config formats:
//   - gateway_config.services (production)
//   - gateway_config.unified_services.services (newer format)
type GatewayConfig struct {
	// GatewayMode names PATH's delegation mode ("centralized", "delegated",
	// "permissionless"). Required when the Shannon backend is in use — startup
	// fails without it — but the *value* is not acted on: SAGE derives how to
	// sign from the keys it was given rather than from a declared mode.
	GatewayMode string `yaml:"gateway_mode"`
	// GatewayAddress is the gateway's own bech32 Shannon address, sent with
	// each relay so the supplier can verify the delegation.
	GatewayAddress string `yaml:"gateway_address"`
	// GatewayPrivateKeyHex is the hex-encoded key the gateway signs relays
	// with. **A secret** — it is why pprof defaults to off, since a heap dump
	// contains it.
	GatewayPrivateKeyHex string `yaml:"gateway_private_key_hex"`
	// OwnedAppsPrivateKeys are hex-encoded application keys the gateway relays
	// on behalf of. Each staked application funds the relays sent under it.
	// **Secrets**, same as above.
	OwnedAppsPrivateKeys []string `yaml:"owned_apps_private_keys_hex"`

	Reputation ReputationConfig `yaml:"reputation_config"`

	// Services supports the production config format (gateway_config.services[])
	Services []ServiceConfig `yaml:"services"`
	// UnifiedServices supports the newer format (gateway_config.unified_services{})
	UnifiedServices UnifiedServicesConfig `yaml:"unified_services"`

	// Retry at gateway level (gateway_config.retry_config in production config)
	Retry RetryConfig `yaml:"retry_config"`

	// MiddlewareChain names the middlewares to run, outermost first. Empty takes
	// relay.DefaultChainOrder. Names are the canonical ones in relay/chain_order.go;
	// an unknown name is a startup error, and the ordering invariants are enforced
	// either way, so this cannot be used to break rotation or response analysis.
	//
	// The chain is global rather than per-service — per-service control is what
	// feature flags are for, and they can be toggled at runtime without a
	// redeploy. This is the coarser, structural switch: which middlewares exist
	// in the chain at all.
	MiddlewareChain []string `yaml:"middleware_chain"`

	// BlockedDomains permanently excludes every endpoint at a domain from the
	// listed RPC types, on every service. See BlockedDomain.
	BlockedDomains []BlockedDomain `yaml:"blocked_domains"`

	// MethodBlocks tunes the per-host, per-method memory consulted at
	// selection: a host that timed out on a method, or said it does not
	// serve it, stops receiving that method for a while and keeps receiving
	// everything else. See MethodBlocksConfig.
	MethodBlocks MethodBlocksConfig `yaml:"method_blocks"`

	HealthChecks        HealthCheckConfig         `yaml:"active_health_checks"`
	ObservationPipeline ObservationPipelineConfig `yaml:"observation_pipeline"`

	LatencyProfiles map[string]LatencyProfile `yaml:"latency_profiles"`
	Defaults        ServiceDefaults           `yaml:"defaults"`

	// EndpointPolicy constrains which supplier URLs may be selected, on every
	// service. Same key as PATH's gateway-level endpoint_policy.
	EndpointPolicy EndpointPolicy `yaml:"endpoint_policy"`
}

// EndpointPolicy constrains which supplier URLs may be selected. Both checks
// are opt-in (the zero value is "no policy"), applied wherever endpoints are
// handed out — selection, retry, hedge, WebSocket bind and health checks.
type EndpointPolicy struct {
	// RequireHTTPS drops any endpoint whose URL is not https:// or wss:// — a
	// plaintext http/ws supplier relays keys and payloads in the clear.
	RequireHTTPS bool `yaml:"require_https"`
	// RequireDomain drops any endpoint whose host is a raw IP literal rather
	// than a domain name.
	RequireDomain bool `yaml:"require_domain"`
}

// BlockedDomain is one entry of the operator domain blocklist.
//
// Every endpoint whose URL is at Domain is excluded from the listed RPC types
// on every service, everywhere endpoints are handed out: relay selection and
// therefore retry, hedge and batch; WebSocket bind; and health checks. It is
// matched on the endpoint's URL, never on a supplier address, so the ban
// survives session rollover by construction — a supplier rotated into a new
// session at a blocked domain is banned the moment it appears, without anyone
// re-applying anything.
//
// This is the blunt instrument, and deliberately unlike the two things next to
// it. The supplier blacklist is earned: a supplier fails validation and is
// dropped for a while, per service. Circuit breaking is earned too, and
// expires. This is neither — it is a gateway operator saying "not this
// infrastructure, not ever", and it does not yield even when honouring it
// would empty the pool. An empty pool is a legible outage; quietly routing to
// infrastructure an operator banned is not.
type BlockedDomain struct {
	// Domain is a registrable domain ("op-alpha.example", matching every host
	// under it) or an exact hostname ("s019.op-alpha.example", matching only
	// that host). Case-insensitive. An empty value is a startup error rather
	// than a no-op.
	Domain string `yaml:"domain"`

	// RPCTypes lists the banned protocols ("json_rpc", "rest", "comet_bft",
	// "websocket", "grpc"). Empty bans every one of them. An unrecognized value
	// is a startup error: a typo here silently narrows a ban, which is the one
	// failure mode this feature cannot have.
	RPCTypes []string `yaml:"rpc_types,omitempty"`
}

// MethodBlocksConfig tunes method-aware blocks. Both fields follow the
// zero-is-default, negative-is-off convention.
type MethodBlocksConfig struct {
	// TTL is how long one mark keeps a method away from a host. Zero means
	// 5m; negative disables marking entirely (the middleware still runs and
	// passes everything through). Short on purpose — a mark is one timeout
	// of evidence and a host re-proves itself with one relay when it lapses.
	TTL time.Duration `yaml:"ttl"`
	// EscalationThreshold is how many distinct methods must be marked on one
	// host inside one TTL before the host is blocked for every method. Zero
	// means 3; negative never escalates.
	EscalationThreshold int `yaml:"escalation_threshold"`
}

// EffectiveTTL resolves TTL: zero to the default, negative to off.
func (m MethodBlocksConfig) EffectiveTTL() time.Duration {
	switch {
	case m.TTL < 0:
		return 0
	case m.TTL == 0:
		return 5 * time.Minute
	}
	return m.TTL
}

// EffectiveEscalation resolves EscalationThreshold: zero to the default,
// negative to never.
func (m MethodBlocksConfig) EffectiveEscalation() int {
	switch {
	case m.EscalationThreshold < 0:
		return 0
	case m.EscalationThreshold == 0:
		return 3
	}
	return m.EscalationThreshold
}

// AllServices returns services from whichever config format was used.
func (g *GatewayConfig) AllServices() []ServiceConfig {
	if len(g.Services) > 0 {
		return g.Services
	}
	return g.UnifiedServices.Services
}

// GetServiceConfig returns the config for a specific service ID, or nil.
func (g *GatewayConfig) GetServiceConfig(id string) *ServiceConfig {
	for i := range g.Services {
		if g.Services[i].ID == id {
			return &g.Services[i]
		}
	}
	for i := range g.UnifiedServices.Services {
		if g.UnifiedServices.Services[i].ID == id {
			return &g.UnifiedServices.Services[i]
		}
	}
	return nil
}

// EffectiveDefaults returns the defaults a service resolves against, merged
// field by field from the three places a PATH config can carry them:
// gateway_config.defaults, gateway_config.retry_config (the production
// layout), and gateway_config.unified_services.defaults — in that order of
// precedence, each field taken from the first source that sets it. The order
// is the one the block-level rule already had (defaults beat the gateway
// block when both named a count); only the granularity changes.
//
// Field by field, not block by block. This used to pick one whole block on
// the strength of its max_retries, so a production config carrying
// `retry_config: {hedge_delay: 100ms}` and nothing else lost the hedge delay
// entirely, and one setting max_retries in both places silently discarded the
// gateway block's other fields. Neither was reported anywhere.
func (g *GatewayConfig) EffectiveDefaults() ServiceDefaults {
	return ServiceDefaults{
		Retry:   mergeRetry(g.Defaults.Retry, mergeRetry(g.Retry, g.UnifiedServices.Defaults.Retry)),
		Timeout: mergeTimeout(g.Defaults.Timeout, g.UnifiedServices.Defaults.Timeout),
		// The inert reputation block is not merged: nothing reads it either way.
		Reputation: g.Defaults.Reputation,
	}
}

// mergeRetry overlays a on b: every field a sets wins, every field it leaves
// zero comes from b. A block that says `enabled: false` disables retries for
// whatever it governs, and that decision is not undone by a lower layer.
func mergeRetry(a, b RetryConfig) RetryConfig {
	out := a
	if out.MaxRetries == 0 {
		out.MaxRetries = b.MaxRetries
	}
	if out.HedgeDelay == 0 {
		out.HedgeDelay = b.HedgeDelay
	}
	if out.MaxLatency == 0 {
		out.MaxLatency = b.MaxLatency
	}
	if out.disabled {
		out.MaxRetries = 0
	} else if b.disabled && a.MaxRetries == 0 {
		// a did not speak to the count and b turned retries off: off it is.
		out.disabled = true
		out.MaxRetries = 0
	}
	return out
}

// mergeTimeout overlays a on b the same way.
func mergeTimeout(a, b TimeoutConfig) TimeoutConfig {
	if a.RelayTimeout == 0 {
		a.RelayTimeout = b.RelayTimeout
	}
	return a
}

// EffectiveMiddlewareChain returns the configured middleware order from
// whichever config format was used, or nil to mean "take the default".
//
// Gateway level wins over unified_services, mirroring EffectiveDefaults. Both
// are read because unified_services.middleware_chain is where the field was
// first declared, and a config using it must keep working.
func (g *GatewayConfig) EffectiveMiddlewareChain() []string {
	if len(g.MiddlewareChain) > 0 {
		return g.MiddlewareChain
	}
	return g.UnifiedServices.MiddlewareChain
}

// UnifiedServicesConfig holds service definitions and defaults.
type UnifiedServicesConfig struct {
	Defaults        ServiceDefaults           `yaml:"defaults"`
	Services        []ServiceConfig           `yaml:"services"`
	LatencyProfiles map[string]LatencyProfile `yaml:"latency_profiles"`
	// MiddlewareChain is the older location for the chain order. The
	// gateway-level gateway_config.middleware_chain wins when both are set;
	// this is read so a config written against the earlier layout keeps
	// working. See GatewayConfig.MiddlewareChain for what the values mean.
	MiddlewareChain []string `yaml:"middleware_chain"`
}

// ServiceConfig defines a single service.
type ServiceConfig struct {
	// ID is the service identifier clients name in the Target-Service-Id
	// header, and the key everything per-service is looked up by.
	ID string `yaml:"id"`
	// Type selects the QoS plugin: "evm", "cosmos", "solana", "tron", or
	// anything else for the passthrough plugin, which relays and scores but
	// understands nothing about the payload.
	//
	// A service on the passthrough gets no health checks, no block-height
	// tracking and no chain view, and its selection is reputation alone — so
	// an unrecognised value here is a serious typo. Startup reports which
	// services are on the passthrough and which named a type nothing
	// implements; see config.QoSCoverageFor.
	Type string `yaml:"type"`
	// RPCTypes lists the protocols this service serves ("json_rpc", "rest",
	// "comet_bft", "websocket", "grpc"). A client request of any other type
	// is refused with 400 before a session is looked up, and a WebSocket
	// upgrade is refused unless "websocket" is listed. Also read by RPC-type
	// detection (a REST-capable service classifies unknown paths as REST) and
	// by the Cosmos plugin, which fronts several protocols on one service.
	RPCTypes []string `yaml:"rpc_types"`
	// SyncAllowance is how many blocks behind the perceived chain head an
	// endpoint may fall and still be selected. Too tight and a healthy pool
	// empties on every block; too loose and clients read stale state.
	//
	// Zero means the plugin's own default, and the plugins do not agree on what
	// that is, because their chains do not. EVM and Cosmos read zero as "no
	// block-height filtering" — a block there is seconds to tens of seconds, so
	// an unset allowance costs a bounded amount of staleness. Solana reads it as
	// 1500 blocks (~10 minutes), because zero there means a strict
	// height >= perceived comparison rather than no comparison, and at ~400ms
	// per block that starves every endpoint except the one that reported last.
	// See qos/solana.defaultSyncAllowance.
	SyncAllowance uint64 `yaml:"sync_allowance"`
	// LatencyProfile is parsed and not implemented. It names an entry in
	// gateway_config.latency_profiles, which is itself not wired.
	LatencyProfile string `yaml:"latency_profile"`

	// ChainID is the chain identifier this service is expected to serve, as the
	// chain itself reports it. When set, health checks assert the endpoint
	// agrees; one serving a different chain is ejected rather than left to
	// answer with another chain's data under this service's name.
	//
	// The value is opaque here on purpose. Its format and how it compares are
	// chain semantics, so they belong to the QoS plugin, not to config: EVM
	// reports hex from eth_chainId ("0x1") and must compare numerically, since
	// "0x531" and "0x0531" are the same chain; CometBFT reports a name from
	// /status ("cosmoshub-4") that compares exactly. Validation therefore lives
	// in the plugin's own Config.Validate, called at wire time — still a
	// startup failure, without teaching config about any one chain.
	//
	// Empty disables the assertion — the zero value keeps existing services
	// behaving exactly as before, so this is opt-in per service.
	ChainID string `yaml:"chain_id"`

	Timeout TimeoutConfig `yaml:"timeout_config"`
	Retry   RetryConfig   `yaml:"retry_config"`

	ExternalBlockSources []ExternalBlockSource `yaml:"external_block_sources"`

	// BlockedSuppliers lists supplier operator addresses ("pokt1...") that must
	// never be selected for this service, whatever their reputation. Unlike the
	// earned supplier blacklist (a validation failure, temporary) this is an
	// operator's standing decision — "not this supplier, not ever" — matched on
	// the supplier address so it survives session rollover. Same key and
	// semantics as PATH's per-service blocked_suppliers.
	BlockedSuppliers []string `yaml:"blocked_suppliers"`

	// RPCTypeFallbacks maps a requested RPC type onto the one to relay through
	// when NO supplier in the session staked the requested one, e.g.
	// `comet_bft: json_rpc`. It is a pool-level switch, as in PATH: with even
	// one supplier staking the requested type the mapping is not consulted
	// for selection. The request is sent unchanged to the fallback type's
	// URL; nothing is translated. It exists because relay miners commonly
	// serve CometBFT's HTTP and JSON-RPC surfaces from one port, so a pool of
	// json_rpc-only suppliers can still answer a comet_bft `/status`.
	//
	// The one per-endpoint use is the cosmos health check, which probes
	// json_rpc-staked suppliers with that same GET /status through this
	// mapping; the earlier per-supplier selection semantics added REST-only
	// suppliers to tron's json_rpc pool and answered 405 from their REST root.
	//
	// Same key as PATH's, so a PATH config carries over. One hop only: a
	// fallback's own fallback is not consulted. Both sides must name a known
	// RPC type and differ; that is validated at load.
	RPCTypeFallbacks map[string]string `yaml:"rpc_type_fallbacks"`
}

// ServiceDefaults provides default values for services.
type ServiceDefaults struct {
	// Retry is the default retry policy, folded into EffectiveDefaults.
	Retry RetryConfig `yaml:"retry_config"`
	// Timeout is the default timeout policy, folded into EffectiveDefaults.
	Timeout TimeoutConfig `yaml:"timeout_config"`
	// Reputation is parsed and not implemented. Reputation is configured from
	// gateway_config.reputation_config only; the selector and the scorer are
	// global, so a copy under defaults is read by nothing. The whole block is
	// reported at startup rather than each key inside it.
	Reputation ReputationConfig `yaml:"reputation_config"`
}

// TimeoutConfig controls relay timeouts.
type TimeoutConfig struct {
	// RelayTimeout bounds a single relay attempt to one endpoint — not the
	// whole client request, which may span several attempts under retry and
	// hedge.
	RelayTimeout time.Duration `yaml:"relay_timeout"`
}

// RetryConfig controls retry behavior. Zero values = disabled.
type RetryConfig struct {
	// Enabled turns retries off when written as `enabled: false`; retries are
	// otherwise on whenever MaxRetries > 0, which is PATH's rule too (its
	// default for an absent key is true). The key's presence is what matters,
	// and a bool cannot tell "absent" from "false", so the loader reads an
	// explicit false from the YAML itself. The startup report says when it
	// did. An `enabled: false` under max_retries: 3 used to retry three times
	// and say nothing.
	Enabled bool `yaml:"enabled"`
	// disabled is set by the loader when the YAML carried `enabled: false`.
	// It survives EffectiveRetry, so a service that turned retries off does
	// not fall back to the defaults' count.
	disabled bool
	// MaxRetries is how many further endpoints a failed relay may be tried on.
	// Zero disables retry. Each attempt re-selects, so a retry rotates away
	// from the endpoint that failed rather than asking it again.
	//
	// Retrying never escalates to a circuit break on its own; those are
	// independent decisions. See the heuristic package.
	MaxRetries int `yaml:"max_retries"`
	// MaxRetryLatency is parsed and not implemented. Bound total time with
	// timeout_config.relay_timeout per attempt instead.
	MaxRetryLatency time.Duration `yaml:"max_retry_latency"`
	// RetryOn5xx is parsed and not implemented. See RetryOnTimeout.
	RetryOn5xx bool `yaml:"retry_on_5xx"`
	// RetryOnTimeout is parsed and not implemented. Whether a failure is
	// retryable is decided by heuristic analysis of the response, not by
	// per-cause switches.
	RetryOnTimeout bool `yaml:"retry_on_timeout"`
	// RetryOnConnection is parsed and not implemented. See RetryOnTimeout.
	RetryOnConnection bool `yaml:"retry_on_connection"`
	// ConnectTimeout is parsed and not implemented.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// HedgeDelay is how long to wait before racing a second endpoint against
	// an in-flight relay. Zero disables hedging.
	//
	// This is the tail-latency control: set it near the service's p95 so the
	// hedge fires only for requests already running long, and costs a
	// duplicate relay only on those. Set it too low and every request is sent
	// twice.
	HedgeDelay time.Duration `yaml:"hedge_delay"`
	// MaxLatency is the total time budget across every retry attempt of one
	// request: once it is spent, the retry middleware stops rotating and
	// delivers what it has. It does not penalise latency in reputation —
	// latency has reporting power only (docs/scoring.md §7.2).
	MaxLatency time.Duration `yaml:"max_latency"`
}

// IsEnabled returns true if retries are configured and not switched off.
func (c RetryConfig) IsEnabled() bool { return c.MaxRetries > 0 && !c.disabled }

// Disabled reports whether the YAML turned retries off with `enabled: false`.
func (c RetryConfig) Disabled() bool { return c.disabled }

// disable records an explicit `enabled: false` from the YAML.
func (c *RetryConfig) disable() {
	c.disabled = true
	c.MaxRetries = 0
}

// HedgeEnabled returns true if hedge racing is configured.
func (c RetryConfig) HedgeEnabled() bool { return c.HedgeDelay > 0 }

// ExternalBlockSource defines an external RPC endpoint for ground-truth block heights.
type ExternalBlockSource struct {
	// URL is the external RPC endpoint to poll. It is trusted as ground truth,
	// so it should be a node you control, not one of the suppliers being
	// measured — the point is an opinion from outside the pool.
	URL string `yaml:"url"`
	// Type is the RPC type to poll it as ("json_rpc", "rest", "comet_bft").
	Type string `yaml:"type"`
	// Method is the JSON-RPC method to call, e.g. "eth_blockNumber".
	Method string `yaml:"method"`
	// Path is the request path, for REST and CometBFT sources.
	Path string `yaml:"path"`
	// Interval is how often to poll. Keep it well below the chain's block time
	// or the reference height is itself stale. A service with several sources
	// polls them all on one ticker, at the shortest interval any of them
	// names.
	Interval time.Duration `yaml:"interval"`
	// Timeout bounds a single poll.
	Timeout time.Duration `yaml:"timeout"`
	// GracePeriod is parsed and not implemented.
	GracePeriod time.Duration `yaml:"grace_period"`
}

// LatencyProfile defines per-chain latency thresholds for reputation scoring.
// The whole latency_profiles block is parsed and not implemented — no field
// below is read. Latency has reporting power only: it is kept as a per-key EWMA
// and shown in the admin listing, never subtracted from a score
// (docs/scoring.md §7.2). The struct exists so a PATH config carrying the block
// still loads.
type LatencyProfile struct {
	// FastThreshold is the latency below which a response counts as fast.
	FastThreshold time.Duration `yaml:"fast_threshold"`
	// NormalThreshold is the upper bound of expected latency.
	NormalThreshold time.Duration `yaml:"normal_threshold"`
	// SlowThreshold is the latency above which a response counts as slow.
	SlowThreshold time.Duration `yaml:"slow_threshold"`
	// PenaltyThreshold is the latency above which a penalty would apply.
	PenaltyThreshold time.Duration `yaml:"penalty_threshold"`
	// SevereThreshold is the latency above which a larger penalty would apply.
	SevereThreshold time.Duration `yaml:"severe_threshold"`
	// FastBonus is the score increase a fast response would earn.
	FastBonus int `yaml:"fast_bonus"`
	// SlowPenalty is the score decrease a slow response would incur.
	SlowPenalty int `yaml:"slow_penalty"`
	// VerySlowPenalty is the score decrease a very slow response would incur.
	VerySlowPenalty int `yaml:"very_slow_penalty"`
}

// ReputationConfig controls the reputation system.
type ReputationConfig struct {
	// Enabled is not read; reputation always runs. Endpoint scoring is how
	// selection works, so there is no meaningful gateway with it switched off.
	Enabled bool `yaml:"enabled"`
	// StorageType is parsed and not implemented. Storage follows redis_config:
	// Redis when an address is set, in-memory otherwise.
	StorageType string `yaml:"storage_type"`
	// KeyGranularity selects what a score is attached to: "per-url" (default),
	// "per-endpoint", "per-domain" or "per-supplier". See reputation/key.go for
	// why per-URL is the default.
	//
	// An unrecognised value is a startup error rather than a fallback to the
	// default — silently changing what scores attach to is not something an
	// operator could detect until an incident.
	KeyGranularity string `yaml:"key_granularity"`
	// InitialScore is the score a newly seen endpoint starts at. Default: 100.
	InitialScore int `yaml:"initial_score"`
	// MinThreshold is the score below which an endpoint is not selected at all
	// (the pool-collapse guard still serves the least-bad one). Global: one
	// selector serves every service. There is no per-service copy — ServiceConfig
	// has no reputation_config, so a copy placed under a service lands in
	// Config.Ignored, and a copy under gateway_config.defaults is reported as
	// inert. Zero means the default (10); a negative value is a startup error,
	// not an off switch.
	MinThreshold int `yaml:"min_threshold"`
	// RecoveryTimeout is parsed and not implemented. SAGE has no cooldown
	// mechanism — reputation is a continuous score, and recovery happens
	// through probation traffic rather than by waiting out a timer.
	RecoveryTimeout time.Duration `yaml:"recovery_timeout"`

	// MaxOperatorShare bounds the fraction of a service's endpoint selections
	// any single operator (registrable domain / eTLD+1) may receive; the excess
	// is water-filled across the other operators. Zero means the default
	// (0.50); negative disables the cap. Two-operator pools use
	// MaxOperatorShareTwoOperators instead — see reputation/concentration.go
	// for why.
	MaxOperatorShare float64 `yaml:"max_operator_share"`
	// MaxOperatorShareTwoOperators is the cap for pools holding exactly two
	// operators, where MaxOperatorShare would sit on the infeasibility
	// boundary. Zero means the default (0.65).
	MaxOperatorShareTwoOperators float64 `yaml:"max_operator_share_two_operators"`
	// OperatorDisplacementCeiling is the multiple of its own entitlement an
	// operator may be displaced up to when absorbing another's capped excess.
	// Zero means the default (3.0); negative removes the ceiling.
	OperatorDisplacementCeiling float64 `yaml:"operator_displacement_ceiling"`

	// ChronicHalfLifeAttempts is the half-life, in attempts, of the EWMA
	// failure rate behind the chronic-failure term of the score. 0 means the
	// default (20000); negative turns the term off. docs/scoring.md §7.3
	// explains the number: long enough that a 6-critical burst on a clean
	// key stays under the onset, short enough to catch a 0.2% violator within
	// tens of thousands of attempts.
	ChronicHalfLifeAttempts int `yaml:"chronic_half_life_attempts"`
	// ChronicOnsetRate is the failure rate at which the chronic penalty starts.
	// 0 means the default (0.0002, i.e. 0.02%). Must be below chronic_full_rate.
	ChronicOnsetRate float64 `yaml:"chronic_onset_rate"`
	// ChronicFullRate is the failure rate at which the chronic penalty reaches
	// -40 points (out of tier 1); it continues to -70 one decade higher. 0
	// means the default (0.01, i.e. 1%). Must be below 1.
	ChronicFullRate float64 `yaml:"chronic_full_rate"`

	// TieredSelection holds the tier thresholds and probation routing. Global:
	// one selector serves every service. There is no per-service copy —
	// ServiceConfig has no reputation_config, so a copy placed under a service
	// lands in Config.Ignored, and a copy under gateway_config.defaults is
	// reported as inert.
	TieredSelection TieredSelectionConfig `yaml:"tiered_selection"`
	// SignalImpacts holds the score delta each signal type carries.
	SignalImpacts SignalImpactsConfig `yaml:"signal_impacts"`
	// NOTE: PATH's strike_system has no field here on purpose. SAGE has no
	// strike/cooldown mechanism — reputation is a continuous score feeding
	// tiered selection, so there is no cooldown for these knobs to tune. The
	// struct existed and was read by nothing; a PATH config carrying the key is
	// now reported at startup via Config.Ignored instead of parsing into fields
	// that do not do anything.
}

// SelectorConfig is the tiered-selector configuration this block asks for,
// with reputation.DefaultSelectorConfig() filling anything unset.
//
// Zero means "unset", which is why each threshold is copied only when
// positive: a config that names two of the five must not drag the other three
// to zero, where every endpoint is tier 1 and nothing is ever filtered out.
func (r ReputationConfig) SelectorConfig() reputation.SelectorConfig {
	sel := reputation.DefaultSelectorConfig()
	if r.TieredSelection.Tier1Threshold > 0 {
		sel.Tier1Threshold = float64(r.TieredSelection.Tier1Threshold)
	}
	if r.TieredSelection.Tier2Threshold > 0 {
		sel.Tier2Threshold = float64(r.TieredSelection.Tier2Threshold)
	}
	if r.MinThreshold > 0 {
		sel.MinThreshold = float64(r.MinThreshold)
	}
	if r.TieredSelection.Probation.Threshold > 0 {
		sel.ProbationThreshold = float64(r.TieredSelection.Probation.Threshold)
	}
	if r.TieredSelection.Probation.TrafficPercent > 0 {
		sel.ProbationPct = r.TieredSelection.Probation.TrafficPercent
	}
	switch p := r.TieredSelection.Tier2TrafficPercent; {
	case p > 0:
		sel.Tier2Pct = p
	case p < 0:
		sel.Tier2Pct = 0
	}
	return sel
}

// Impacts is the signal_impacts block as the reputation package reads it. The
// three PATH signal types SAGE no longer has are not carried across; they are
// reported as inert instead.
func (r ReputationConfig) Impacts() reputation.SignalImpacts {
	return reputation.SignalImpacts{
		Success: r.SignalImpacts.Success, MinorError: r.SignalImpacts.MinorError,
		MajorError: r.SignalImpacts.MajorError, CriticalError: r.SignalImpacts.CriticalError,
		FatalError: r.SignalImpacts.FatalError,
	}
}

// RateConfig is the chronic-failure term's configuration.
func (r ReputationConfig) RateConfig() reputation.RateConfig {
	return reputation.RateConfig{
		HalfLifeAttempts: r.ChronicHalfLifeAttempts,
		OnsetRate:        r.ChronicOnsetRate,
		FullRate:         r.ChronicFullRate,
	}
}

// TieredSelectionConfig controls endpoint tiering: the score bands, and the
// traffic shares that reach below the top band.
//
// Every key here except `enabled` is honoured: they fill in
// reputation.SelectorConfig, anything left zero taking
// reputation.DefaultSelectorConfig(). They are global — one TieredSelector
// serves every service — so the block is read from
// gateway_config.reputation_config and a per-service copy of it changes
// nothing. Thresholds that do not descend (tier2 at or above tier1, probation
// at or above tier2) still load — a PATH config in production has them, and
// the selector copes by classifying probation first — but they leave a
// sentence in Config.Warnings saying which band ends up empty.
type TieredSelectionConfig struct {
	// Enabled is parsed and not implemented: tiering always runs. Selection is
	// tiered by construction, so there is no untiered mode to switch back to.
	Enabled bool `yaml:"enabled"`
	// Tier1Threshold is the minimum score for the best tier. Zero means the
	// default (80) and a negative value is a startup error. Should be above
	// tier2_threshold; a set that does not descend loads and warns rather than
	// failing.
	Tier1Threshold int `yaml:"tier1_threshold"`
	// Tier2Threshold is the minimum score for the second tier. Zero means the
	// default (50), and a negative value is a startup error. Should be above
	// the probation threshold, for the same reason.
	Tier2Threshold int `yaml:"tier2_threshold"`
	// Tier2TrafficPercent is the share of relays that try a tier-2 endpoint
	// first when tier 1 is populated, with the tier-1 pick behind it as the
	// retry fallback. It is what lets a tier-2 endpoint be measured by traffic
	// rather than by health-check probes alone (docs/scoring.md §7.7). Zero
	// means the default (5), a negative value turns the trickle off, and the
	// value must be at most 100. A SAGE key; PATH has no equivalent.
	Tier2TrafficPercent int `yaml:"tier2_traffic_percent"`
	// Probation holds the probation-routing thresholds.
	Probation ProbationConfig `yaml:"probation"`
}

// ProbationConfig controls probation routing for recovering endpoints.
//
// Honoured, and global, for the same reasons as TieredSelectionConfig.
type ProbationConfig struct {
	// Enabled is parsed and not implemented: probation routing always runs.
	// Probation traffic is how an endpoint earns its score back, so there is
	// no mode in which a demoted endpoint is never retried.
	Enabled bool `yaml:"enabled"`
	// Threshold is the score below which an endpoint counts as on probation.
	// Zero means the default (30) and a negative value is a startup error.
	// Should be below tier2_threshold and at or above min_threshold; if it is
	// not, the bands overlap, the config still loads, and Config.Warnings says
	// which one ends up empty.
	Threshold int `yaml:"threshold"`
	// TrafficPercent is the share of requests a probation endpoint is
	// prepended to, so a recovering endpoint can earn its way back. Zero means
	// the default (10); the value must be 0..100.
	TrafficPercent int `yaml:"traffic_percent"`
	// RecoveryMultiplier is parsed and not implemented. SAGE has no recovery
	// multiplier — a probation endpoint recovers through the traffic it is
	// given, at the same score deltas as everything else.
	RecoveryMultiplier float64 `yaml:"recovery_multiplier"`
}

// SignalImpactsConfig defines how each signal type affects reputation.
//
// The five surviving signal types are honoured; a zero field takes the default
// from reputation.SignalImpacts. The three PATH keys below them do nothing,
// because the signal types themselves no longer exist — see docs/scoring.md
// §7.2. Each says so on its own, so that this type doc does not stand in for
// the eight fields under it.
type SignalImpactsConfig struct {
	// Success is the score change for a successful relay. Zero means the
	// default (+5).
	Success int `yaml:"success"`
	// MinorError is the score change for a minor, recoverable error. Zero
	// means the default (-3).
	MinorError int `yaml:"minor_error"`
	// MajorError is the score change for an error affecting reliability. Zero
	// means the default (-10).
	MajorError int `yaml:"major_error"`
	// CriticalError is the score change for a severe error. Zero means the
	// default (-25).
	CriticalError int `yaml:"critical_error"`
	// FatalError is the score change for an error warranting immediate
	// removal from selection. Zero means the default (-50).
	FatalError int `yaml:"fatal_error"`
	// RecoverySuccess is parsed and not implemented: the signal type was
	// removed. A successful relay from a recovering endpoint is a success like
	// any other (docs/scoring.md §7.2).
	RecoverySuccess int `yaml:"recovery_success"`
	// SlowResponse is parsed and not implemented: latency does not penalise a
	// score. It is reported instead, as a per-key EWMA in the admin listing
	// (docs/scoring.md §7.2).
	SlowResponse int `yaml:"slow_response"`
	// VerySlowResponse is parsed and not implemented, for the same reason as
	// slow_response (docs/scoring.md §7.2).
	VerySlowResponse int `yaml:"very_slow_response"`
}

// HealthCheckConfig controls active health checks.
//
// NOTE: PATH's active_health_checks.external has no field here on purpose.
// PATH fetches a fleet-wide rule file from that URL (it did not when SAGE
// forked; it does as of PATH 2026-08). SAGE does not: the plugin's own checks
// cover what the file's block-number, chain-id and status rows do, with a real
// chain-id comparison and sync tracked by block consensus, and the file's
// per-service `check_interval: 10s` rows are most of PATH's probe volume. A
// PATH config carrying the key is reported at startup via Config.Ignored
// rather than parsing into a field that does nothing; see docs/path-compat.md.
// If remote rules are wanted later, add them as a live feature then.
type HealthCheckConfig struct {
	// Enabled turns active health checking off when written as `enabled:
	// false`; absent, probing is on, which is PATH's default too. Checks probe
	// endpoints on a schedule rather than waiting for client traffic to reveal
	// a problem, and are what keep block height and chain ID tracking current.
	// Off, selection is graded by client traffic alone and readiness does not
	// wait for a warm-up. The key's presence is read from the YAML itself,
	// since a bool cannot tell absent from false; until 2026-09-04 the value
	// gated only the readiness warm-up and probing ran regardless.
	Enabled bool `yaml:"enabled"`
	// disabled is set by the loader when the YAML carried `enabled: false`.
	disabled bool

	// Interval is how often every backend of every service is probed. Zero
	// means 30s. Each probe is a paid relay against the app's stake, and the
	// cost is linear in it: at 30s a gateway with 60 services and ~1,500
	// distinct backend URLs spends roughly 60 probe relays a second with no
	// client traffic at all. A per-service `local[].check_interval` overrides
	// it for that service; a check may also carry a slower cadence of its own
	// (the EVM chain-id check probes every 5m, since a chain id does not
	// change), which is never made faster by this.
	Interval time.Duration `yaml:"interval"`

	// MaxWorkers is how many health-check probes may be in flight at once.
	// Zero means 4.
	//
	// It is the second dial on probe cost, and it trades against `interval` in
	// opposite directions. A cycle walks every backend of every service and
	// dispatches through this pool, and the cycle runs on the scheduler's own
	// goroutine — so when a cycle takes longer than `interval`, the achieved
	// cadence is the cycle, not the interval, and probes arrive in one burst
	// per cycle rather than spread across it. More workers shortens the cycle;
	// a longer interval cuts the number of probes outright.
	//
	// Raise it carefully. The suppliers a probe talks to are the same ones
	// serving client relays, so probe concurrency competes with traffic for
	// them — which is why the fix for a slow cycle was a probe timeout before
	// it was more workers. `sage_health_check_cycle_seconds` is the
	// before-and-after; `sage_health_check_cycle_overruns_total` says whether
	// the interval is being achieved at all.
	MaxWorkers int `yaml:"max_workers"`

	// DisableBackendURLDedup turns off per-backend deduplication, restoring one
	// health-check relay per supplier.
	//
	// Deduplication is on by default because the thing a check measures is the
	// backend, not the registration pointing at it: several staked suppliers
	// routinely front one URL, and probing each of them asks the same machine
	// the same question several times per cycle. That is 2.5-3x the relay
	// volume for no extra information, and it dilutes the signal — a backend
	// probed once per cycle shows an outage immediately, one probed through
	// five suppliers shows five samples of the same moment.
	//
	// The flag is negative-sense on purpose: the zero value has to be the
	// behavior we want, and an operator disabling a default needs to say so in
	// the config rather than leave a field unset.
	DisableBackendURLDedup bool `yaml:"disable_backend_url_dedup"`

	// ProbeTimeout bounds one health check. Zero means 5s.
	//
	// It is deliberately separate from defaults.timeout.relay_timeout, which a
	// probe would otherwise inherit: a hung backend then holds one of the few
	// health-check workers for the whole relay timeout, and the sweep that is
	// supposed to run every `interval` takes minutes instead. A backend that
	// has not answered a health check in a few seconds is unhealthy, and
	// waiting longer does not change that verdict.
	//
	// Raise it rather than lower it if probes start reporting timeouts against
	// backends that are merely loaded: a probe cut off early is graded a minor
	// error, so too short a value manufactures the failure it reports.
	ProbeTimeout time.Duration `yaml:"probe_timeout"`

	// MinTrafficSignals is how many client-traffic reputation signals a
	// backend must record within one cycle before traffic-informed probing
	// skips its check. Only consulted when the traffic_informed_probing
	// feature flag is on for the service; zero derives it from the
	// observation pipeline's sample_rate, which is the number that decides
	// how much traffic it takes to replace a probe's observation.
	//
	// Raise it to probe more and trust traffic less. There is deliberately no
	// value meaning "skip on any traffic": a probe is the only observation
	// source that bypasses sampling, so one relay does not stand in for one
	// probe.
	MinTrafficSignals uint64 `yaml:"min_traffic_signals"`

	// Local defines per-service health checks in the config file. They are
	// additional to whatever the service's QoS plugin already checks, never a
	// replacement: the plugin's checks are what make block height and chain ID
	// tracking work, and a config that silently switched them off would degrade
	// endpoint selection without saying so.
	Local []ServiceHealthChecks `yaml:"local"`
}

// ServiceHealthChecks is the set of configured checks for one service.
type ServiceHealthChecks struct {
	// ServiceID is the service these checks apply to.
	ServiceID string `yaml:"service_id"`
	// CheckInterval is this service's probe cadence, overriding the global
	// `interval`. It applies to the plugin's checks as well as the ones listed
	// here, and it applies whether or not `enabled` is set — it is the
	// service's cadence, not a property of the check list. Same key as PATH's;
	// note PATH configs commonly carry `10s`, which against a 30s global
	// triples that service's probe spend. Zero means the global interval.
	CheckInterval time.Duration `yaml:"check_interval"`
	// Enabled must be set for the checks to run, mirroring the PATH configs
	// this parses — they spell out `enabled: true` on every block. A block with
	// checks and no `enabled` is warned about at startup rather than quietly
	// doing nothing.
	Enabled bool          `yaml:"enabled"`
	Checks  []HealthCheck `yaml:"checks"`
}

// HealthCheck is a single configured request to send to every endpoint of a
// service.
type HealthCheck struct {
	// Name identifies the check in logs and metrics.
	Name string `yaml:"name"`
	// Method is the HTTP method, or the JSON-RPC method for a json_rpc check.
	Method string `yaml:"method"`
	// Path is the request path, for REST and CometBFT checks.
	Path string `yaml:"path"`
	// Body is the raw request body, typically a JSON-RPC envelope.
	Body string `yaml:"body"`
	// Type names the RPC type to relay as ("json_rpc", "rest", "comet_bft").
	// Empty means json_rpc, which is what the overwhelming majority of checks
	// are.
	Type string `yaml:"type"`
	// ExpectedStatusCode is the HTTP status treated as healthy. Zero means any
	// 2xx, which is the sane default and keeps a check from failing because a
	// backend answered 204 instead of 200.
	ExpectedStatusCode int `yaml:"expected_status_code"`
	// ReputationSignal names the penalty on failure: "minor_error" (default),
	// "major_error", "critical_error".
	ReputationSignal string `yaml:"reputation_signal"`
	// Timeout bounds this check. It should be tighter than a relay timeout —
	// a check is meant to notice a slow endpoint, not to wait one out.
	Timeout time.Duration `yaml:"timeout"`
}

// Disabled reports whether the YAML turned active health checks off with
// `enabled: false`.
func (h HealthCheckConfig) Disabled() bool { return h.disabled }

// IsDeclaredButOff reports a block that defines checks and never runs them —
// the failure mode this config shape invites, worth saying out loud at startup.
func (s ServiceHealthChecks) IsDeclaredButOff() bool {
	return !s.Enabled && len(s.Checks) > 0
}

// ObservationPipelineConfig controls async observation processing.
type ObservationPipelineConfig struct {
	// Enabled turns on async observation processing.
	Enabled bool `yaml:"enabled"`
	// SampleRate is the fraction of relays observed, 0.0 to 1.0. Health checks
	// are observed regardless — they are low-volume and deliberate, while relay
	// traffic is the hot path and deep parsing of all of it would cost more
	// than it reveals.
	SampleRate float64 `yaml:"sample_rate"`
	// WorkerCount is how many goroutines drain the observation queue.
	WorkerCount int `yaml:"worker_count"`
	// QueueSize is the queue depth. When it fills, observations are dropped
	// rather than blocking: the pipeline is best-effort by design, and must
	// never apply back-pressure to relays.
	QueueSize int `yaml:"queue_size"`
}

// GetServiceConfig returns the config for a specific service ID, or nil.
func (u *UnifiedServicesConfig) GetServiceConfig(id string) *ServiceConfig {
	for i := range u.Services {
		if u.Services[i].ID == id {
			return &u.Services[i]
		}
	}
	return nil
}

// EffectiveRetry returns the retry config for a service, falling back to
// defaults field by field. A service's own `enabled: false` sticks.
func (s *ServiceConfig) EffectiveRetry(defaults ServiceDefaults) RetryConfig {
	return mergeRetry(s.Retry, defaults.Retry)
}

// EffectiveTimeout returns the timeout config, falling back to defaults.
func (s *ServiceConfig) EffectiveTimeout(defaults ServiceDefaults) TimeoutConfig {
	t := s.Timeout
	if t.RelayTimeout == 0 {
		t.RelayTimeout = defaults.Timeout.RelayTimeout
	}
	return t
}
