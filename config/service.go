package config

import "time"

// GatewayConfig holds the gateway-specific configuration.
// Supports both PATH config formats:
//   - gateway_config.services (production)
//   - gateway_config.unified_services.services (newer format)
type GatewayConfig struct {
	GatewayMode          string   `yaml:"gateway_mode"`
	GatewayAddress       string   `yaml:"gateway_address"`
	GatewayPrivateKeyHex string   `yaml:"gateway_private_key_hex"`
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

	HealthChecks        HealthCheckConfig         `yaml:"active_health_checks"`
	ObservationPipeline ObservationPipelineConfig `yaml:"observation_pipeline"`

	LatencyProfiles map[string]LatencyProfile `yaml:"latency_profiles"`
	Defaults        ServiceDefaults           `yaml:"defaults"`
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

// EffectiveDefaults returns defaults from whichever config format was used.
// Checks gateway-level retry_config, then Defaults, then UnifiedServices.Defaults.
func (g *GatewayConfig) EffectiveDefaults() ServiceDefaults {
	// Gateway-level retry_config (production format: gateway_config.retry_config)
	if g.Retry.MaxRetries > 0 {
		defaults := g.Defaults
		if defaults.Retry.MaxRetries == 0 {
			defaults.Retry = g.Retry
		}
		return defaults
	}
	if g.Defaults.Retry.MaxRetries > 0 || g.Defaults.Timeout.RelayTimeout > 0 {
		return g.Defaults
	}
	return g.UnifiedServices.Defaults
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
	MiddlewareChain []string                  `yaml:"middleware_chain"`
}

// ServiceConfig defines a single service.
type ServiceConfig struct {
	ID             string   `yaml:"id"`
	Type           string   `yaml:"type"`
	RPCTypes       []string `yaml:"rpc_types"`
	SyncAllowance  uint64   `yaml:"sync_allowance"`
	LatencyProfile string   `yaml:"latency_profile"`

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

	// NOTE: PATH's rpc_type_fallbacks has no field here on purpose. It was
	// declared and read by nothing — dead config describing a fallback SAGE does
	// not implement. SAGE's fallback for an unclassifiable request is fixed, not
	// configurable: detectRPCType defaults to JSON-RPC (relay/middleware/parse.go).
	// A PATH config carrying the key is now reported at startup via Config.Ignored
	// rather than parsing into a field that does nothing. If a configurable
	// per-service fallback is ever wanted, add it as a live field then — do not
	// resurrect a dead one. (Not to be confused with the planned per-service
	// allowed-methods allowlist, which is a separate, live feature.)
}

// ServiceDefaults provides default values for services.
type ServiceDefaults struct {
	Retry      RetryConfig      `yaml:"retry_config"`
	Timeout    TimeoutConfig    `yaml:"timeout_config"`
	Reputation ReputationConfig `yaml:"reputation_config"`
}

// TimeoutConfig controls relay timeouts.
type TimeoutConfig struct {
	RelayTimeout time.Duration `yaml:"relay_timeout"`
}

// RetryConfig controls retry behavior. Zero values = disabled.
type RetryConfig struct {
	Enabled           bool          `yaml:"enabled"`
	MaxRetries        int           `yaml:"max_retries"`
	MaxRetryLatency   time.Duration `yaml:"max_retry_latency"`
	RetryOn5xx        bool          `yaml:"retry_on_5xx"`
	RetryOnTimeout    bool          `yaml:"retry_on_timeout"`
	RetryOnConnection bool          `yaml:"retry_on_connection"`
	ConnectTimeout    time.Duration `yaml:"connect_timeout"`
	HedgeDelay        time.Duration `yaml:"hedge_delay"`
	MaxLatency        time.Duration `yaml:"max_latency"`
}

// IsEnabled returns true if retries are configured.
func (c RetryConfig) IsEnabled() bool { return c.MaxRetries > 0 }

// HedgeEnabled returns true if hedge racing is configured.
func (c RetryConfig) HedgeEnabled() bool { return c.HedgeDelay > 0 }

// ExternalBlockSource defines an external RPC endpoint for ground-truth block heights.
type ExternalBlockSource struct {
	URL         string        `yaml:"url"`
	Type        string        `yaml:"type"`
	Method      string        `yaml:"method"`
	Path        string        `yaml:"path"`
	Interval    time.Duration `yaml:"interval"`
	Timeout     time.Duration `yaml:"timeout"`
	GracePeriod time.Duration `yaml:"grace_period"`
}

// LatencyProfile defines per-chain latency thresholds for reputation scoring.
type LatencyProfile struct {
	FastThreshold    time.Duration `yaml:"fast_threshold"`
	NormalThreshold  time.Duration `yaml:"normal_threshold"`
	SlowThreshold    time.Duration `yaml:"slow_threshold"`
	PenaltyThreshold time.Duration `yaml:"penalty_threshold"`
	SevereThreshold  time.Duration `yaml:"severe_threshold"`
	FastBonus        int           `yaml:"fast_bonus"`
	SlowPenalty      int           `yaml:"slow_penalty"`
	VerySlowPenalty  int           `yaml:"very_slow_penalty"`
}

// ReputationConfig controls the reputation system.
type ReputationConfig struct {
	Enabled         bool          `yaml:"enabled"`
	StorageType     string        `yaml:"storage_type"`
	KeyGranularity  string        `yaml:"key_granularity"`
	InitialScore    int           `yaml:"initial_score"`
	MinThreshold    int           `yaml:"min_threshold"`
	RecoveryTimeout time.Duration `yaml:"recovery_timeout"`

	TieredSelection TieredSelectionConfig `yaml:"tiered_selection"`
	SignalImpacts   SignalImpactsConfig   `yaml:"signal_impacts"`
	// NOTE: PATH's strike_system has no field here on purpose. SAGE has no
	// strike/cooldown mechanism — reputation is a continuous score feeding
	// tiered selection, so there is no cooldown for these knobs to tune. The
	// struct existed and was read by nothing; a PATH config carrying the key is
	// now reported at startup via Config.Ignored instead of parsing into fields
	// that do not do anything.
}

// TieredSelectionConfig controls endpoint tiering.
type TieredSelectionConfig struct {
	Enabled        bool            `yaml:"enabled"`
	Tier1Threshold int             `yaml:"tier1_threshold"`
	Tier2Threshold int             `yaml:"tier2_threshold"`
	Probation      ProbationConfig `yaml:"probation"`
}

// ProbationConfig controls probation routing for recovering endpoints.
type ProbationConfig struct {
	Enabled            bool    `yaml:"enabled"`
	Threshold          int     `yaml:"threshold"`
	TrafficPercent     int     `yaml:"traffic_percent"`
	RecoveryMultiplier float64 `yaml:"recovery_multiplier"`
}

// SignalImpactsConfig defines how each signal type affects reputation.
type SignalImpactsConfig struct {
	Success          int `yaml:"success"`
	MinorError       int `yaml:"minor_error"`
	MajorError       int `yaml:"major_error"`
	CriticalError    int `yaml:"critical_error"`
	FatalError       int `yaml:"fatal_error"`
	RecoverySuccess  int `yaml:"recovery_success"`
	SlowResponse     int `yaml:"slow_response"`
	VerySlowResponse int `yaml:"very_slow_response"`
}

// HealthCheckConfig controls active health checks.
//
// NOTE: PATH's active_health_checks.external has no field here on purpose. It
// declared a URL to fetch health check rules from, and nothing read it — a
// config key that reads to an operator as "my fleet-wide rules are live" while
// no rule was ever fetched. A PATH config carrying it is now reported at
// startup via Config.Ignored instead of parsing into a field that does nothing.
// If remote rules are wanted later, add them as a live feature then.
type HealthCheckConfig struct {
	Enabled bool `yaml:"enabled"`

	// Local defines per-service health checks in the config file. They are
	// additional to whatever the service's QoS plugin already checks, never a
	// replacement: the plugin's checks are what make block height and chain ID
	// tracking work, and a config that silently switched them off would degrade
	// endpoint selection without saying so.
	Local []ServiceHealthChecks `yaml:"local"`
}

// ServiceHealthChecks is the set of configured checks for one service.
//
// CheckInterval is deliberately absent: PATH allows a per-service interval, the
// executor runs one global loop, and declaring the field would parse a value
// SAGE never honours. It is reported at startup instead.
type ServiceHealthChecks struct {
	ServiceID string `yaml:"service_id"`
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
	Name   string `yaml:"name"`
	Method string `yaml:"method"`
	Path   string `yaml:"path"`
	Body   string `yaml:"body"`
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
	ReputationSignal string        `yaml:"reputation_signal"`
	Timeout          time.Duration `yaml:"timeout"`
}

// IsDeclaredButOff reports a block that defines checks and never runs them —
// the failure mode this config shape invites, worth saying out loud at startup.
func (s ServiceHealthChecks) IsDeclaredButOff() bool {
	return !s.Enabled && len(s.Checks) > 0
}

// ObservationPipelineConfig controls async observation processing.
type ObservationPipelineConfig struct {
	Enabled     bool    `yaml:"enabled"`
	SampleRate  float64 `yaml:"sample_rate"`
	WorkerCount int     `yaml:"worker_count"`
	QueueSize   int     `yaml:"queue_size"`
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

// EffectiveRetry returns the retry config for a service, falling back to defaults.
func (s *ServiceConfig) EffectiveRetry(defaults ServiceDefaults) RetryConfig {
	r := s.Retry
	if r.MaxRetries == 0 {
		r.MaxRetries = defaults.Retry.MaxRetries
	}
	if r.HedgeDelay == 0 {
		r.HedgeDelay = defaults.Retry.HedgeDelay
	}
	if r.MaxLatency == 0 {
		r.MaxLatency = defaults.Retry.MaxLatency
	}
	if !r.RetryOn5xx && defaults.Retry.RetryOn5xx {
		r.RetryOn5xx = defaults.Retry.RetryOn5xx
	}
	return r
}

// EffectiveTimeout returns the timeout config, falling back to defaults.
func (s *ServiceConfig) EffectiveTimeout(defaults ServiceDefaults) TimeoutConfig {
	t := s.Timeout
	if t.RelayTimeout == 0 {
		t.RelayTimeout = defaults.Timeout.RelayTimeout
	}
	return t
}
