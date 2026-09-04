package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	return parse(data)
}

// LoadFromEnv loads configuration from the GATEWAY_CONFIG environment variable.
func LoadFromEnv() (*Config, error) {
	data := os.Getenv("GATEWAY_CONFIG")
	if data == "" {
		return nil, fmt.Errorf("GATEWAY_CONFIG environment variable not set")
	}
	return parse([]byte(data))
}

func parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config YAML: %w", err)
	}
	cfg.Ignored = ignoredFields(data)
	tree := yamlTree(data)
	cfg.Inert = InertKeys(tree)
	cfg.Unimplemented = UnimplementedKeys(tree)
	applyDefaults(&cfg)
	cfg.Warnings = reputationWarnings(cfg.Gateway.Reputation)
	cfg.Warnings = append(cfg.Warnings, applyRetryDisabled(&cfg, tree)...)
	cfg.Warnings = append(cfg.Warnings, applyHealthChecksDisabled(&cfg, tree)...)
	cfg.Warnings = append(cfg.Warnings, gatewayModeWarnings(cfg.Gateway.GatewayMode)...)
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return &cfg, nil
}

// notFoundInType matches the yaml.v3 message for a key with no struct field.
var notFoundInType = regexp.MustCompile(`field (\S+) not found in type (\S+)`)

// ignoredFields returns the config keys present in the YAML that SAGE has no
// field for, so a caller can say so out loud at startup.
//
// This is a second, throwaway decode with KnownFields set. The real decode
// above stays lenient on purpose: erroring would mean SAGE could no longer load
// a PATH config unmodified, which is a property worth keeping. The cost of
// leniency is silence, and silence is what this buys back — a key that parses
// and then does nothing (active_health_checks.local, coordination) reads to an
// operator as a feature that is on.
func ignoredFields(data []byte) []string {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var probe Config
	err := dec.Decode(&probe)
	if err == nil {
		return nil
	}

	var typeErr *yaml.TypeError
	if !errors.As(err, &typeErr) {
		// A genuine parse failure — the lenient decode above already reported
		// it, so nothing to add here.
		return nil
	}

	var out []string
	for _, e := range typeErr.Errors {
		// KnownFields lumps unknown keys in with ordinary type errors. Only the
		// former are "SAGE has no such field"; a real type mismatch is the
		// lenient decode's business, not ours.
		//
		// Keep yaml's whole message rather than just the field name: it carries
		// the line and the struct, and "field timeout not found" on its own does
		// not tell an operator which timeout, out of the several a config has.
		if notFoundInType.MatchString(e) {
			out = append(out, e)
		}
	}
	return out
}

// yamlTree decodes the file a second time into generic maps and slices. It
// is how the keys an operator actually wrote are told apart from the fields
// a struct has: a zero-valued field cannot say whether anyone set it, and a
// value-typed bool cannot say whether "false" was written or defaulted. The
// inert and unimplemented registries walk it, and so does the retry switch.
// A parse failure yields nil; the real decode has already reported it.
func yamlTree(data []byte) any {
	var tree any
	if err := yaml.Unmarshal(data, &tree); err != nil {
		return nil
	}
	return tree
}

// gatewayModeWarnings says what SAGE makes of a mode other than centralized.
// The key is required and its value is not acted on: SAGE signs with the
// keys it holds. A config that says "delegated" was written for a gateway
// that honours App-Address per request, and this one does not
// (docs/path-compat.md), which a client can notice; so it is said here.
func gatewayModeWarnings(mode string) []string {
	switch mode {
	case "", "centralized":
		return nil
	}
	return []string{fmt.Sprintf(
		"gateway_config.gateway_mode is %q, but SAGE signs every relay with its configured owned_apps_private_keys_hex and ignores the App-Address header; a per-request delegated app is not supported", mode)}
}

func applyDefaults(cfg *Config) {
	// Router defaults
	if cfg.Router.Port == 0 {
		cfg.Router.Port = 3069
	}
	// PATH's server defaults; see RouterConfig. The router applies the same
	// ones for a RouterConfig built without this, but this is where a loaded
	// file gets them, and the 30/30/120 that used to be here would have made
	// the router's defaults unreachable from a config file.
	if cfg.Router.ReadTimeout == 0 {
		cfg.Router.ReadTimeout = 60 * time.Second
	}
	if cfg.Router.WriteTimeout == 0 {
		cfg.Router.WriteTimeout = 120 * time.Second
	}
	if cfg.Router.IdleTimeout == 0 {
		cfg.Router.IdleTimeout = 180 * time.Second
	}

	// Redis defaults
	if cfg.Redis.PoolSize == 0 {
		cfg.Redis.PoolSize = 10
	}
	if cfg.Redis.DialTimeout == 0 {
		cfg.Redis.DialTimeout = 5 * time.Second
	}

	// Metrics defaults
	if cfg.Metrics.PrometheusAddr == "" {
		cfg.Metrics.PrometheusAddr = ":9090"
	}
	// PprofAddr is deliberately NOT defaulted. /debug/pprof serves heap dumps,
	// which contain whatever is in memory — including signing keys — so the
	// sensible default for the zero value is off, not ":6060" on every
	// interface. Set it explicitly to enable (see bench/mock-config.yaml).

	// Admin defaults to loopback rather than off: unlike pprof, the admin API is
	// how flags are toggled at runtime, so defaulting it off would retire a
	// feature. Loopback keeps it working while making "unconfigured" mean
	// "unreachable from outside this host". See DefaultAdminAddr.
	if cfg.Admin.Addr == "" {
		cfg.Admin.Addr = DefaultAdminAddr
	}

	// Logger defaults
	if cfg.Logger.Level == "" {
		cfg.Logger.Level = "info"
	}

	// Concurrency defaults
	if cfg.Concurrency.MaxConcurrentRelays == 0 {
		cfg.Concurrency.MaxConcurrentRelays = 10000
	}
	if cfg.Concurrency.MaxBatchPayloads == 0 {
		cfg.Concurrency.MaxBatchPayloads = 5500
	}

	// Feature flags need no defaulting here: cfg.FeatureFlags carries only what
	// YAML set, and the stores fall back to featureflag.DefaultFlags for anything
	// absent (including an entirely omitted section, which parses to a nil map).

	// Observation pipeline defaults
	if cfg.Gateway.ObservationPipeline.SampleRate == 0 {
		cfg.Gateway.ObservationPipeline.SampleRate = 0.1
	}
	if cfg.Gateway.ObservationPipeline.WorkerCount == 0 {
		cfg.Gateway.ObservationPipeline.WorkerCount = 4
	}
	if cfg.Gateway.ObservationPipeline.QueueSize == 0 {
		cfg.Gateway.ObservationPipeline.QueueSize = 1000
	}
}

func validate(cfg *Config) error {
	switch cfg.Protocol.Type {
	case "", ProtocolTypeShannon:
		// Shannon needs a fullnode and gateway identity.
		if cfg.FullNode.RPCURL == "" {
			return fmt.Errorf("full_node_config.rpc_url is required")
		}
		if cfg.FullNode.GRPCConfig.HostPort == "" {
			return fmt.Errorf("full_node_config.grpc_config.host_port is required")
		}
		if cfg.Gateway.GatewayMode == "" {
			return fmt.Errorf("gateway_config.gateway_mode is required")
		}
	case ProtocolTypeMock:
		// Mock backend is self-contained — no fullnode/identity required.
		for i, rate := range cfg.Protocol.Mock.FailureRates {
			if rate < 0 || rate > 1 {
				return fmt.Errorf("protocol.mock.failure_rates[%d] must be in [0, 1], got %v", i, rate)
			}
		}
	default:
		return fmt.Errorf("protocol.type must be %q or %q, got %q",
			ProtocolTypeShannon, ProtocolTypeMock, cfg.Protocol.Type)
	}
	if err := ValidateAdmin(cfg.Admin); err != nil {
		return err
	}
	if cfg.Concurrency.MaxBatchPayloads > cfg.Concurrency.MaxConcurrentRelays {
		return fmt.Errorf("max_batch_payloads (%d) must be <= max_concurrent_relays (%d)",
			cfg.Concurrency.MaxBatchPayloads, cfg.Concurrency.MaxConcurrentRelays)
	}
	if err := validateReputation(cfg.Gateway.Reputation); err != nil {
		return err
	}
	if err := validateRPCTypeFallbacks(cfg.Gateway.AllServices()); err != nil {
		return err
	}
	return nil
}

// knownRPCTypes is what a config may name where an RPC type is expected.
var knownRPCTypes = map[string]bool{
	"json_rpc": true, "rest": true, "comet_bft": true, "websocket": true, "grpc": true,
}

// validateRPCTypeFallbacks refuses a fallback that cannot be followed: an
// unknown type on either side, or a type mapped onto itself. Either would be a
// mapping the protocol silently never applies, which reads as "the fallback is
// on" in the file while every affected supplier stays invisible.
func validateRPCTypeFallbacks(services []ServiceConfig) error {
	for _, svc := range services {
		for from, to := range svc.RPCTypeFallbacks {
			if !knownRPCTypes[from] {
				return fmt.Errorf("service %q rpc_type_fallbacks: unknown rpc type %q", svc.ID, from)
			}
			if !knownRPCTypes[to] {
				return fmt.Errorf("service %q rpc_type_fallbacks: %s falls back to unknown rpc type %q", svc.ID, from, to)
			}
			if from == to {
				return fmt.Errorf("service %q rpc_type_fallbacks: %s falls back to itself", svc.ID, from)
			}
		}
	}
	return nil
}

// validateReputation refuses a scoring block that cannot mean anything.
//
// The line it draws is between impossible and merely odd. A traffic share of
// 101%, or a chronic onset rate at or above the full rate, describes no
// behaviour at all — there is nothing to do with it but refuse. Thresholds
// that fail to descend are a different case: they describe a real selection,
// just a lopsided one, and a PATH config in production has them. Those are
// warnings; see reputationWarnings.
//
// Only the gateway-level block is checked. One TieredSelector serves every
// service, so a per-service copy of these keys is not read at all.
func validateReputation(r ReputationConfig) error {
	// A negative threshold is the same kind of nothing as a negative traffic
	// share: scores run 0..100, so it names no band. It cannot be allowed to
	// fall through to SelectorConfig, which only copies positive values and
	// would hand back the default — an operator who typed -1 would get 80.
	for _, t := range []struct {
		key   string
		value int
	}{
		{"reputation_config.min_threshold", r.MinThreshold},
		{"reputation_config.tiered_selection.tier1_threshold", r.TieredSelection.Tier1Threshold},
		{"reputation_config.tiered_selection.tier2_threshold", r.TieredSelection.Tier2Threshold},
		{"reputation_config.tiered_selection.probation.threshold", r.TieredSelection.Probation.Threshold},
	} {
		if t.value < 0 {
			return fmt.Errorf("%s must not be negative, got %d", t.key, t.value)
		}
	}
	if p := r.TieredSelection.Probation.TrafficPercent; p < 0 || p > 100 {
		return fmt.Errorf("reputation_config.tiered_selection.probation.traffic_percent must be 0..100, got %d", p)
	}
	if p := r.TieredSelection.Tier2TrafficPercent; p > 100 {
		return fmt.Errorf("reputation_config.tiered_selection.tier2_traffic_percent must be at most 100, got %d", p)
	}
	// Defaulted, not Normalized: Normalized repairs an inconsistent pair for
	// programmatic callers, and reading through it here would turn an operator's
	// mistake into a silently different curve instead of a refusal.
	rc := r.RateConfig().Defaulted()
	if rc.OnsetRate < 0 || rc.FullRate >= 1 || rc.OnsetRate >= rc.FullRate {
		return fmt.Errorf("reputation_config: chronic_onset_rate (%v) must be >= 0 and below chronic_full_rate (%v), which must be below 1", rc.OnsetRate, rc.FullRate)
	}
	return nil
}

// reputationWarnings reports thresholds that load and select in a way the
// operator probably did not intend.
//
// The thresholds are read as the selector will see them — defaults filled in,
// not the raw keys — because that is what decides behaviour, and because a
// config naming two of the five can collide with the defaults of the other
// three. Each sentence says what the selector actually does with the values
// given, since the operator's question on reading the log is "so what happens
// to my traffic", not "which inequality did I break".
//
// The classification order in reputation.TieredSelector.classify is what makes
// these harmless-but-wrong rather than broken: min_threshold first, then
// probation, then tier 1, then tier 2. A band that a later branch can never be
// reached for is empty, and nothing errors.
func reputationWarnings(r ReputationConfig) []string {
	sel := r.SelectorConfig()
	var out []string

	if sel.Tier2Threshold >= sel.Tier1Threshold {
		out = append(out, fmt.Sprintf(
			"reputation_config.tiered_selection.tier2_threshold (%v) is not below tier1_threshold (%v): tier 2 is empty — every score at or above %v is tier 1, and everything below it falls to tier 3",
			sel.Tier2Threshold, sel.Tier1Threshold, sel.Tier1Threshold))
	}
	if sel.ProbationThreshold >= sel.Tier2Threshold {
		// What tier 2 is left with depends on whether probation also swallowed
		// tier 1's band. Saying "from 90 up to tier1_threshold (80)" would be
		// worse than saying nothing, and it would contradict the tier-2-is-
		// empty sentence above whenever both fire.
		leftover := "tier 2 is empty"
		if sel.ProbationThreshold < sel.Tier1Threshold {
			leftover = fmt.Sprintf("tier 2 holds only scores from %v up to tier1_threshold (%v)",
				sel.ProbationThreshold, sel.Tier1Threshold)
		}
		out = append(out, fmt.Sprintf(
			"reputation_config.tiered_selection.probation.threshold (%v) is not below tier2_threshold (%v): scores from %v up to %v are treated as probation, so %s",
			sel.ProbationThreshold, sel.Tier2Threshold, sel.MinThreshold, sel.ProbationThreshold, leftover))
	}
	if sel.MinThreshold > sel.ProbationThreshold {
		out = append(out, fmt.Sprintf(
			"reputation_config.min_threshold (%v) is above the probation threshold (%v): no endpoint is ever put on probation, because a score below %v is filtered out before the probation band is reached",
			sel.MinThreshold, sel.ProbationThreshold, sel.MinThreshold))
	}
	return out
}
