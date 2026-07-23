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
	applyDefaults(&cfg)
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

func applyDefaults(cfg *Config) {
	// Router defaults
	if cfg.Router.Port == 0 {
		cfg.Router.Port = 3069
	}
	if cfg.Router.ReadTimeout == 0 {
		cfg.Router.ReadTimeout = 30 * time.Second
	}
	if cfg.Router.WriteTimeout == 0 {
		cfg.Router.WriteTimeout = 30 * time.Second
	}
	if cfg.Router.IdleTimeout == 0 {
		cfg.Router.IdleTimeout = 120 * time.Second
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
	default:
		return fmt.Errorf("protocol.type must be %q or %q, got %q",
			ProtocolTypeShannon, ProtocolTypeMock, cfg.Protocol.Type)
	}
	if cfg.Concurrency.MaxBatchPayloads > cfg.Concurrency.MaxConcurrentRelays {
		return fmt.Errorf("max_batch_payloads (%d) must be <= max_concurrent_relays (%d)",
			cfg.Concurrency.MaxBatchPayloads, cfg.Concurrency.MaxConcurrentRelays)
	}
	return nil
}
