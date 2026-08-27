package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/reputation"
)

// SAGE is a restructured fork of PATH and must load a PATH config unmodified.
// These tests pin that down against checked-in fixtures rather than a config on
// one machine: an absolute path into somebody's home directory skips for every
// other developer and for CI, and silently rots when that file changes — which
// is exactly what it did, failing on a service count that had drifted.
//
// The fixtures are synthetic but exhaustive by construction: see
// TestConfigFixtureIsExhaustive, which fails if a field exists in the config
// structs and not in the YAML. That is the guarantee a real-world config was
// standing in for.
const (
	fixturePath        = "testdata/path_config.yaml"
	unifiedFixturePath = "testdata/path_config_unified.yaml"
)

func loadFixture(t *testing.T, path string) *Config {
	t.Helper()
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile(%s) failed: %v", path, err)
	}
	return cfg
}

func TestConfigCompatibility_GlobalSections(t *testing.T) {
	cfg := loadFixture(t, fixturePath)

	t.Run("redis", func(t *testing.T) {
		want := RedisConfig{
			Address:      "redis.invalid:6379",
			Password:     "fixture-password",
			DB:           7,
			PoolSize:     42,
			DialTimeout:  6 * time.Second,
			ReadTimeout:  4 * time.Second,
			WriteTimeout: 3 * time.Second,
		}
		if cfg.Redis != want {
			t.Errorf("redis = %+v, want %+v", cfg.Redis, want)
		}
	})

	t.Run("router", func(t *testing.T) {
		if cfg.Router.Port != 3069 {
			t.Errorf("port = %d, want 3069", cfg.Router.Port)
		}
		if cfg.Router.ReadTimeout != 31*time.Second {
			t.Errorf("read_timeout = %v, want 31s", cfg.Router.ReadTimeout)
		}
		if cfg.Router.WriteTimeout != 32*time.Second {
			t.Errorf("write_timeout = %v, want 32s", cfg.Router.WriteTimeout)
		}
		if cfg.Router.IdleTimeout != 121*time.Second {
			t.Errorf("idle_timeout = %v, want 121s", cfg.Router.IdleTimeout)
		}
		if cfg.Router.WebsocketMessageBufferSize != 8192 {
			t.Errorf("websocket_message_buffer_size = %d, want 8192", cfg.Router.WebsocketMessageBufferSize)
		}
		if got := strings.Join(cfg.Router.TrustedProxies, ","); got != "10.0.0.0/8,192.168.0.0/16" {
			t.Errorf("trusted_proxies = %v", cfg.Router.TrustedProxies)
		}
	})

	t.Run("logger_metrics_admin", func(t *testing.T) {
		if cfg.Logger.Level != "warn" {
			t.Errorf("logger level = %q, want warn", cfg.Logger.Level)
		}
		if cfg.Metrics.PrometheusAddr != ":9099" {
			t.Errorf("prometheus_addr = %q, want :9099", cfg.Metrics.PrometheusAddr)
		}
		if cfg.Metrics.PprofAddr != "localhost:6060" {
			t.Errorf("pprof_addr = %q, want localhost:6060", cfg.Metrics.PprofAddr)
		}
		if cfg.Admin.Addr != "localhost:9095" {
			t.Errorf("admin addr = %q, want localhost:9095", cfg.Admin.Addr)
		}
		if cfg.Admin.MaxDrain != 2*time.Hour {
			t.Errorf("admin max_drain = %v, want 2h", cfg.Admin.MaxDrain)
		}
	})

	t.Run("concurrency", func(t *testing.T) {
		if cfg.Concurrency.MaxConcurrentRelays != 8000 {
			t.Errorf("max_concurrent_relays = %d, want 8000", cfg.Concurrency.MaxConcurrentRelays)
		}
		if cfg.Concurrency.MaxBatchPayloads != 4000 {
			t.Errorf("max_batch_payloads = %d, want 4000", cfg.Concurrency.MaxBatchPayloads)
		}
	})

	t.Run("full_node", func(t *testing.T) {
		fn := cfg.FullNode
		if fn.RPCURL != "https://fullnode.invalid/rpc" {
			t.Errorf("rpc_url = %q", fn.RPCURL)
		}
		if fn.GRPCConfig.HostPort != "fullnode.invalid:443" || !fn.GRPCConfig.Insecure {
			t.Errorf("grpc_config = %+v", fn.GRPCConfig)
		}
		if !fn.LazyMode {
			t.Error("lazy_mode = false, want true")
		}
		if fn.SessionRolloverBlocks != 12 {
			t.Errorf("session_rollover_blocks = %d, want 12", fn.SessionRolloverBlocks)
		}
		if fn.CacheConfig.SessionTTL != 90*time.Second {
			t.Errorf("session_ttl = %v, want 90s", fn.CacheConfig.SessionTTL)
		}
	})

	t.Run("websocket", func(t *testing.T) {
		ws := cfg.WebSocket
		if ws.FrameObservationSampleRate != 0.02 {
			t.Errorf("frame_observation_sample_rate = %v, want 0.02", ws.FrameObservationSampleRate)
		}
		if ws.CloseObservationSampleRate != 0.75 {
			t.Errorf("close_observation_sample_rate = %v, want 0.75", ws.CloseObservationSampleRate)
		}
		if ws.EffectiveMaxConcurrentConnections() != 4096 {
			t.Errorf("max_concurrent_connections = %d, want 4096", ws.EffectiveMaxConcurrentConnections())
		}
	})

	t.Run("protocol", func(t *testing.T) {
		if cfg.Protocol.Type != ProtocolTypeShannon {
			t.Errorf("protocol type = %q, want shannon", cfg.Protocol.Type)
		}
		if cfg.Protocol.GRPCMode != "web" {
			t.Errorf("grpc_mode = %q, want web", cfg.Protocol.GRPCMode)
		}
		if cfg.Protocol.Mock.EndpointCount != 17 {
			t.Errorf("mock endpoint_count = %d, want 17", cfg.Protocol.Mock.EndpointCount)
		}
		if cfg.Protocol.Mock.Latency != 45*time.Millisecond {
			t.Errorf("mock latency = %v, want 45ms", cfg.Protocol.Mock.Latency)
		}
		if cfg.Protocol.Mock.ResponseBody == "" {
			t.Error("mock response_body is empty")
		}
	})
}

func TestConfigCompatibility_Gateway(t *testing.T) {
	cfg := loadFixture(t, fixturePath)
	g := cfg.Gateway

	t.Run("identity", func(t *testing.T) {
		if g.GatewayMode != "centralized" {
			t.Errorf("gateway_mode = %q, want centralized", g.GatewayMode)
		}
		if g.GatewayAddress == "" || g.GatewayPrivateKeyHex == "" {
			t.Error("gateway address/key did not parse")
		}
		if len(g.OwnedAppsPrivateKeys) != 2 {
			t.Errorf("owned_apps_private_keys_hex = %d entries, want 2", len(g.OwnedAppsPrivateKeys))
		}
	})

	t.Run("middleware_chain", func(t *testing.T) {
		want := "timeout,parse,retry,select_endpoint,send_relay"
		if got := strings.Join(g.EffectiveMiddlewareChain(), ","); got != want {
			t.Errorf("middleware_chain = %q, want %q", got, want)
		}
	})

	t.Run("reputation", func(t *testing.T) {
		rep := g.Reputation
		if !rep.Enabled || rep.StorageType != "redis" || rep.KeyGranularity != "per-supplier" {
			t.Errorf("reputation head = %+v", rep)
		}
		if rep.InitialScore != 100 || rep.MinThreshold != 5 || rep.RecoveryTimeout != 7*time.Minute {
			t.Errorf("reputation scores = %+v", rep)
		}

		if rep.ChronicHalfLifeAttempts != 15000 || rep.ChronicOnsetRate != 0.0005 || rep.ChronicFullRate != 0.02 {
			t.Errorf("chronic keys = %+v", rep)
		}

		want := TieredSelectionConfig{
			Enabled:        true,
			Tier1Threshold: 85,
			Tier2Threshold: 35,
			Probation: ProbationConfig{
				Enabled:            true,
				Threshold:          45,
				TrafficPercent:     25,
				RecoveryMultiplier: 4.5,
			},
		}
		if rep.TieredSelection != want {
			t.Errorf("tiered_selection = %+v, want %+v", rep.TieredSelection, want)
		}

		// The keys are not merely parsed: these are the numbers the selector
		// and the scorer are built from.
		wantSel := reputation.SelectorConfig{
			Tier1Threshold: 85, Tier2Threshold: 35, MinThreshold: 5,
			ProbationThreshold: 45, ProbationPct: 25,
		}
		if got := rep.SelectorConfig(); got != wantSel {
			t.Errorf("SelectorConfig() = %+v, want %+v", got, wantSel)
		}
		// The fixture's thresholds do not descend (probation 45 sits above
		// tier2 35), which is the shape a PATH config in production has. It
		// must load — loadFixture would have failed the test otherwise — and
		// it must say something, because tier 2 ends up narrower than it
		// reads.
		if len(cfg.Warnings) == 0 {
			t.Error("a PATH config whose thresholds do not descend loaded silently")
		}
		if !strings.Contains(strings.Join(cfg.Warnings, "\n"), "probation.threshold") {
			t.Errorf("warnings = %v, want one naming probation.threshold", cfg.Warnings)
		}

		wantRate := reputation.RateConfig{HalfLifeAttempts: 15000, OnsetRate: 0.0005, FullRate: 0.02}
		if got := rep.RateConfig(); got != wantRate {
			t.Errorf("RateConfig() = %+v, want %+v", got, wantRate)
		}

		wantImpacts := SignalImpactsConfig{
			Success: 2, MinorError: -2, MajorError: -4, CriticalError: -16,
			FatalError: -26, RecoverySuccess: 14, SlowResponse: -3, VerySlowResponse: -5,
		}
		if rep.SignalImpacts != wantImpacts {
			t.Errorf("signal_impacts = %+v, want %+v", rep.SignalImpacts, wantImpacts)
		}
	})

	t.Run("retry", func(t *testing.T) {
		want := RetryConfig{
			Enabled: true, MaxRetries: 3, MaxRetryLatency: 900 * time.Millisecond,
			RetryOn5xx: true, RetryOnTimeout: true, RetryOnConnection: true,
			ConnectTimeout: 250 * time.Millisecond, HedgeDelay: 300 * time.Millisecond,
			MaxLatency: 2 * time.Second,
		}
		if g.Retry != want {
			t.Errorf("retry_config = %+v, want %+v", g.Retry, want)
		}
		if !g.Retry.IsEnabled() || !g.Retry.HedgeEnabled() {
			t.Error("retry should report enabled and hedging")
		}
	})

	t.Run("health_checks_and_observation", func(t *testing.T) {
		if !g.HealthChecks.Enabled {
			t.Error("active_health_checks disabled")
		}
		if len(g.HealthChecks.Local) != 1 {
			t.Fatalf("local health check blocks = %d, want 1", len(g.HealthChecks.Local))
		}
		svc := g.HealthChecks.Local[0]
		if svc.ServiceID != "eth" || !svc.Enabled {
			t.Errorf("local block = %+v", svc)
		}
		if len(svc.Checks) != 1 {
			t.Fatalf("checks = %d, want 1", len(svc.Checks))
		}
		check := svc.Checks[0]
		if check.Name != "eth_blockNumber" || check.Method != "POST" || check.Path != "/" {
			t.Errorf("check = %+v", check)
		}
		if check.Type != "json_rpc" || check.ReputationSignal != "minor_error" {
			t.Errorf("check type/signal = %q %q", check.Type, check.ReputationSignal)
		}
		if check.Timeout != 5*time.Second || check.ExpectedStatusCode != 200 {
			t.Errorf("check timeout/status = %v %d", check.Timeout, check.ExpectedStatusCode)
		}

		want := ObservationPipelineConfig{Enabled: true, SampleRate: 0.25, WorkerCount: 8, QueueSize: 20000}
		if g.ObservationPipeline != want {
			t.Errorf("observation_pipeline = %+v, want %+v", g.ObservationPipeline, want)
		}
	})

	t.Run("latency_profiles", func(t *testing.T) {
		for _, name := range []string{"fast", "medium", "slow"} {
			if _, ok := g.LatencyProfiles[name]; !ok {
				t.Errorf("latency profile %q missing (have %v)", name, profileNames(g.LatencyProfiles))
			}
		}
		want := LatencyProfile{
			FastThreshold: 55 * time.Millisecond, NormalThreshold: 210 * time.Millisecond,
			SlowThreshold: 510 * time.Millisecond, PenaltyThreshold: 1010 * time.Millisecond,
			SevereThreshold: 3010 * time.Millisecond,
			FastBonus:       3, SlowPenalty: 2, VerySlowPenalty: 1,
		}
		if g.LatencyProfiles["fast"] != want {
			t.Errorf("profile fast = %+v, want %+v", g.LatencyProfiles["fast"], want)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		d := g.EffectiveDefaults()
		if d.Timeout.RelayTimeout != 15*time.Second {
			t.Errorf("defaults relay_timeout = %v, want 15s", d.Timeout.RelayTimeout)
		}
		if d.Retry.MaxRetries != 2 || d.Retry.HedgeDelay != 400*time.Millisecond {
			t.Errorf("defaults retry = %+v", d.Retry)
		}
		if d.Reputation.StorageType != "memory" || d.Reputation.InitialScore != 90 {
			t.Errorf("defaults reputation = %+v", d.Reputation)
		}
	})
}

func TestConfigCompatibility_Services(t *testing.T) {
	cfg := loadFixture(t, fixturePath)

	if got := len(cfg.Gateway.AllServices()); got != 5 {
		t.Errorf("service count = %d, want 5", got)
	}

	t.Run("types_and_rpc_types", func(t *testing.T) {
		tests := []struct {
			id           string
			wantType     string
			wantRPCTypes []string
		}{
			{"eth", "evm", []string{"json_rpc", "websocket"}},
			{"poly", "evm", []string{"json_rpc", "websocket"}},
			{"solana", "solana", []string{"json_rpc"}},
			{"akash", "cosmos", []string{"json_rpc", "rest", "comet_bft"}},
			{"near", "generic", []string{"json_rpc"}},
		}

		for _, tt := range tests {
			t.Run(tt.id, func(t *testing.T) {
				svc := cfg.Gateway.GetServiceConfig(tt.id)
				if svc == nil {
					t.Fatalf("service %q not found", tt.id)
				}
				if svc.Type != tt.wantType {
					t.Errorf("type = %q, want %q", svc.Type, tt.wantType)
				}
				if strings.Join(svc.RPCTypes, ",") != strings.Join(tt.wantRPCTypes, ",") {
					t.Errorf("rpc_types = %v, want %v", svc.RPCTypes, tt.wantRPCTypes)
				}
			})
		}
	})

	t.Run("sync_allowance", func(t *testing.T) {
		tests := []struct {
			id   string
			want uint64
		}{
			{"eth", 25}, {"poly", 150}, {"solana", 1500}, {"akash", 30},
		}
		for _, tt := range tests {
			svc := cfg.Gateway.GetServiceConfig(tt.id)
			if svc == nil {
				t.Fatalf("service %q not found", tt.id)
			}
			if svc.SyncAllowance != tt.want {
				t.Errorf("%s: sync_allowance = %d, want %d", tt.id, svc.SyncAllowance, tt.want)
			}
		}
	})

	// chain_id is opaque to config on purpose — the QoS plugin owns its format.
	// All config has to do is carry it through unaltered, hex and name alike.
	t.Run("chain_id_is_carried_verbatim", func(t *testing.T) {
		if got := cfg.Gateway.GetServiceConfig("eth").ChainID; got != "0x1" {
			t.Errorf("eth chain_id = %q, want 0x1", got)
		}
		if got := cfg.Gateway.GetServiceConfig("akash").ChainID; got != "akashnet-2" {
			t.Errorf("akash chain_id = %q, want akashnet-2", got)
		}
		if got := cfg.Gateway.GetServiceConfig("poly").ChainID; got != "" {
			t.Errorf("poly chain_id = %q, want empty", got)
		}
	})

	t.Run("per_service_overrides", func(t *testing.T) {
		eth := cfg.Gateway.GetServiceConfig("eth")
		defaults := cfg.Gateway.EffectiveDefaults()

		if got := eth.EffectiveTimeout(defaults).RelayTimeout; got != 12*time.Second {
			t.Errorf("eth relay_timeout = %v, want 12s (its own, not the default)", got)
		}
		if got := eth.EffectiveRetry(defaults).MaxRetries; got != 4 {
			t.Errorf("eth max_retries = %d, want 4", got)
		}

		// poly sets neither, so both fall back.
		poly := cfg.Gateway.GetServiceConfig("poly")
		if got := poly.EffectiveTimeout(defaults).RelayTimeout; got != 15*time.Second {
			t.Errorf("poly relay_timeout = %v, want the default 15s", got)
		}
		if got := poly.EffectiveRetry(defaults).MaxRetries; got != 2 {
			t.Errorf("poly max_retries = %d, want the default 2", got)
		}
	})

	t.Run("external_block_sources", func(t *testing.T) {
		eth := cfg.Gateway.GetServiceConfig("eth")
		if len(eth.ExternalBlockSources) != 1 {
			t.Fatalf("eth external_block_sources = %d, want 1", len(eth.ExternalBlockSources))
		}
		want := ExternalBlockSource{
			URL: "https://eth-rpc.invalid", Type: "evm", Method: "eth_blockNumber", Path: "/",
			Interval: 20 * time.Second, Timeout: 6 * time.Second, GracePeriod: 45 * time.Second,
		}
		if eth.ExternalBlockSources[0] != want {
			t.Errorf("external_block_source = %+v, want %+v", eth.ExternalBlockSources[0], want)
		}
	})
}

// Config carries only the flags an operator set. What matters for compatibility
// is that every flag a PATH config names is one SAGE implements — an unknown one
// parses fine and then does nothing at all.
func TestConfigCompatibility_FeatureFlagsAreKnown(t *testing.T) {
	cfg := loadFixture(t, fixturePath)

	if len(cfg.FeatureFlags) == 0 {
		t.Fatal("fixture set no feature flags, so this proves nothing")
	}
	for name := range cfg.FeatureFlags {
		if !featureflag.IsKnownFlag(name) {
			t.Errorf("config sets feature flag %q that SAGE does not implement", name)
		}
	}
	if enabled, ok := cfg.FeatureFlags["hedge"]; !ok || enabled {
		t.Error("hedge: false did not survive parsing — an explicit false must not read as unset")
	}
}

// The fixture must name only fields SAGE implements. If it grows a typo, or a
// field is removed from the config structs while the YAML keeps setting it, the
// key lands in Ignored — which is how an operator's key silently does nothing.
func TestConfigFixturesHaveNoIgnoredKeys(t *testing.T) {
	for _, path := range []string{fixturePath, unifiedFixturePath} {
		cfg := loadFixture(t, path)
		if len(cfg.Ignored) > 0 {
			t.Errorf("%s: keys SAGE does not implement: %v", path, cfg.Ignored)
		}
	}
}

// The newer PATH shape nests services and defaults under unified_services.
// GatewayConfig reads both, so a fixture for each is the only thing separating
// "supported" from "parses into a field nothing reads".
func TestConfigCompatibility_UnifiedServicesFormat(t *testing.T) {
	cfg := loadFixture(t, unifiedFixturePath)
	g := cfg.Gateway

	if len(g.Services) != 0 {
		t.Fatal("fixture should define services only under unified_services")
	}

	services := g.AllServices()
	if len(services) != 2 {
		t.Fatalf("AllServices = %d, want 2 from unified_services", len(services))
	}
	if g.GetServiceConfig("unified-osmosis").ChainID != "osmosis-1" {
		t.Error("unified service chain_id did not parse")
	}

	d := g.EffectiveDefaults()
	if d.Retry.MaxRetries != 5 || d.Timeout.RelayTimeout != 17*time.Second {
		t.Errorf("unified defaults = %+v", d)
	}

	want := "timeout,parse,send_relay"
	if got := strings.Join(g.EffectiveMiddlewareChain(), ","); got != want {
		t.Errorf("unified middleware_chain = %q, want %q", got, want)
	}

	if _, ok := g.UnifiedServices.LatencyProfiles["unified_fast"]; !ok {
		t.Error("unified_services.latency_profiles did not parse")
	}
}

// A synthetic fixture is only as good as its coverage, and coverage is exactly
// what rots silently: add a field to config/ and the fixture keeps passing while
// testing nothing about it. So the fixture is not maintained by hand — this
// walks the config structs and fails if any yaml key is absent from the corpus.
//
// Presence, not correctness: it cannot tell that a value is asserted anywhere.
// It only guarantees a new field cannot be added without someone touching these
// files, which is the point at which they will notice.
func TestConfigFixtureIsExhaustive(t *testing.T) {
	corpus := ""
	for _, path := range []string{fixturePath, unifiedFixturePath} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		corpus += string(data)
	}

	var missing []string
	for _, tag := range yamlTags(reflect.TypeOf(Config{}), map[reflect.Type]bool{}) {
		if !strings.Contains(corpus, tag+":") {
			missing = append(missing, tag)
		}
	}

	if len(missing) > 0 {
		t.Errorf("config fields absent from the fixtures: %v\n"+
			"add them to config/testdata/ so they are actually parsed by a test", missing)
	}
}

// yamlTags collects every yaml key reachable from t, descending through nested
// structs, slices and maps. seen guards against a type reachable from itself.
func yamlTags(t reflect.Type, seen map[reflect.Type]bool) []string {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || seen[t] {
		return nil
	}
	seen[t] = true

	// time.Duration and friends are leaves, not structures to descend into.
	if t == reflect.TypeOf(time.Time{}) {
		return nil
	}

	var tags []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		tags = append(tags, tag)
		tags = append(tags, yamlTags(f.Type, seen)...)
	}
	return tags
}

// profileNames returns the keys of a latency profile map for diagnostic output.
func profileNames(m map[string]LatencyProfile) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return names
}
