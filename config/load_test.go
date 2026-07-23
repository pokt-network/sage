package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFromFile_MinimalConfig(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
    insecure: true
gateway_config:
  gateway_mode: centralized
  unified_services:
    services:
      - id: eth
        type: evm
        rpc_types: [json_rpc, websocket]
        sync_allowance: 5
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Verify parsed values
	if cfg.FullNode.RPCURL != "http://localhost:26657" {
		t.Errorf("RPCURL = %q", cfg.FullNode.RPCURL)
	}
	if cfg.Gateway.GatewayMode != "centralized" {
		t.Errorf("GatewayMode = %q", cfg.Gateway.GatewayMode)
	}
	if len(cfg.Gateway.AllServices()) != 1 {
		t.Fatalf("Services count = %d", len(cfg.Gateway.AllServices()))
	}
	svc := cfg.Gateway.AllServices()[0]
	if svc.ID != "eth" {
		t.Errorf("Service ID = %q", svc.ID)
	}
	if svc.Type != "evm" {
		t.Errorf("Service Type = %q", svc.Type)
	}
	if svc.SyncAllowance != 5 {
		t.Errorf("SyncAllowance = %d", svc.SyncAllowance)
	}

	// Verify defaults applied
	if cfg.Router.Port != 3069 {
		t.Errorf("Router.Port = %d, want 3069", cfg.Router.Port)
	}
	if cfg.Logger.Level != "info" {
		t.Errorf("Logger.Level = %q, want info", cfg.Logger.Level)
	}

	// Feature flags carry only what YAML set. This config sets none, so the map
	// is empty — the defaults live in featureflag.DefaultFlags and are applied by
	// the store, not here. (An omitted section must not materialise as a struct
	// of falses, which is what used to drag every unset flag to disabled.)
	if len(cfg.FeatureFlags) != 0 {
		t.Errorf("FeatureFlags = %v, want empty when the section is omitted", cfg.FeatureFlags)
	}
}

func TestLoadFromFile_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			"missing rpc_url",
			`
full_node_config:
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
`,
		},
		{
			"missing grpc host_port",
			`
full_node_config:
  rpc_url: http://localhost:26657
gateway_config:
  gateway_mode: centralized
`,
		},
		{
			"missing gateway_mode",
			`
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(tt.yaml), 0644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadFromFile(path)
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestRetryConfig(t *testing.T) {
	r := RetryConfig{MaxRetries: 3, HedgeDelay: 100}
	if !r.IsEnabled() {
		t.Error("should be enabled")
	}
	if !r.HedgeEnabled() {
		t.Error("hedge should be enabled")
	}

	r2 := RetryConfig{}
	if r2.IsEnabled() {
		t.Error("zero value should be disabled")
	}
	if r2.HedgeEnabled() {
		t.Error("zero value hedge should be disabled")
	}
}

func TestServiceConfig_EffectiveRetry(t *testing.T) {
	defaults := ServiceDefaults{
		Retry: RetryConfig{MaxRetries: 3, HedgeDelay: 100, RetryOn5xx: true},
	}

	svc := ServiceConfig{
		Retry: RetryConfig{MaxRetries: 5}, // override max retries, inherit rest
	}

	effective := svc.EffectiveRetry(defaults)
	if effective.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", effective.MaxRetries)
	}
	if effective.HedgeDelay != 100 {
		t.Errorf("HedgeDelay = %d, want 100 (from defaults)", effective.HedgeDelay)
	}
	if !effective.RetryOn5xx {
		t.Error("RetryOn5xx should be inherited from defaults")
	}
}

func TestGetServiceConfig(t *testing.T) {
	g := GatewayConfig{
		Services: []ServiceConfig{
			{ID: "eth", Type: "evm"},
			{ID: "poly", Type: "evm"},
		},
	}

	svc := g.GetServiceConfig("poly")
	if svc == nil || svc.ID != "poly" {
		t.Error("should find poly")
	}
	if g.GetServiceConfig("missing") != nil {
		t.Error("should return nil for missing service")
	}
}

// pprof serves heap dumps — which hold signing keys — with no authentication.
// Defaulting it on, on every interface, made the "is it enabled" guard in
// main.go dead code that could not evaluate false.
func TestApplyDefaults_PprofIsOffUnlessConfigured(t *testing.T) {
	var cfg Config
	applyDefaults(&cfg)
	if cfg.Metrics.PprofAddr != "" {
		t.Errorf("PprofAddr defaulted to %q, want empty (disabled)", cfg.Metrics.PprofAddr)
	}
}

// The loader is lenient on purpose — SAGE must load a PATH config unmodified —
// but leniency was silent, and a key that parses and does nothing reads to
// whoever wrote it as a feature that is on. The real case: PATH configs carry
// active_health_checks.local rules and a coordination block, both of which SAGE
// has no field for and quietly discarded.
func TestLoadFromFile_ReportsIgnoredKeys(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
    insecure: true
gateway_config:
  gateway_mode: centralized
  active_health_checks:
    enabled: true
    coordination:
      type: leader_election
      lease_duration: 15s
    external:
      url: ""
      refresh_interval: 5m
    local:
      - service_id: eth
        check_interval: 10s
  unified_services:
    services:
      - id: eth
        type: evm
        rpc_types: [json_rpc]
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("an unimplemented key must not fail the load: %v", err)
	}

	// The config still loads correctly — leniency is the point.
	if cfg.Gateway.GatewayMode != "centralized" {
		t.Errorf("GatewayMode = %q, want centralized", cfg.Gateway.GatewayMode)
	}

	joined := strings.Join(cfg.Ignored, "\n")
	// "local" is deliberately absent from this list: configured health checks
	// are implemented, so reporting them would be the false alarm this test
	// guards against. "external" replaced it — it declared a rules URL nothing
	// ever fetched, and was retired rather than left parsing into a dead field.
	for _, want := range []string{"coordination", "external", "check_interval"} {
		if !strings.Contains(joined, want) {
			t.Errorf("ignored keys must mention %q; got:\n%s", want, joined)
		}
	}
	// The message has to say where, or an operator cannot act on it.
	if !strings.Contains(joined, "line ") {
		t.Errorf("ignored keys should carry a line number; got:\n%s", joined)
	}
	// Implemented keys must never be reported.
	for _, unwanted := range []string{"gateway_mode", "rpc_url", "unified_services", "service_id"} {
		if strings.Contains(joined, unwanted) {
			t.Errorf("%q is implemented and must not be reported ignored; got:\n%s", unwanted, joined)
		}
	}
}

// A config SAGE fully understands must report nothing, or the warning becomes
// noise that gets tuned out.
func TestLoadFromFile_CleanConfigReportsNothing(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
    insecure: true
gateway_config:
  gateway_mode: centralized
  unified_services:
    services:
      - id: eth
        type: evm
        rpc_types: [json_rpc]
        chain_id: "0x1"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Ignored) != 0 {
		t.Errorf("clean config reported ignored keys: %v", cfg.Ignored)
	}
}

// TestMiddlewareChain_ParsesAtBothLevels covers the two places the chain may be
// declared. unified_services.middleware_chain is where the field was first
// declared and must keep working; gateway_config.middleware_chain is the level
// the production services format can reach.
func TestMiddlewareChain_ParsesAtBothLevels(t *testing.T) {
	base := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
`
	tests := []struct {
		name string
		yaml string
		want []string
	}{
		{
			name: "unset takes the default",
			yaml: base,
			want: nil,
		},
		{
			name: "gateway level",
			yaml: base + "  middleware_chain: [parse, send_relay]\n",
			want: []string{"parse", "send_relay"},
		},
		{
			name: "unified_services level",
			yaml: base + "  unified_services:\n    middleware_chain: [parse, cache, send_relay]\n",
			want: []string{"parse", "cache", "send_relay"},
		},
		{
			name: "gateway level wins over unified_services",
			yaml: base + "  middleware_chain: [parse, send_relay]\n" +
				"  unified_services:\n    middleware_chain: [parse, cache, send_relay]\n",
			want: []string{"parse", "send_relay"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(tt.yaml), 0644); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadFromFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got := cfg.Gateway.EffectiveMiddlewareChain()
			if len(got) != len(tt.want) {
				t.Fatalf("EffectiveMiddlewareChain() = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("EffectiveMiddlewareChain()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestMiddlewareChain_IsNotReportedAsIgnored guards the property that made this
// field worth wiring: it used to parse into a struct nothing read, which is
// exactly the silence Config.Ignored exists to break. Now that it is read, it
// must not be reported — and must not regress to being reported either.
func TestMiddlewareChain_IsNotReportedAsIgnored(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
  middleware_chain: [parse, send_relay]
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, ig := range cfg.Ignored {
		if strings.Contains(ig, "middleware_chain") {
			t.Errorf("middleware_chain reported as ignored: %q", ig)
		}
	}
}

// TestAdminAddr_DefaultsToLoopback pins the security-relevant default. The admin
// API has no authentication, so "unconfigured" has to mean "not reachable from
// off this host". A bare ":9091" here would publish an unauthenticated control
// plane on every interface of every deployment that never set the key.
func TestAdminAddr_DefaultsToLoopback(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Admin.Addr != DefaultAdminAddr {
		t.Errorf("Admin.Addr = %q, want %q", cfg.Admin.Addr, DefaultAdminAddr)
	}
	if !strings.HasPrefix(cfg.Admin.Addr, "localhost") && !strings.HasPrefix(cfg.Admin.Addr, "127.") {
		t.Errorf("Admin.Addr = %q, want a loopback address by default", cfg.Admin.Addr)
	}
}

func TestAdminAddr_ExplicitIsKept(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
admin_config:
  addr: "127.0.0.1:7777"
gateway_config:
  gateway_mode: centralized
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Admin.Addr != "127.0.0.1:7777" {
		t.Errorf("Admin.Addr = %q, want the configured value", cfg.Admin.Addr)
	}
}

// TestFeatureFlags_PartialMap locks the contract that fixes the all-or-nothing
// bug: setting one flag in YAML yields a map with exactly that key, not a struct
// where every unset flag has become false. Defaulting happens in the store,
// against featureflag.DefaultFlags, for whatever this map leaves out.
func TestFeatureFlags_PartialMap(t *testing.T) {
	yaml := `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
feature_flags:
  shadow_mode: true
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.FeatureFlags) != 1 {
		t.Fatalf("FeatureFlags = %v, want exactly the one flag that was set", cfg.FeatureFlags)
	}
	if !cfg.FeatureFlags["shadow_mode"] {
		t.Errorf("FeatureFlags[shadow_mode] = false, want true")
	}
	// The un-set flags must be ABSENT, not present-and-false — absent is what
	// lets the store fall back to their real defaults.
	if _, present := cfg.FeatureFlags["retry"]; present {
		t.Error("retry should be absent from the map, not materialised as false")
	}
}

// Defaults used to be applied all-or-nothing: setting any single WebSocket
// field skipped the whole block, so tuning one knob silently zeroed the others
// and took WS observability with it. Each field now resolves on its own.
func TestWebSocketConfig_PartialConfigKeepsOtherDefaults(t *testing.T) {
	// Only the connection cap is set — the case an operator is most likely to
	// hit, since it is the one knob with an obvious reason to change.
	cfg := WebSocketConfig{MaxConcurrentConnections: 4096}

	if got := cfg.EffectiveMaxConcurrentConnections(); got != 4096 {
		t.Errorf("max connections = %d, want the configured 4096", got)
	}
	if got := cfg.EffectiveFrameObservationSampleRate(); got != DefaultFrameObservationSampleRate {
		t.Errorf("frame sample rate = %v, want the default %v", got, DefaultFrameObservationSampleRate)
	}
	if got := cfg.EffectiveCloseObservationSampleRate(); got != DefaultCloseObservationSampleRate {
		t.Errorf("close sample rate = %v, want the default %v", got, DefaultCloseObservationSampleRate)
	}
}

// Zero cannot mean "off" — that is the value an unset field already has — so
// turning sampling off has to be asked for explicitly.
func TestWebSocketConfig_NegativeDisablesSampling(t *testing.T) {
	cfg := WebSocketConfig{
		FrameObservationSampleRate: -1,
		CloseObservationSampleRate: -1,
	}
	if got := cfg.EffectiveFrameObservationSampleRate(); got != 0 {
		t.Errorf("frame sample rate = %v, want 0", got)
	}
	if got := cfg.EffectiveCloseObservationSampleRate(); got != 0 {
		t.Errorf("close sample rate = %v, want 0", got)
	}
}

func TestWebSocketConfig_ExplicitValuesAreKept(t *testing.T) {
	cfg := WebSocketConfig{
		FrameObservationSampleRate: 0.5,
		CloseObservationSampleRate: 0.25,
	}
	if got := cfg.EffectiveFrameObservationSampleRate(); got != 0.5 {
		t.Errorf("frame sample rate = %v, want 0.5", got)
	}
	if got := cfg.EffectiveCloseObservationSampleRate(); got != 0.25 {
		t.Errorf("close sample rate = %v, want 0.25", got)
	}
}
