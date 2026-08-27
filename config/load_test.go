package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pokt-network/sage/reputation"
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

// fullNodePreamble is the minimum a Shannon config needs to reach validate at
// all. parseString prepends it so a case can be written as only the block it is
// actually about.
const fullNodePreamble = `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
    insecure: true
`

// parseString runs a YAML fragment through the real load path — decode,
// ignored/inert reporting, defaults and validation — by writing it to a temp
// file and calling LoadFromFile. Anything asserted about it is therefore
// asserted about what a gateway would actually boot with.
func parseString(t *testing.T, yaml string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(fullNodePreamble+yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return LoadFromFile(path)
}

// mustParse is parseString for a config that is expected to load.
func mustParse(t *testing.T, yaml string) *Config {
	t.Helper()
	cfg, err := parseString(t, yaml)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return cfg
}

// The scoring keys PATH ships are honoured now, not merely parsed. Each
// assertion below names a value nothing in SAGE reads unless the accessors
// thread it through.
func TestReputationConfig_ScoringKeysAreLive(t *testing.T) {
	cfg := mustParse(t, `
gateway_config:
  gateway_mode: centralized
  reputation_config:
    min_threshold: 15
    chronic_half_life_attempts: 5000
    chronic_onset_rate: 0.001
    chronic_full_rate: 0.05
    tiered_selection:
      tier1_threshold: 90
      tier2_threshold: 60
      probation:
        threshold: 25
        traffic_percent: 5
    signal_impacts:
      success: 2
      critical_error: -40
`)

	sel := cfg.Gateway.Reputation.SelectorConfig()
	if sel.Tier1Threshold != 90 {
		t.Errorf("tier1 = %v, want 90", sel.Tier1Threshold)
	}
	if sel.Tier2Threshold != 60 {
		t.Errorf("tier2 = %v, want 60", sel.Tier2Threshold)
	}
	if sel.MinThreshold != 15 {
		t.Errorf("min = %v, want 15", sel.MinThreshold)
	}
	if sel.ProbationThreshold != 25 {
		t.Errorf("probation threshold = %v, want 25", sel.ProbationThreshold)
	}
	if sel.ProbationPct != 5 {
		t.Errorf("probation pct = %d, want 5", sel.ProbationPct)
	}

	imp := cfg.Gateway.Reputation.Impacts().Normalized()
	if imp.Success != 2 {
		t.Errorf("success impact = %d, want 2", imp.Success)
	}
	if imp.CriticalError != -40 {
		t.Errorf("critical impact = %d, want -40", imp.CriticalError)
	}
	if imp.MinorError != -3 {
		t.Errorf("minor impact = %d, want the default -3 when unset", imp.MinorError)
	}

	rc := cfg.Gateway.Reputation.RateConfig()
	if rc.HalfLifeAttempts != 5000 {
		t.Errorf("half life = %d, want 5000", rc.HalfLifeAttempts)
	}
	if rc.OnsetRate != 0.001 {
		t.Errorf("onset = %v, want 0.001", rc.OnsetRate)
	}
	if rc.FullRate != 0.05 {
		t.Errorf("full = %v, want 0.05", rc.FullRate)
	}

	// A key that is honoured must no longer be reported as doing nothing.
	joined := strings.Join(cfg.Inert, "\n")
	for _, gone := range []string{"signal_impacts", "tiered_selection", "min_threshold"} {
		if strings.Contains(joined, gone) {
			t.Errorf("%q is still reported as inert:\n%s", gone, joined)
		}
	}
}

func TestReputationConfig_UnsetSelectorIsDefault(t *testing.T) {
	cfg := mustParse(t, "gateway_config:\n  gateway_mode: centralized\n")

	if got, want := cfg.Gateway.Reputation.SelectorConfig(), reputation.DefaultSelectorConfig(); got != want {
		t.Errorf("selector = %+v, want the package default %+v", got, want)
	}
}

// The leaves that stay inert stay reported. Going live on the tuning keys is
// only safe if the ones SAGE still ignores keep saying so.
func TestReputationConfig_InertLeavesStillReported(t *testing.T) {
	cfg := mustParse(t, `
gateway_config:
  gateway_mode: centralized
  reputation_config:
    recovery_timeout: 5m
    tiered_selection:
      enabled: true
      probation:
        recovery_multiplier: 1.5
    signal_impacts:
      slow_response: -1
      recovery_success: 3
`)

	joined := strings.Join(cfg.Inert, "\n")
	for _, want := range []string{
		"recovery_timeout",
		"tiered_selection.enabled",
		"probation.recovery_multiplier",
		"signal_impacts.slow_response",
		"signal_impacts.recovery_success",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected %q in the inert report, got:\n%s", want, joined)
		}
	}
}

// Only a value that describes no behaviour at all is refused. A share of 101%
// and a chronic onset at or above the full rate are arithmetic that cannot be
// carried out; thresholds that fail to descend are merely lopsided, and the
// beta config we run in production has them.
func TestReputationConfig_Validation(t *testing.T) {
	cases := map[string]struct {
		body string
		want string
	}{
		"traffic_percent": {
			body: "tiered_selection:\n      probation:\n        traffic_percent: 101\n",
			want: "traffic_percent",
		},
		"onset >= full": {
			body: "chronic_onset_rate: 0.02\n    chronic_full_rate: 0.01\n",
			want: "chronic_onset_rate",
		},
		"full >= 1": {
			body: "chronic_full_rate: 1.5\n",
			want: "chronic_full_rate",
		},
		"negative onset": {
			body: "chronic_onset_rate: -0.01\n",
			want: "chronic_onset_rate",
		},
		// Scores run 0..100, so a negative threshold names no band. It must
		// not fall through to SelectorConfig, which copies only positive
		// values and would quietly hand back the default 80.
		"negative tier1": {
			body: "tiered_selection:\n      tier1_threshold: -1\n",
			want: "tier1_threshold",
		},
		"negative min_threshold": {
			body: "min_threshold: -5\n",
			want: "min_threshold",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseString(t, "gateway_config:\n  gateway_mode: centralized\n  reputation_config:\n    "+tc.body)
			if err == nil {
				t.Fatalf("expected a validation error for %s", name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

// Thresholds that do not descend load, and say what they will actually do.
//
// SAGE must load a PATH config unmodified, and the beta config we run has
// tier2_threshold 30 with probation.threshold 50 — PATH accepts it and the
// selector copes, classifying probation before tier 2 so a band simply ends up
// empty. Refusing to boot on that would break compatibility over something
// that works; saying nothing would leave an operator believing in a tier that
// never gets an endpoint.
func TestReputationConfig_ThresholdOrderIsAWarningNotAnError(t *testing.T) {
	cases := map[string]struct {
		body string
		want string
	}{
		"tier2 >= tier1": {
			body: "tiered_selection:\n      tier1_threshold: 60\n      tier2_threshold: 70\n",
			want: "tier2_threshold",
		},
		"probation >= tier2": {
			body: "tiered_selection:\n      tier2_threshold: 50\n      probation:\n        threshold: 55\n",
			want: "probation.threshold",
		},
		"min above probation": {
			body: "min_threshold: 40\n    tiered_selection:\n      probation:\n        threshold: 30\n",
			want: "min_threshold",
		},
		"the beta config's own shape": {
			body: "tiered_selection:\n      tier2_threshold: 30\n      probation:\n        threshold: 50\n",
			want: "probation.threshold",
		},
		"probation above tier1": {
			body: "tiered_selection:\n      tier1_threshold: 60\n      tier2_threshold: 50\n      probation:\n        threshold: 70\n",
			want: "probation.threshold",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := mustParse(t, "gateway_config:\n  gateway_mode: centralized\n  reputation_config:\n    "+tc.body)
			// Exactly one: a rule that fires on a config it has nothing to say
			// about turns the whole channel into noise, and a second sentence
			// about the same numbers would be a rule contradicting itself.
			if len(cfg.Warnings) != 1 {
				t.Fatalf("%s produced %d warnings, want exactly 1: %v", name, len(cfg.Warnings), cfg.Warnings)
			}
			if !strings.Contains(cfg.Warnings[0], tc.want) {
				t.Errorf("warning %q does not name %q", cfg.Warnings[0], tc.want)
			}
		})
	}
}

// The warning has to answer the question the operator actually has — what
// happens to my traffic — not just restate the inequality they broke.
func TestReputationConfig_WarningSaysWhatTheSelectorWillDo(t *testing.T) {
	cfg := mustParse(t, `
gateway_config:
  gateway_mode: centralized
  reputation_config:
    tiered_selection:
      tier2_threshold: 30
      probation:
        threshold: 50
`)

	joined := strings.Join(cfg.Warnings, "\n")
	for _, want := range []string{"(50)", "(30)", "probation", "tier 2 holds only scores from 50 up to tier1_threshold (80)"} {
		if !strings.Contains(joined, want) {
			t.Errorf("warning %q does not mention %q", joined, want)
		}
	}
}

// A probation threshold above tier 1 leaves tier 2 with nothing, and the
// sentence has to say that rather than describe a band that runs backwards
// ("from 70 up to tier1_threshold (60)") — which would also contradict the
// tier-2-is-empty sentence whenever both rules fire.
func TestReputationConfig_WarningDoesNotDescribeABackwardsBand(t *testing.T) {
	cfg := mustParse(t, `
gateway_config:
  gateway_mode: centralized
  reputation_config:
    tiered_selection:
      tier1_threshold: 60
      tier2_threshold: 50
      probation:
        threshold: 70
`)

	joined := strings.Join(cfg.Warnings, "\n")
	if !strings.Contains(joined, "tier 2 is empty") {
		t.Errorf("warning %q does not say tier 2 is empty", joined)
	}
	if strings.Contains(joined, "up to tier1_threshold") {
		t.Errorf("warning %q describes a band that runs backwards", joined)
	}
}

// The coherent config the cases above are variations on must load and say
// nothing, so a warning there is the rule firing and not the fixture.
func TestReputationConfig_CoherentThresholdsWarnAboutNothing(t *testing.T) {
	cfg := mustParse(t, `
gateway_config:
  gateway_mode: centralized
  reputation_config:
    min_threshold: 15
    chronic_onset_rate: 0.001
    chronic_full_rate: 0.05
    tiered_selection:
      tier1_threshold: 90
      tier2_threshold: 60
      probation:
        threshold: 25
        traffic_percent: 5
`)

	if len(cfg.Warnings) != 0 {
		t.Errorf("warnings = %v, want none for a config whose thresholds descend", cfg.Warnings)
	}
}

// An omitted block takes the package defaults, which descend. Nothing to warn
// about is the common case and must stay silent, or the warning becomes noise
// every operator learns to skip.
func TestReputationConfig_DefaultsWarnAboutNothing(t *testing.T) {
	cfg := mustParse(t, "gateway_config:\n  gateway_mode: centralized\n")

	if len(cfg.Warnings) != 0 {
		t.Errorf("warnings = %v, want none for a config with no reputation block", cfg.Warnings)
	}
}

// A reputation block under gateway_config.defaults parses in full and is read
// by nothing — reputation is configured from gateway_config.reputation_config
// only, because one selector and one scorer serve every service. That is the
// quiet half of PATH compatibility, so it has to be reported, and the value
// written there must not reach the selector.
func TestReputationConfig_DefaultsBlockIsInert(t *testing.T) {
	cfg := mustParse(t, `
gateway_config:
  gateway_mode: centralized
  defaults:
    reputation_config:
      tiered_selection:
        tier1_threshold: 90
`)

	joined := strings.Join(cfg.Inert, "\n")
	if !strings.Contains(joined, "gateway_config.defaults.reputation_config") {
		t.Errorf("inert = %v, want the defaults reputation block reported", cfg.Inert)
	}
	// Reported once, as a block: the keys inside it share the one reason.
	if len(cfg.Inert) != 1 {
		t.Errorf("inert = %v, want exactly one report", cfg.Inert)
	}
	if got := cfg.Gateway.Reputation.SelectorConfig().Tier1Threshold; got != 80 {
		t.Errorf("tier1 = %v, want the default 80 — a threshold under defaults reached the selector", got)
	}
}
