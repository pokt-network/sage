package config

import (
	"strings"
	"testing"
	"time"
)

// A minimal Shannon config the loader accepts, with the retry blocks under test.
func loadRetry(t *testing.T, retryYAML string) *Config {
	t.Helper()
	cfg, err := parse([]byte(`
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
` + retryYAML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cfg
}

func TestRetryEnabledFalse_TurnsRetriesOffAndSaysSo(t *testing.T) {
	cfg := loadRetry(t, `
  retry_config:
    enabled: false
    max_retries: 3
    hedge_delay: 100ms
`)
	eff := cfg.Gateway.EffectiveDefaults().Retry
	if eff.IsEnabled() || eff.MaxRetries != 0 {
		t.Fatalf("retries still on: %+v; PATH honours enabled: false and so must this", eff)
	}
	if eff.HedgeDelay != 100*time.Millisecond {
		t.Errorf("hedge_delay = %v; enabled: false governs retries, not hedging", eff.HedgeDelay)
	}
	if !strings.Contains(strings.Join(cfg.Warnings, "\n"), "gateway_config.retry_config.enabled is false") {
		t.Errorf("no warning says the switch was honoured: %v", cfg.Warnings)
	}
}

func TestRetryEnabledAbsent_MeansOnWhenMaxRetriesSet(t *testing.T) {
	cfg := loadRetry(t, `
  retry_config:
    max_retries: 2
`)
	if !cfg.Gateway.EffectiveDefaults().Retry.IsEnabled() {
		t.Fatal("an absent enabled key means on, as on PATH")
	}
	if len(cfg.Warnings) != 0 {
		t.Errorf("unexpected warnings: %v", cfg.Warnings)
	}
}

func TestRetryEnabledFalse_PerServiceSticksOverDefaults(t *testing.T) {
	cfg := loadRetry(t, `
  retry_config:
    max_retries: 3
  services:
    - id: eth
      retry_config:
        enabled: false
    - id: poly
`)
	defaults := cfg.Gateway.EffectiveDefaults()
	eth := cfg.Gateway.GetServiceConfig("eth").EffectiveRetry(defaults)
	if eth.IsEnabled() {
		t.Fatalf("eth: %+v; a service that turned retries off must not inherit the defaults' count", eth)
	}
	poly := cfg.Gateway.GetServiceConfig("poly").EffectiveRetry(defaults)
	if poly.MaxRetries != 3 {
		t.Errorf("poly: max_retries = %d, want 3 from defaults", poly.MaxRetries)
	}
}

// The three retry sources merge field by field. A production config with a
// hedge delay and no max_retries at the gateway level used to lose the delay.
func TestEffectiveDefaults_MergesRetryFieldByField(t *testing.T) {
	cfg := loadRetry(t, `
  retry_config:
    hedge_delay: 100ms
  defaults:
    retry_config:
      max_retries: 2
      max_latency: 2s
    timeout_config:
      relay_timeout: 9s
`)
	eff := cfg.Gateway.EffectiveDefaults()
	if eff.Retry.HedgeDelay != 100*time.Millisecond || eff.Retry.MaxRetries != 2 || eff.Retry.MaxLatency != 2*time.Second {
		t.Errorf("retry = %+v, want hedge 100ms from the gateway block and the rest from defaults", eff.Retry)
	}
	if eff.Timeout.RelayTimeout != 9*time.Second {
		t.Errorf("relay_timeout = %v, want 9s", eff.Timeout.RelayTimeout)
	}
}

func TestGatewayMode_DelegatedIsWarned(t *testing.T) {
	cfg := loadRetry(t, ``)
	if len(cfg.Warnings) != 0 {
		t.Fatalf("centralized must be silent: %v", cfg.Warnings)
	}
	cfg, err := parse([]byte(`
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: delegated
`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(cfg.Warnings, "\n"), "App-Address") {
		t.Errorf("delegated mode loads silently: %v", cfg.Warnings)
	}
}

func TestHealthChecksEnabledFalse_IsHonouredByPresence(t *testing.T) {
	absent := loadRetry(t, ``)
	if absent.Gateway.HealthChecks.Disabled() {
		t.Fatal("an absent enabled key means probing is on, as on PATH")
	}
	off := loadRetry(t, `
  active_health_checks:
    enabled: false
`)
	if !off.Gateway.HealthChecks.Disabled() {
		t.Fatal("enabled: false must turn probing off")
	}
	if !strings.Contains(strings.Join(off.Warnings, "\n"), "no health-check probes are sent") {
		t.Errorf("no warning says probing is off: %v", off.Warnings)
	}
	on := loadRetry(t, `
  active_health_checks:
    enabled: true
`)
	if on.Gateway.HealthChecks.Disabled() {
		t.Fatal("enabled: true must not disable")
	}
}
