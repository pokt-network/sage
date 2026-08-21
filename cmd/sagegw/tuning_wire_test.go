package main

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/tuning"
)

// tuningTestConfig is a config with one service that sets its own retry and
// timeout, so the test can tell a config value from an override.
func tuningTestConfig() *config.Config {
	return &config.Config{
		Gateway: config.GatewayConfig{
			Defaults: config.ServiceDefaults{
				Retry:   config.RetryConfig{MaxRetries: 2, HedgeDelay: 500 * time.Millisecond},
				Timeout: config.TimeoutConfig{RelayTimeout: 10 * time.Second},
			},
			Services: []config.ServiceConfig{
				{
					ID:      "eth",
					Retry:   config.RetryConfig{MaxRetries: 4},
					Timeout: config.TimeoutConfig{RelayTimeout: 20 * time.Second},
				},
			},
		},
	}
}

// TestTuningOverridesReachTheMiddlewares is the integration the store's own
// tests cannot cover: the closures the middlewares actually call have to
// consult the store, or the admin API accepts changes that never take effect.
func TestTuningOverridesReachTheMiddlewares(t *testing.T) {
	cfg := tuningTestConfig()
	store := tuning.NewStore()
	retryFn := newRetryFn(cfg, store)
	timeoutFn := newTimeoutFn(cfg, store)

	// Config values, before anyone overrides anything.
	if got := retryFn("eth").MaxRetries; got != 4 {
		t.Fatalf("eth max_retries = %d, want the service's configured 4", got)
	}
	if got := retryFn("poly").MaxRetries; got != 2 {
		t.Fatalf("poly max_retries = %d, want the default 2", got)
	}
	if got := timeoutFn("eth"); got != 20*time.Second {
		t.Fatalf("eth relay timeout = %s, want the configured 20s", got)
	}

	// A global override displaces the config for every service.
	if err := store.Set(tuning.KnobRetryMaxRetries, "", "0"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := retryFn("eth").MaxRetries; got != 0 {
		t.Fatalf("eth max_retries = %d after a global override, want 0", got)
	}
	if got := retryFn("poly").MaxRetries; got != 0 {
		t.Fatalf("poly max_retries = %d after a global override, want 0", got)
	}

	// A per-service override displaces the global one for that service only.
	if err := store.Set(tuning.KnobRetryMaxRetries, "eth", "7"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := retryFn("eth").MaxRetries; got != 7 {
		t.Fatalf("eth max_retries = %d, want the per-service 7", got)
	}
	if got := retryFn("poly").MaxRetries; got != 0 {
		t.Fatalf("poly max_retries = %d, want the global 0", got)
	}

	// Every knob the closures claim to serve, not just the first one.
	if err := store.Set(tuning.KnobHedgeDelay, "eth", "25ms"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Set(tuning.KnobRelayTimeout, "eth", "3s"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Set(tuning.KnobRetryMaxLatency, "eth", "2s"); err != nil {
		t.Fatalf("set: %v", err)
	}
	got := retryFn("eth")
	if got.HedgeDelay != 25*time.Millisecond {
		t.Fatalf("hedge delay = %s, want 25ms", got.HedgeDelay)
	}
	if got.MaxLatency != 2*time.Second {
		t.Fatalf("max latency = %s, want 2s", got.MaxLatency)
	}
	if timeoutFn("eth") != 3*time.Second {
		t.Fatalf("relay timeout = %s, want 3s", timeoutFn("eth"))
	}

	// Clearing returns the config value, rather than leaving the last override
	// in place or zeroing the setting.
	store.Delete(tuning.KnobRelayTimeout, "eth")
	if got := timeoutFn("eth"); got != 20*time.Second {
		t.Fatalf("relay timeout = %s after clearing, want the configured 20s back", got)
	}
}
