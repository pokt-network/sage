package healthcheck

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

func localConfig(services ...config.ServiceHealthChecks) config.HealthCheckConfig {
	return config.HealthCheckConfig{Enabled: true, Local: services}
}

func TestBuildConfiguredChecks(t *testing.T) {
	cfg := localConfig(config.ServiceHealthChecks{
		ServiceID: "pnf-anvil",
		Enabled:   true,
		Checks: []config.HealthCheck{
			{
				Name:             "eth_blockNumber",
				Type:             "json_rpc",
				Method:           "POST",
				Path:             "/",
				Body:             `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
				ReputationSignal: "minor_error",
				Timeout:          5 * time.Second,
			},
			{
				Name:             "status",
				Type:             "comet_bft",
				Method:           "GET",
				Path:             "/status",
				ReputationSignal: "major_error",
			},
		},
	})

	built, warnings := BuildConfiguredChecks(cfg)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}

	checks := built.For("pnf-anvil")
	if len(checks) != 2 {
		t.Fatalf("built %d checks, want 2", len(checks))
	}

	t.Run("json_rpc check", func(t *testing.T) {
		c := checks[0]
		if c.Payload.RPCType() != domain.RPCTypeJSONRPC {
			t.Errorf("RPCType = %q", c.Payload.RPCType())
		}
		if c.Payload.HTTPMethod() != "POST" || c.Payload.Path() != "/" {
			t.Errorf("verb/path = %q %q", c.Payload.HTTPMethod(), c.Payload.Path())
		}
		if len(c.Payload.Bytes()) == 0 {
			t.Error("body was not carried onto the payload")
		}
	})

	// The path is the whole request for a CometBFT check; dropping it would
	// send every check to the backend root.
	t.Run("comet_bft check keeps its path and verb", func(t *testing.T) {
		c := checks[1]
		if c.Payload.RPCType() != domain.RPCTypeCometBFT {
			t.Errorf("RPCType = %q", c.Payload.RPCType())
		}
		if c.Payload.Path() != "/status" || c.Payload.HTTPMethod() != "GET" {
			t.Errorf("verb/path = %q %q, want GET /status", c.Payload.HTTPMethod(), c.Payload.Path())
		}
	})

	// Two services may each define "eth_blockNumber"; their signals must not
	// collide.
	t.Run("names are namespaced by service", func(t *testing.T) {
		if checks[0].Name != "pnf-anvil:eth_blockNumber" {
			t.Errorf("Name = %q", checks[0].Name)
		}
	})
}

func TestBuildConfiguredChecks_Defaults(t *testing.T) {
	cfg := localConfig(config.ServiceHealthChecks{
		ServiceID: "svc",
		Enabled:   true,
		Checks:    []config.HealthCheck{{Name: "bare"}},
	})

	built, warnings := BuildConfiguredChecks(cfg)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	c := built.For("svc")[0]
	if c.Payload.RPCType() != domain.RPCTypeJSONRPC {
		t.Errorf("default RPCType = %q, want json_rpc", c.Payload.RPCType())
	}
	if c.Payload.HTTPMethod() != "POST" || c.Payload.Path() != "/" {
		t.Errorf("defaults = %q %q, want POST /", c.Payload.HTTPMethod(), c.Payload.Path())
	}
}

// A check that cannot be built must not stop the gateway, but it must also not
// disappear quietly — a missing check reads exactly like a passing one.
func TestBuildConfiguredChecks_BadRulesWarnAndSkip(t *testing.T) {
	cfg := localConfig(
		config.ServiceHealthChecks{ServiceID: "", Enabled: true},
		config.ServiceHealthChecks{
			ServiceID: "svc",
			Enabled:   true,
			Checks: []config.HealthCheck{
				{Name: ""},                        // no name
				{Name: "ws", Type: "websocket"},   // unsupported for checks
				{Name: "bogus", Type: "nonsense"}, // unknown type
				{Name: "good"},
			},
		},
	)

	built, warnings := BuildConfiguredChecks(cfg)
	if len(warnings) != 4 {
		t.Errorf("got %d warnings, want 4: %v", len(warnings), warnings)
	}
	if got := len(built.For("svc")); got != 1 {
		t.Errorf("built %d checks, want only the valid one", got)
	}
}

// The trap this config shape invites: rules written, never run.
func TestBuildConfiguredChecks_DeclaredButDisabledIsLoud(t *testing.T) {
	cfg := localConfig(config.ServiceHealthChecks{
		ServiceID: "svc",
		Enabled:   false,
		Checks:    []config.HealthCheck{{Name: "eth_blockNumber"}},
	})

	built, warnings := BuildConfiguredChecks(cfg)
	if len(warnings) != 1 {
		t.Fatalf("a disabled block with checks must warn, got %v", warnings)
	}
	if len(built.For("svc")) != 0 {
		t.Error("a disabled block must not contribute checks")
	}
}

func TestConfiguredChecks_SignalFor(t *testing.T) {
	cfg := localConfig(config.ServiceHealthChecks{
		ServiceID: "svc",
		Enabled:   true,
		Checks: []config.HealthCheck{
			{Name: "critical", ReputationSignal: "critical_error"},
			{Name: "unset"},
			{Name: "bogus", ReputationSignal: "not_a_signal"},
		},
	})
	built, _ := BuildConfiguredChecks(cfg)

	if _, ok := built.SignalFor("svc:critical", "reason", time.Second); !ok {
		t.Error("configured critical_error signal was not applied")
	}
	if _, ok := built.SignalFor("svc:unset", "reason", time.Second); ok {
		t.Error("a check with no configured signal should fall back to default grading")
	}
	if _, ok := built.SignalFor("svc:bogus", "reason", time.Second); ok {
		t.Error("an unrecognised signal name should fall back to default grading")
	}
}

// A nil ConfiguredChecks is the normal case for a config with no local block.
func TestConfiguredChecks_NilIsSafe(t *testing.T) {
	var c *ConfiguredChecks
	if got := c.For("svc"); got != nil {
		t.Errorf("For on nil = %v, want nil", got)
	}
	if _, ok := c.SignalFor("x", "y", time.Second); ok {
		t.Error("SignalFor on nil should report no configured signal")
	}
}
