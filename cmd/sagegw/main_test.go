package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pokt-network/sage/config"
)

// The warning this drives is the only thing standing between a copied config
// line and a publicly readable heap dump, so the bare-port case matters most:
// ":6060" looks local in a config file and binds every interface.
func TestIsLoopbackAddr(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"localhost:6060", true},
		{"127.0.0.1:6060", true},
		{"[::1]:6060", true},
		{":6060", false},        // bare port = every interface
		{"0.0.0.0:6060", false}, // explicit all-interfaces
		{"192.168.1.10:6060", false},
		{"gateway.internal:6060", false}, // a name we cannot resolve to loopback
		{"not-an-addr", false},           // unparseable: assume exposed
		{"", false},
	}

	for _, tc := range cases {
		t.Run(tc.addr, func(t *testing.T) {
			if got := isLoopbackAddr(tc.addr); got != tc.want {
				t.Errorf("isLoopbackAddr(%q) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestDefaultAdminAddrIsLoopback ties the default to the check that warns about
// it. The admin API is unauthenticated, so a default that isLoopbackAddr calls
// exposed would ship a control plane on every interface AND log a warning about
// it on every startup — the config default must never be the thing being warned
// about.
func TestDefaultAdminAddrIsLoopback(t *testing.T) {
	if !isLoopbackAddr(config.DefaultAdminAddr) {
		t.Errorf("config.DefaultAdminAddr = %q, which isLoopbackAddr considers exposed; the unauthenticated admin API must default to loopback",
			config.DefaultAdminAddr)
	}
}

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"info":    slog.LevelInfo,
		// An unrecognized level must not silence the gateway. Info is the
		// safe reading of a typo; error or a panic would not be.
		"":        slog.LevelInfo,
		"verbose": slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLogLevel(in); got != want {
			t.Errorf("parseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestLoadConfig_FlagWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	minimal := "full_node_config:\n  rpc_url: http://fullnode.invalid:26657\n  grpc_config:\n    host_port: fullnode.invalid:9090\ngateway_config:\n  gateway_mode: centralized\n"
	if err := os.WriteFile(path, []byte(minimal), 0o600); err != nil {
		t.Fatal(err)
	}

	// Set the env var too: the flag is the explicit instruction and must win,
	// or an operator pointing at a specific file silently gets another one.
	t.Setenv("GATEWAY_CONFIG", "/nonexistent/config.yaml")

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Gateway.GatewayMode != "centralized" {
		t.Errorf("gateway_mode = %q, want the file's value", cfg.Gateway.GatewayMode)
	}
}

func TestLoadConfig_MissingFileIsAnError(t *testing.T) {
	if _, err := loadConfig(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("loadConfig accepted a path that does not exist")
	}
}

// Starting with no config at all must fail loudly. A gateway that booted on
// defaults would have no services, no identity, and nothing to relay with.
func TestLoadConfig_NoFlagAndNoEnvFails(t *testing.T) {
	t.Setenv("GATEWAY_CONFIG", "")

	_, err := loadConfig("")
	if err == nil {
		t.Fatal("loadConfig succeeded with neither a flag nor an env var")
	}
	if !strings.Contains(err.Error(), "-config") {
		t.Errorf("error %q does not tell the operator how to fix it", err)
	}
}
