package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
