package config

import (
	"strings"
	"testing"
)

// The env override exists for a deployment whose config file is a sealed
// secret: the level must change with an environment edit alone, and the
// change must announce itself, because a file that says "error" while the
// pods log at debug is otherwise a mystery to the next reader.
func TestLogLevelEnv_OverridesFileAndWarns(t *testing.T) {
	t.Setenv(EnvLogLevel, "  DEBUG ")
	cfg, err := parse([]byte(minimalConfigYAML + `
logger_config:
  level: error
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logger.Level != "debug" {
		t.Fatalf("Level = %q, want debug from the environment", cfg.Logger.Level)
	}
	if !hasWarning(cfg.Warnings, `logger_config.level "error" overridden by SAGE_LOG_LEVEL="debug"`) {
		t.Fatalf("no override warning in %q", cfg.Warnings)
	}
}

func TestLogLevelEnv_UnsetLeavesFile(t *testing.T) {
	t.Setenv(EnvLogLevel, "")
	cfg, err := parse([]byte(minimalConfigYAML + `
logger_config:
  level: error
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logger.Level != "error" {
		t.Fatalf("Level = %q, want the file's error", cfg.Logger.Level)
	}
	if hasWarning(cfg.Warnings, EnvLogLevel) {
		t.Fatalf("unexpected env warning in %q", cfg.Warnings)
	}
}

func TestLogLevelEnv_SameAsFileIsSilent(t *testing.T) {
	t.Setenv(EnvLogLevel, "error")
	cfg, err := parse([]byte(minimalConfigYAML + `
logger_config:
  level: error
`))
	if err != nil {
		t.Fatal(err)
	}
	if hasWarning(cfg.Warnings, EnvLogLevel) {
		t.Fatalf("an override that changes nothing should not warn: %q", cfg.Warnings)
	}
}

func TestLogLevelEnv_BadValueIgnoredWithWarning(t *testing.T) {
	t.Setenv(EnvLogLevel, "verbose")
	cfg, err := parse([]byte(minimalConfigYAML + `
logger_config:
  level: error
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logger.Level != "error" {
		t.Fatalf("Level = %q, want the file's error kept for a bad value", cfg.Logger.Level)
	}
	if !hasWarning(cfg.Warnings, `SAGE_LOG_LEVEL="verbose" is not a log level`) {
		t.Fatalf("no bad-value warning in %q", cfg.Warnings)
	}
}

// Absent from the file, the default is info and the env still wins over it.
func TestLogLevelEnv_OverridesDefault(t *testing.T) {
	t.Setenv(EnvLogLevel, "warn")
	cfg, err := parse([]byte(minimalConfigYAML))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Logger.Level != "warn" {
		t.Fatalf("Level = %q, want warn", cfg.Logger.Level)
	}
	if !hasWarning(cfg.Warnings, `logger_config.level "info" overridden`) {
		t.Fatalf("no override warning in %q", cfg.Warnings)
	}
}

// minimalConfigYAML is the least a config needs to pass validation.
const minimalConfigYAML = `
full_node_config:
  rpc_url: http://localhost:26657
  grpc_config:
    host_port: localhost:9090
gateway_config:
  gateway_mode: centralized
`

func hasWarning(ws []string, sub string) bool {
	for _, w := range ws {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}
