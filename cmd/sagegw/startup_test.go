package main

import (
	"log/slog"
	"testing"
)

// The boot-time config report is how SAGE says what it made of the config it
// was handed, and a deployment running at "error" silenced every line of it.
// The mainnet canary ran exactly that on 2026-09-03: it carried a rule file
// SAGE does not read and a max_workers SAGE did not implement, the lines
// saying so were emitted and dropped, and an operator spent an afternoon
// asking what the startup log had already answered.
func TestStartupReporter_EmitsWarningsWhateverTheLevel(t *testing.T) {
	cases := []struct {
		configured string
		wantWarn   bool
	}{
		{configured: "debug", wantWarn: true},
		{configured: "info", wantWarn: true},
		{configured: "warn", wantWarn: true},
		{configured: "error", wantWarn: true},
		{configured: "", wantWarn: true},
		{configured: "nonsense", wantWarn: true},
	}
	for _, tc := range cases {
		t.Run("level "+tc.configured, func(t *testing.T) {
			got := startupReporter(tc.configured).Enabled(t.Context(), slog.LevelWarn)
			if got != tc.wantWarn {
				t.Errorf("WARN enabled = %v at configured level %q, want %v", got, tc.configured, tc.wantWarn)
			}
		})
	}
}

// It reports, it does not shout. Promoting these to ERROR to defeat a filter
// would be lying about severity to win an argument with a config.
func TestStartupReporter_DoesNotPromoteSeverity(t *testing.T) {
	r := startupReporter("error")
	if !r.Enabled(t.Context(), slog.LevelWarn) {
		t.Fatal("precondition: WARN not enabled")
	}
	if r.Enabled(t.Context(), slog.LevelInfo) {
		t.Error("INFO is enabled: the report should lift the floor to WARN, not below it")
	}
}

// A more verbose configured level is honoured rather than overridden — an
// operator running at debug asked for debug.
func TestStartupReporter_KeepsAMoreVerboseLevel(t *testing.T) {
	if !startupReporter("debug").Enabled(t.Context(), slog.LevelDebug) {
		t.Error("debug was configured and the report dropped to warn")
	}
}
