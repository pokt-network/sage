package config

import (
	"strings"
	"testing"
)

func TestQoSCoverageFor(t *testing.T) {
	services := []ServiceConfig{
		{ID: "eth", Type: "evm"},
		{ID: "osmosis", Type: "cosmos"},
		{ID: "solana", Type: "solana"},
		{ID: "tron", Type: "generic"},
		{ID: "near", Type: "generic"},
		{ID: "nothing", Type: ""},
		{ID: "typo", Type: "evmm"},
	}

	cov := QoSCoverageFor(services)

	// A chain-specific plugin is covered and says nothing.
	for _, id := range []string{"eth", "osmosis", "solana"} {
		if strings.Contains(strings.Join(cov.Passthrough, " ")+strings.Join(cov.Unrecognised, " "), id) {
			t.Errorf("%s has a plugin and should not be reported", id)
		}
	}

	// Declared generic, and an absent type, are choices.
	wantPassthrough := []string{"near", "nothing", "tron"}
	if strings.Join(cov.Passthrough, ",") != strings.Join(wantPassthrough, ",") {
		t.Errorf("passthrough = %v, want %v", cov.Passthrough, wantPassthrough)
	}

	// A type that matched nothing is almost certainly a typo, and is reported
	// separately because it needs a different answer.
	if len(cov.Unrecognised) != 1 || !strings.Contains(cov.Unrecognised[0], "typo") {
		t.Fatalf("unrecognised = %v, want the one typo", cov.Unrecognised)
	}
	if !strings.Contains(cov.Unrecognised[0], `"evmm"`) {
		t.Errorf("the line does not quote the value that was typed: %s", cov.Unrecognised[0])
	}
}

// The two cases need different answers, so they must not share a line: a
// deliberate passthrough is a choice to confirm, a typo is a mistake to fix.
func TestQoSCoverage_LinesSeparateTheTwoCases(t *testing.T) {
	cov := QoSCoverageFor([]ServiceConfig{
		{ID: "tron", Type: "generic"},
		{ID: "typo", Type: "evmm"},
	})

	lines := cov.Lines()
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want one per case:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	joined := strings.Join(lines, "\n")
	for _, want := range []string{
		"typo in `type`", // the mistake case names the mistake
		"evm, cosmos, solana",
		"Deliberate if", // the choice case reads as a choice
		"reputation",    // both say what is actually lost
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("no line mentions %q:\n%s", want, joined)
		}
	}
}

// A fleet where every service has a plugin says nothing at all: the report is
// for what needs attention, not an inventory.
func TestQoSCoverage_SilentWhenEverythingIsCovered(t *testing.T) {
	cov := QoSCoverageFor([]ServiceConfig{
		{ID: "eth", Type: "evm"},
		{ID: "osmosis", Type: "cosmos"},
	})
	if lines := cov.Lines(); len(lines) != 0 {
		t.Errorf("reported %v for a fully covered fleet", lines)
	}
}

// One line per case, not per service: a fleet can carry dozens and the report
// is read by somebody deciding whether to act.
func TestQoSCoverage_OneLinePerCase(t *testing.T) {
	var services []ServiceConfig
	for i := range 40 {
		services = append(services, ServiceConfig{ID: string(rune('a'+i%26)) + "-svc", Type: "generic"})
	}
	lines := QoSCoverageFor(services).Lines()
	if len(lines) != 1 {
		t.Fatalf("got %d lines for 40 passthrough services, want 1", len(lines))
	}
	if !strings.Contains(lines[0], "40 service(s)") {
		t.Errorf("the line does not say how many: %s", lines[0])
	}
}
