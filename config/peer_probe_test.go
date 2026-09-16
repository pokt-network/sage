package config

import (
	"strings"
	"testing"
	"time"
)

// Reading this instance's own stream as a peer would make its leader skip its
// own probes. That block is turned off with a warning, not refused: refusing
// crash-looped mainnet pods on 2026-09-15. Another db loads as written.
func TestPeerProbeStream_OwnDBIsTurnedOffWithAWarning(t *testing.T) {
	base := "redis_config:\n  db: 6\n" + ownedAppsBase + "  owned_apps_addresses: [pokt1e3scnf3tfs9t44pawlvpemm6r0ggy3un4avmdk]\n  active_health_checks:\n"

	cfg, err := LoadFromBytes([]byte(base + "    peer_probe_stream: {enabled: true, db: 6}\n"))
	if err != nil {
		t.Fatalf("own db: err = %v, want the config to load", err)
	}
	if cfg.Gateway.HealthChecks.PeerProbeStream.Enabled {
		t.Error("own db: peer_probe_stream is still enabled")
	}
	if !strings.Contains(strings.Join(cfg.Warnings, "\n"), "peer_probe_stream.db (6)") {
		t.Errorf("own db: warnings = %v, want one naming peer_probe_stream.db", cfg.Warnings)
	}

	cfg, err = LoadFromBytes([]byte(base + "    peer_probe_stream: {enabled: true, db: 7, max_age: 150s}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p := cfg.Gateway.HealthChecks.PeerProbeStream; !p.Enabled || p.DB != 7 || p.MaxAge != 150*time.Second {
		t.Fatalf("peer_probe_stream = %+v", p)
	}
	for _, w := range cfg.Warnings {
		if strings.Contains(w, "peer_probe_stream") {
			t.Errorf("another db warned: %q", w)
		}
	}
}
