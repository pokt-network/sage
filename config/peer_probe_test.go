package config

import (
	"strings"
	"testing"
	"time"
)

// Reading this instance's own stream as a peer would make its leader skip
// its own probes, so that db is refused; another db loads.
func TestPeerProbeStream_OwnDBIsRefused(t *testing.T) {
	base := "redis_config:\n  db: 6\n" + ownedAppsBase + "  owned_apps_addresses: [pokt1e3scnf3tfs9t44pawlvpemm6r0ggy3un4avmdk]\n  active_health_checks:\n"

	if _, err := LoadFromBytes([]byte(base + "    peer_probe_stream: {enabled: true, db: 6}\n")); err == nil || !strings.Contains(err.Error(), "peer_probe_stream.db") {
		t.Fatalf("own db: err = %v, want a refusal", err)
	}

	cfg, err := LoadFromBytes([]byte(base + "    peer_probe_stream: {enabled: true, db: 7, max_age: 150s}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p := cfg.Gateway.HealthChecks.PeerProbeStream; !p.Enabled || p.DB != 7 || p.MaxAge != 150*time.Second {
		t.Fatalf("peer_probe_stream = %+v", p)
	}
}
