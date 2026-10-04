package config

import "testing"

// Every Redis key SAGE writes used to be a hard-coded literal beginning
// "sage:". They are composed from RedisConfig.Key now, and an operator who has
// not set key_prefix must keep the keys their data is already under — a
// composition that differs by one byte is a fleet that silently loses its
// flags, drains and scores at the next roll.
//
// The names on the right are the literals as of 2026-09-18, one per family.
// Changing one is a migration, not an edit.
func TestRedisKey_DefaultsMatchTheHistoricalLiterals(t *testing.T) {
	var unset RedisConfig
	for name, want := range map[string]string{
		"reputation:":        "sage:reputation:",
		"overrides:":         "sage:overrides:",
		"flags:":             "sage:flags:",
		"circuit:":           "sage:circuit:",
		"drain":              "sage:drain",
		"blocked_domains":    "sage:blocked_domains",
		"probes":             "sage:probes",
		"leader:healthcheck": "sage:leader:healthcheck",
		"auto_drain:events":  "sage:auto_drain:events",
	} {
		if got := unset.Key(name); got != want {
			t.Errorf("Key(%q) = %q, want the historical %q", name, got, want)
		}
	}
}

// A prefix an operator sets replaces "sage:" entirely, so two deployments
// sharing a database share nothing.
func TestRedisKey_PrefixIsolatesDeployments(t *testing.T) {
	a := RedisConfig{KeyPrefix: "mainnet:"}
	b := RedisConfig{KeyPrefix: "canary:"}
	if a.Key("leader:healthcheck") == b.Key("leader:healthcheck") {
		t.Fatal("two prefixes must not compose the same election key")
	}
	if got := a.Key("reputation:"); got != "mainnet:reputation:" {
		t.Errorf("Key = %q, want mainnet:reputation:", got)
	}
}

// One Redis or Sentinel, never both, and never half of Sentinel: each of
// these would boot against something the operator did not mean.
func TestRedisConfig_Validate(t *testing.T) {
	sentinels := []string{"s1:26379", "s2:26379"}
	for name, tc := range map[string]struct {
		cfg     RedisConfig
		wantErr bool
	}{
		"none":                     {RedisConfig{}, false},
		"address":                  {RedisConfig{Address: "r:6379"}, false},
		"sentinel":                 {RedisConfig{SentinelMaster: "mymaster", SentinelAddresses: sentinels}, false},
		"both":                     {RedisConfig{Address: "r:6379", SentinelMaster: "mymaster", SentinelAddresses: sentinels}, true},
		"master without sentinels": {RedisConfig{SentinelMaster: "mymaster"}, true},
		"sentinels without master": {RedisConfig{SentinelAddresses: sentinels}, true},
		"address beside sentinels": {RedisConfig{Address: "r:6379", SentinelAddresses: sentinels}, true},
	} {
		if err := tc.cfg.Validate(); (err != nil) != tc.wantErr {
			t.Errorf("%s: Validate() = %v, want error %v", name, err, tc.wantErr)
		}
	}
	if !(RedisConfig{SentinelMaster: "m", SentinelAddresses: sentinels}).Enabled() || (RedisConfig{}).Enabled() {
		t.Error("Enabled must be true for Sentinel and false for nothing configured")
	}
}
