package config

import (
	"strings"
	"testing"
)

const ownedAppsBase = `
full_node_config:
  rpc_url: https://rpc.example
  grpc_config:
    host_port: grpc.example:443
gateway_config:
  gateway_mode: centralized
`

// Apps named by address load quietly; apps named by private key load with a
// warning that SAGE never signs with them. Naming none is refused by the
// Shannon backend at startup (protocol/shannon buildOwnedApps), not here.
func TestOwnedApps_AddressesKeysAndNeither(t *testing.T) {
	cfg, err := LoadFromBytes([]byte(ownedAppsBase + "  owned_apps_addresses: [pokt1e3scnf3tfs9t44pawlvpemm6r0ggy3un4avmdk]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Gateway.OwnedAppsAddresses) != 1 || strings.Contains(strings.Join(cfg.Warnings, "\n"), "owned_apps_private_keys_hex") {
		t.Fatalf("addresses = %v, warnings = %v; want one address and no key warning", cfg.Gateway.OwnedAppsAddresses, cfg.Warnings)
	}

	cfg, err = LoadFromBytes([]byte(ownedAppsBase + "  owned_apps_private_keys_hex: [\"00\"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(cfg.Warnings, "\n"), "never signs with") {
		t.Fatalf("warnings = %v, want one saying the app keys are never signed with", cfg.Warnings)
	}
}
