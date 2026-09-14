package shannon

import (
	"strings"
	"testing"
)

// A wrong-length secp256k1 key still derives a valid-looking pokt1… address, so
// without this check the failure surfaces as "app not found" against an address
// that was never staked — pointing at staking, the full node and the network
// rather than at a stray character. A 33-byte key is worse still: the extra byte
// is ignored and it derives the *correct* address, so nothing looks wrong.
func TestBuildOwnedApps_RejectsWrongLengthKeys(t *testing.T) {
	const valid = "1a5ce3ec4677f984be0c4fa87ac3d22f72013d2af8b082daf95305d127fea8ee" // 32 bytes

	tests := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"truncated paste", "1a5ce3ec"},
		{"half a key", valid[:32]},
		{"one stray byte", valid + "ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildOwnedApps(nil, []string{tt.key}, nil, newTestLogger())
			if err == nil {
				t.Fatal("a wrong-length key must be rejected before it derives an address")
			}
			if !strings.Contains(err.Error(), "bytes, want 32") {
				t.Errorf("the error must name the length, got: %v", err)
			}
			// The key itself must never reach a log or an error string.
			if tt.key != "" && strings.Contains(err.Error(), tt.key) {
				t.Errorf("the private key leaked into the error: %v", err)
			}
		})
	}
}

// An app named by address needs no key. Named both ways it counts once, and
// an address that is not pokt1… fails at startup, not as "app not found".
func TestOwnedAppAddresses(t *testing.T) {
	const key = "1a5ce3ec4677f984be0c4fa87ac3d22f72013d2af8b082daf95305d127fea8ee"
	const other = "pokt1e3scnf3tfs9t44pawlvpemm6r0ggy3un4avmdk"

	derived, err := ownedAppAddresses([]string{key}, nil)
	if err != nil || len(derived) != 1 || !strings.HasPrefix(derived[0], "pokt1") {
		t.Fatalf("from a key: %v, %v", derived, err)
	}

	got, err := ownedAppAddresses([]string{key}, []string{derived[0], other})
	if err != nil || len(got) != 2 || got[0] != derived[0] || got[1] != other {
		t.Fatalf("key plus its own address plus another = %v, %v; want each app once, in order", got, err)
	}

	got, err = ownedAppAddresses(nil, []string{other})
	if err != nil || len(got) != 1 || got[0] != other {
		t.Fatalf("addresses alone = %v, %v", got, err)
	}

	if _, err := buildOwnedApps(nil, nil, nil, newTestLogger()); err == nil || !strings.Contains(err.Error(), "owned_apps_addresses") {
		t.Errorf("no apps at all: err = %v, want a startup refusal naming owned_apps_addresses", err)
	}

	for _, bad := range []string{"", "pokt1notbech32", "cosmos1e3scnf3tfs9t44pawlvpemm6r0ggy3uncl5hcd"} {
		if _, err := ownedAppAddresses(nil, []string{bad}); err == nil || !strings.Contains(err.Error(), "owned_apps_addresses[0]") {
			t.Errorf("address %q: err = %v, want a refusal naming the entry", bad, err)
		}
	}
}
