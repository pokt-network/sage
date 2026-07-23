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
			_, err := buildOwnedApps(nil, []string{tt.key}, newTestLogger())
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
