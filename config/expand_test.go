package config

import (
	"strings"
	"testing"
)

func TestExpandEnv(t *testing.T) {
	t.Setenv("SAGE_TEST_KEY", "deadbeef")
	t.Setenv("SAGE_TEST_EMPTY", "")

	tests := []struct {
		name string
		in   string
		want string
		err  string
	}{
		{name: "expands a reference", in: "key: ${SAGE_TEST_KEY}\n", want: "key: deadbeef\n"},
		{name: "expands a quoted reference", in: "key: \"${SAGE_TEST_KEY}\"\n", want: "key: \"deadbeef\"\n"},
		{name: "expands every occurrence", in: "a: ${SAGE_TEST_KEY}\nb: ${SAGE_TEST_KEY}\n", want: "a: deadbeef\nb: deadbeef\n"},
		{name: "a bare dollar is left alone", in: "password: p$ss$word\n", want: "password: p$ss$word\n"},
		{name: "a bare $NAME is not a reference", in: "key: $SAGE_TEST_KEY\n", want: "key: $SAGE_TEST_KEY\n"},
		{name: "$$ escapes a literal", in: "key: $${SAGE_TEST_KEY}\n", want: "key: ${SAGE_TEST_KEY}\n"},
		{name: "$$ outside a reference is left alone", in: "password: a$$b$$\n", want: "password: a$$b$$\n"},
		{name: "unset is refused", in: "key: ${SAGE_TEST_MISSING}\n", err: "SAGE_TEST_MISSING is not set"},
		{name: "set but empty is refused", in: "key: ${SAGE_TEST_EMPTY}\n", err: "SAGE_TEST_EMPTY is not set"},
		{name: "a default is refused", in: "key: ${SAGE_TEST_KEY:-x}\n", err: "plain variable name"},
		{name: "an unterminated reference is refused", in: "key: ${SAGE_TEST_KEY\n", err: "unterminated"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := expandEnv([]byte(tc.in))
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want one containing %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expandEnv: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// The line number is the whole diagnostic for a config of several hundred
// lines, so it is part of the contract rather than decoration.
func TestExpandEnvErrorNamesTheLine(t *testing.T) {
	_, err := expandEnv([]byte("a: 1\nb: 2\nc: ${SAGE_TEST_MISSING}\n"))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("err = %v, want one naming line 3", err)
	}
}

// A value with a newline would add keys the file does not show. It is refused,
// and the refusal does not print the value: this is the secret path.
func TestExpandEnvRefusesNewlineWithoutPrintingTheValue(t *testing.T) {
	t.Setenv("SAGE_TEST_MULTILINE", "deadbeef\nadmin_config:\n  addr: 0.0.0.0:9999")

	_, err := expandEnv([]byte("key: ${SAGE_TEST_MULTILINE}\n"))
	if err == nil || !strings.Contains(err.Error(), "newline") {
		t.Fatalf("err = %v, want a refusal naming the newline", err)
	}
	if strings.Contains(err.Error(), "deadbeef") || strings.Contains(err.Error(), "admin_config") {
		t.Errorf("the error printed the value: %v", err)
	}
}

// Expansion lives in parse, so every entry point gets it: the file, the
// GATEWAY_CONFIG document, PUT /admin/config (LoadFromBytes) and everything
// the reload path re-reads.
func TestLoadFromBytesExpandsTheSigningKey(t *testing.T) {
	t.Setenv("SAGE_TEST_GATEWAY_KEY", "1a2b3c")

	cfg, err := LoadFromBytes([]byte(ownedAppsBase + "  gateway_private_key_hex: \"${SAGE_TEST_GATEWAY_KEY}\"\n"))
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	if cfg.Gateway.GatewayPrivateKeyHex != "1a2b3c" {
		t.Errorf("gateway_private_key_hex = %q, want the expanded value", cfg.Gateway.GatewayPrivateKeyHex)
	}
}

// The unset case fails the load rather than starting with an empty signing
// key, and says which variable to set without quoting anything secret.
func TestLoadFromBytesRefusesAnUnsetVariable(t *testing.T) {
	_, err := LoadFromBytes([]byte(ownedAppsBase + "  gateway_private_key_hex: \"${SAGE_TEST_UNSET_KEY}\"\n"))
	if err == nil || !strings.Contains(err.Error(), "SAGE_TEST_UNSET_KEY") {
		t.Fatalf("err = %v, want a refusal naming SAGE_TEST_UNSET_KEY", err)
	}
}
