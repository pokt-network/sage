package config

import (
	"strings"
	"testing"
)

func TestValidateAdmin(t *testing.T) {
	const goodToken = "0123456789abcdef0123456789abcdef"

	tests := []struct {
		name    string
		cfg     AdminConfig
		wantErr string
	}{
		{
			name: "loopback without a token is allowed",
			cfg:  AdminConfig{Addr: DefaultAdminAddr},
		},
		{
			name: "loopback IP without a token is allowed",
			cfg:  AdminConfig{Addr: "127.0.0.1:9091"},
		},
		{
			name:    "every interface without a token is refused",
			cfg:     AdminConfig{Addr: ":9091"},
			wantErr: "reachable from outside this host",
		},
		{
			name:    "routable address without a token is refused",
			cfg:     AdminConfig{Addr: "10.0.0.7:9091"},
			wantErr: "reachable from outside this host",
		},
		{
			name: "routable address with a token is allowed",
			cfg:  AdminConfig{Addr: "10.0.0.7:9091", AuthToken: goodToken},
		},
		{
			name:    "short token is refused even on loopback",
			cfg:     AdminConfig{Addr: DefaultAdminAddr, AuthToken: "hunter2"},
			wantErr: "minimum is",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAdmin(tt.cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestEffectiveAuthToken_EnvWins pins the precedence: the env var exists so the
// token never has to be written into a config file, which is the artifact most
// likely to be committed or baked into an image.
func TestEffectiveAuthToken_EnvWins(t *testing.T) {
	t.Setenv(EnvAdminToken, "  env-token-0123456789abcdef  ")

	cfg := AdminConfig{AuthToken: "file-token-0123456789abcdef"}
	if got := cfg.EffectiveAuthToken(); got != "env-token-0123456789abcdef" {
		t.Fatalf("token = %q, want the trimmed env value", got)
	}
}

func TestEffectiveAuthToken_FileFallback(t *testing.T) {
	t.Setenv(EnvAdminToken, "")

	cfg := AdminConfig{AuthToken: "file-token-0123456789abcdef"}
	if got := cfg.EffectiveAuthToken(); got != "file-token-0123456789abcdef" {
		t.Fatalf("token = %q, want the config value", got)
	}
}
