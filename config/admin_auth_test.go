package config

import (
	"strings"
	"testing"
	"time"
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

// TestEffectiveMaxDrain pins zero taking the 24h default rather than meaning
// "unbounded" — an unbounded drain defeats the ceiling the ceiling exists for.
func TestEffectiveMaxDrain(t *testing.T) {
	if got := (AdminConfig{}).EffectiveMaxDrain(); got != DefaultMaxDrain {
		t.Fatalf("zero max_drain = %v, want the default %v", got, DefaultMaxDrain)
	}
	if got := (AdminConfig{MaxDrain: 2 * time.Hour}).EffectiveMaxDrain(); got != 2*time.Hour {
		t.Fatalf("max_drain = %v, want 2h", got)
	}
}

// The warning this drives is the only thing standing between a copied config
// line and a publicly readable heap dump, so the bare-port case matters most:
// ":6060" looks local in a config file and binds every interface.
func TestIsLoopbackAddr(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"localhost:6060", true},
		{"127.0.0.1:6060", true},
		{"[::1]:6060", true},
		{":6060", false},        // bare port = every interface
		{"0.0.0.0:6060", false}, // explicit all-interfaces
		{"192.168.1.10:6060", false},
		{"gateway.internal:6060", false}, // a name we cannot resolve to loopback
		{"not-an-addr", false},           // unparseable: assume exposed
		{"", false},
	}

	for _, tc := range cases {
		t.Run(tc.addr, func(t *testing.T) {
			if got := IsLoopbackAddr(tc.addr); got != tc.want {
				t.Errorf("IsLoopbackAddr(%q) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestDefaultAdminAddrIsLoopback ties the default to the check that warns about
// it. The admin API is unauthenticated, so a default that IsLoopbackAddr calls
// exposed would ship a control plane on every interface AND log a warning about
// it on every startup — the config default must never be the thing being warned
// about.
func TestDefaultAdminAddrIsLoopback(t *testing.T) {
	if !IsLoopbackAddr(DefaultAdminAddr) {
		t.Errorf("DefaultAdminAddr = %q, which IsLoopbackAddr considers exposed; the unauthenticated admin API must default to loopback",
			DefaultAdminAddr)
	}
}
