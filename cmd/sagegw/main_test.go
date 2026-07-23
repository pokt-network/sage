package main

import (
	"testing"

	"github.com/pokt-network/sage/config"
)

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
			if got := isLoopbackAddr(tc.addr); got != tc.want {
				t.Errorf("isLoopbackAddr(%q) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestDefaultAdminAddrIsLoopback ties the default to the check that warns about
// it. The admin API is unauthenticated, so a default that isLoopbackAddr calls
// exposed would ship a control plane on every interface AND log a warning about
// it on every startup — the config default must never be the thing being warned
// about.
func TestDefaultAdminAddrIsLoopback(t *testing.T) {
	if !isLoopbackAddr(config.DefaultAdminAddr) {
		t.Errorf("config.DefaultAdminAddr = %q, which isLoopbackAddr considers exposed; the unauthenticated admin API must default to loopback",
			config.DefaultAdminAddr)
	}
}
