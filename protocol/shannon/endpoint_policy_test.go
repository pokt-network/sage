package shannon

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

func policyTestProtocol(t *testing.T, url string) (*Protocol, string) {
	t.Helper()
	supplier := "pokt1supplierpolicy"
	session := buildRelayTestSession(supplier, url)
	sm := newSessionManager(&mockRelayFullNode{session: session}, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	p := &Protocol{
		sessions:  sm,
		bl:        newBlacklist(),
		ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		logger:    newTestLogger(),
	}
	return p, supplier
}

func TestAvailableEndpoints_BlockedSuppliers(t *testing.T) {
	p, supplier := policyTestProtocol(t, "https://node.example.com")

	got, _ := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if len(got) != 1 {
		t.Fatalf("without a block the supplier is available, got %v", got)
	}

	p.blockedSuppliers = buildBlockedSuppliers([]config.ServiceConfig{
		{ID: "eth", BlockedSuppliers: []string{supplier}},
	})
	got, _ = p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if len(got) != 0 {
		t.Errorf("a blocked supplier must not be selected, got %v", got)
	}
	// A dry-run (RegisteredEndpoints) still counts it — it exists, just excluded.
	reg, _ := p.RegisteredEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
	if len(reg) != 1 {
		t.Errorf("RegisteredEndpoints must still list a blocked supplier, got %v", reg)
	}
}

func TestAvailableEndpoints_EndpointPolicy(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		policy  config.EndpointPolicy
		allowed bool
	}{
		{"https allowed under require_https", "https://node.example.com", config.EndpointPolicy{RequireHTTPS: true}, true},
		{"http dropped under require_https", "http://node.example.com", config.EndpointPolicy{RequireHTTPS: true}, false},
		{"wss allowed under require_https", "wss://node.example.com", config.EndpointPolicy{RequireHTTPS: true}, true},
		{"http allowed when require_https off", "http://node.example.com", config.EndpointPolicy{}, true},
		{"domain allowed under require_domain", "https://node.example.com", config.EndpointPolicy{RequireDomain: true}, true},
		{"raw IP dropped under require_domain", "https://62.84.183.58", config.EndpointPolicy{RequireDomain: true}, false},
		{"raw IP with port dropped", "https://62.84.183.58:8545", config.EndpointPolicy{RequireDomain: true}, false},
		{"raw IP allowed when require_domain off", "https://62.84.183.58", config.EndpointPolicy{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := policyTestProtocol(t, tc.url)
			p.endpointPolicy = newEndpointPolicy(tc.policy)
			got, _ := p.AvailableEndpoints(context.Background(), "eth", domain.RPCTypeJSONRPC)
			if (len(got) == 1) != tc.allowed {
				t.Errorf("url %q policy %+v: available=%d, want allowed=%v", tc.url, tc.policy, len(got), tc.allowed)
			}
		})
	}
}
