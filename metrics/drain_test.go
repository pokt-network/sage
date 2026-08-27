package metrics

import (
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

type stubDrains struct{ drains map[string][]DrainEntry }

func (s *stubDrains) ActiveDrains(serviceID string) []DrainEntry {
	return s.drains[serviceID]
}

func TestDrainCollector_ReportsActiveDrains(t *testing.T) {
	lister := &stubDrains{drains: map[string][]DrainEntry{
		"eth":    {{Domain: "bad.example.com", RPCType: "websocket"}},
		"solana": {{Domain: "dead.example.com"}}, // unscoped: "all" label
	}}
	out := scrape(t, NewDrainCollector(lister, []domain.ServiceID{"eth", "solana", "poly"}))
	for _, want := range []string{
		`sage_drained_operators{domain="bad.example.com",rpc_type="websocket",service_id="eth"} 1`,
		`sage_drained_operators{domain="dead.example.com",rpc_type="all",service_id="solana"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `service_id="poly"`) {
		t.Error("a service with no drains must be absent, not 0")
	}
}

func TestDrainCollector_NilListerIsSafe(t *testing.T) {
	out := scrape(t, NewDrainCollector(nil, []domain.ServiceID{"eth"}))
	if strings.Contains(out, "sage_drained_operators{") {
		t.Errorf("nil lister must produce no series, got:\n%s", out)
	}
}
