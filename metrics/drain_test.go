package metrics

import (
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

type stubDrains struct{ drains map[string][][2]string }

func (s *stubDrains) rows(serviceID string, yield func(...string)) {
	for _, d := range s.drains[serviceID] {
		yield(d[0], d[1])
	}
}

func TestDrainCollector_ReportsActiveDrains(t *testing.T) {
	lister := &stubDrains{drains: map[string][][2]string{
		"eth":    {{"bad.example.com", "websocket"}},
		"solana": {{"dead.example.com", ""}}, // unscoped: "all" label
	}}
	out := scrape(t, NewDrainCollector([]domain.ServiceID{"eth", "solana", "poly"}, lister.rows))
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
	out := scrape(t, NewDrainCollector([]domain.ServiceID{"eth"}, nil))
	if strings.Contains(out, "sage_drained_operators{") {
		t.Errorf("nil lister must produce no series, got:\n%s", out)
	}
}
