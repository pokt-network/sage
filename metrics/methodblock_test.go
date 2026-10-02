package metrics

import (
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

type stubMethodBlocks struct{ blocks map[string][][2]string }

func (s *stubMethodBlocks) rows(serviceID string, yield func(...string)) {
	for _, b := range s.blocks[serviceID] {
		yield(b[0], b[1])
	}
}

func TestMethodBlockCollector_ReportsActiveBlocks(t *testing.T) {
	lister := &stubMethodBlocks{blocks: map[string][][2]string{
		"eth":    {{"slow.example.com", "eth_getLogs"}},
		"solana": {{"dead.example.com", ""}}, // host-level: empty method label
	}}
	out := scrape(t, NewMethodBlockCollector([]domain.ServiceID{"eth", "solana", "poly"}, lister.rows))
	for _, want := range []string{
		`sage_method_blocks{domain="slow.example.com",method="eth_getLogs",service_id="eth"} 1`,
		`sage_method_blocks{domain="dead.example.com",method="",service_id="solana"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `service_id="poly"`) {
		t.Error("a service with no blocks must be absent, not 0")
	}
}
