package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

func TestSeedCollectorReportsEachParty(t *testing.T) {
	c := NewSeedCollector(func(yield func(domain.ServiceID, string, string, float64)) {
		yield("base", "op.example", "json_rpc", 70)
		yield("base", "pokt1owner", "websocket", 50)
	})
	ch := make(chan prometheus.Metric, 4)
	c.Collect(ch)
	if n := len(ch); n != 2 {
		t.Fatalf("%d series, want one per party", n)
	}
}
