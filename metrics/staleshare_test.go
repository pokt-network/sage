package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

func TestStaleShareCollectorReportsEachParty(t *testing.T) {
	c := NewStaleShareCollector(func(yield func(domain.ServiceID, string, float64, float64, bool)) {
		yield("tron", "owner-a", 0.78, -40, true)
		yield("tron", "op.example", 0.01, 0, false)
	})
	ch := make(chan prometheus.Metric, 8)
	c.Collect(ch)
	if n := len(ch); n != 4 {
		t.Fatalf("%d series, want share and penalty for two parties", n)
	}
}
