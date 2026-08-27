package metrics

import (
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
)

// stubTrafficLister answers PreviousWindow from a fixed map, mirroring how a
// real traffic.Sampler answers only for a service with a completed window.
type stubTrafficLister struct {
	windows map[string][2]float64 // serviceID -> {distinctRatio, top1Share}
}

func (s *stubTrafficLister) PreviousWindow(serviceID string) (distinctRatio, top1Share float64, ok bool) {
	w, ok := s.windows[serviceID]
	if !ok {
		return 0, 0, false
	}
	return w[0], w[1], true
}

func TestTrafficCollector_ReportsPreviousWindow(t *testing.T) {
	lister := &stubTrafficLister{windows: map[string][2]float64{
		"eth": {0.25, 0.6},
	}}
	out := scrape(t, NewTrafficCollector(lister, []domain.ServiceID{"eth", "poly"}))

	for _, want := range []string{
		`sage_request_sample_distinct_ratio{service_id="eth"} 0.25`,
		`sage_request_sample_top_share{service_id="eth"} 0.6`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `service_id="poly"`) {
		t.Error("a service with no previous window must be absent, not 0")
	}
}

func TestTrafficCollector_NilListerIsSafe(t *testing.T) {
	out := scrape(t, NewTrafficCollector(nil, []domain.ServiceID{"eth"}))
	if strings.Contains(out, "sage_request_sample") {
		t.Error("expected no series with a nil lister")
	}
}
