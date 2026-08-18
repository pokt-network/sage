package metrics

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/pokt-network/sage/domain"
)

type fakeScoreLister struct {
	scores map[domain.ServiceID]map[string]float64
	err    error
}

func (f *fakeScoreLister) GetScores(_ context.Context, serviceID domain.ServiceID) (map[string]float64, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.scores[serviceID], nil
}

// gather collects one scrape from a registry holding only the collector.
func gather(t *testing.T, c prometheus.Collector) []*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	return mfs
}

func familyByName(mfs []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

func TestScoreCollector_ReportsScores(t *testing.T) {
	lister := &fakeScoreLister{scores: map[domain.ServiceID]map[string]float64{
		"eth": {"https://a.example.com|json_rpc": 90, "https://b.example.com|json_rpc": 40},
	}}

	mfs := gather(t, NewScoreCollector(lister, []domain.ServiceID{"eth"}))

	mf := familyByName(mfs, "sage_endpoint_reputation_score")
	if mf == nil {
		t.Fatal("sage_endpoint_reputation_score not reported")
	}
	if got := len(mf.GetMetric()); got != 2 {
		t.Fatalf("series = %d, want 2", got)
	}
}

// A key that stops being scored must stop being reported. This is the whole
// reason the metric is a Collector: a pushed GaugeVec would keep the old child
// forever, and a supplier registration that rotated out three sessions ago
// would still be on the scrape.
func TestScoreCollector_DoesNotRetainVanishedKeys(t *testing.T) {
	lister := &fakeScoreLister{scores: map[domain.ServiceID]map[string]float64{
		"eth": {"https://a.example.com|json_rpc": 90, "https://gone.example.com|json_rpc": 10},
	}}
	c := NewScoreCollector(lister, []domain.ServiceID{"eth"})

	if got := len(familyByName(gather(t, c), "sage_endpoint_reputation_score").GetMetric()); got != 2 {
		t.Fatalf("first scrape series = %d, want 2", got)
	}

	delete(lister.scores["eth"], "https://gone.example.com|json_rpc")

	mf := familyByName(gather(t, c), "sage_endpoint_reputation_score")
	if got := len(mf.GetMetric()); got != 1 {
		t.Fatalf("second scrape series = %d, want 1 — a key no longer scored must not be reported", got)
	}
	for _, m := range mf.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetValue() == "https://gone.example.com|json_rpc" {
				t.Error("a key that is no longer scored was still reported")
			}
		}
	}
}

// Under per-endpoint granularity the key carries a supplier address, which
// rotates every session, so the set is unbounded by anything SAGE controls.
func TestScoreCollector_CapsSeriesAndKeepsTheWorst(t *testing.T) {
	scores := make(map[string]float64, maxScoreSeriesPerService+500)
	for i := 0; i < maxScoreSeriesPerService+500; i++ {
		// Scores climb with i, so the first 500 are the ones worth keeping.
		scores[fmt.Sprintf("pokt1supplier%05d-https://node.example.com|json_rpc", i)] = float64(i)
	}
	lister := &fakeScoreLister{scores: map[domain.ServiceID]map[string]float64{"eth": scores}}

	mfs := gather(t, NewScoreCollector(lister, []domain.ServiceID{"eth"}))

	mf := familyByName(mfs, "sage_endpoint_reputation_score")
	if got := len(mf.GetMetric()); got != maxScoreSeriesPerService {
		t.Fatalf("series = %d, want the cap %d", got, maxScoreSeriesPerService)
	}
	for _, m := range mf.GetMetric() {
		if v := m.GetGauge().GetValue(); v >= float64(maxScoreSeriesPerService) {
			t.Fatalf("kept a score of %v — truncation must keep the LOWEST scores", v)
		}
	}

	dropped := familyByName(mfs, "sage_endpoint_reputation_scores_dropped")
	if dropped == nil {
		t.Fatal("truncation was not reported")
	}
	if got := dropped.GetMetric()[0].GetGauge().GetValue(); got != 500 {
		t.Errorf("dropped = %v, want 500", got)
	}
}

// Absence reads as "no data". Zero reads as the worst score there is, which
// would page someone for a Redis blip.
func TestScoreCollector_SkipsServiceOnError(t *testing.T) {
	lister := &fakeScoreLister{err: fmt.Errorf("redis unreachable")}

	mfs := gather(t, NewScoreCollector(lister, []domain.ServiceID{"eth"}))

	if mf := familyByName(mfs, "sage_endpoint_reputation_score"); mf != nil {
		t.Errorf("reported %d series for a service whose scores could not be read", len(mf.GetMetric()))
	}
}

func TestScoreCollector_NilListerIsInert(t *testing.T) {
	if mfs := gather(t, NewScoreCollector(nil, []domain.ServiceID{"eth"})); len(mfs) != 0 {
		t.Errorf("nil lister reported %d families", len(mfs))
	}
}
