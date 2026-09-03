package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

type fakeChainViews struct {
	views map[domain.ServiceID]qos.ChainView
}

func (f fakeChainViews) ChainViewFor(id domain.ServiceID) (qos.ChainView, bool) {
	v, ok := f.views[id]
	return v, ok
}

func collectChainView(t *testing.T, src ChainViewSource, services []domain.ServiceID, now time.Time) map[string]float64 {
	t.Helper()
	c := NewChainViewCollector(src, services)
	c.now = func() time.Time { return now }
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	return scrapeAll(t, reg)
}

// The four numbers an operator needs, and staleness above all: it is what
// shows a chain view going quiet, which nothing else in the metric set does.
func TestChainViewCollector_ExportsTheView(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	src := fakeChainViews{views: map[domain.ServiceID]qos.ChainView{
		"eth": {Perceived: 1000, Highest: 1004, Lowest: 998, Endpoints: 3, Newest: now.Add(-45 * time.Second)},
	}}

	got := collectChainView(t, src, []domain.ServiceID{"eth"}, now)

	want := map[string]float64{
		`sage_chain_view_height{service_id="eth"}`:            1000,
		`sage_chain_view_spread_blocks{service_id="eth"}`:     6,
		`sage_chain_view_endpoints{service_id="eth"}`:         3,
		`sage_chain_view_staleness_seconds{service_id="eth"}`: 45,
	}
	for name, v := range want {
		if got[name] != v {
			t.Errorf("%s = %v, want %v", name, got[name], v)
		}
	}
}

// A service whose plugin tracks no height is absent, not zero — a zero height
// would read as a chain at genesis.
func TestChainViewCollector_SkipsServicesWithNoView(t *testing.T) {
	now := time.Now()
	src := fakeChainViews{views: map[domain.ServiceID]qos.ChainView{
		"eth": {Perceived: 1000, Endpoints: 1, Newest: now},
	}}

	got := collectChainView(t, src, []domain.ServiceID{"eth", "noop-service"}, now)

	for name := range got {
		if strings.Contains(name, "noop-service") {
			t.Errorf("exported %s for a service with no chain view", name)
		}
	}
	if got[`sage_chain_view_height{service_id="eth"}`] != 1000 {
		t.Error("the service that does have a view was not exported")
	}
}

// The trap this guards: a zero Newest is "no observation", and subtracting it
// from now exports the age of the Unix epoch — a number that reads as a
// catastrophically stale chain rather than as no data.
func TestChainViewCollector_OmitsStalenessWithNoObservation(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	src := fakeChainViews{views: map[domain.ServiceID]qos.ChainView{
		// Perceived survives an empty window: selection still filters on it.
		"eth": {Perceived: 1000, Endpoints: 0},
	}}

	got := collectChainView(t, src, []domain.ServiceID{"eth"}, now)

	if v, ok := got[`sage_chain_view_staleness_seconds{service_id="eth"}`]; ok {
		t.Errorf("staleness exported as %v with no observation; it must be absent", v)
	}
	if got[`sage_chain_view_height{service_id="eth"}`] != 1000 {
		t.Error("height must still be exported: it is what selection is filtering on")
	}
	if got[`sage_chain_view_endpoints{service_id="eth"}`] != 0 {
		t.Error("endpoints must read 0, which is what says the spread means silence")
	}
}

// scrapeAll returns every exported series keyed by its full name and label
// set, so a test can assert on absence as well as value.
func scrapeAll(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	out := make(map[string]float64)
	for _, name := range []string{
		"sage_chain_view_height",
		"sage_chain_view_spread_blocks",
		"sage_chain_view_endpoints",
		"sage_chain_view_staleness_seconds",
	} {
		for labels, v := range scrapeValues(t, reg, name) {
			out[name+labels] = v
		}
	}
	return out
}
