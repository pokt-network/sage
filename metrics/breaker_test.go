package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pokt-network/sage/domain"
)

// stubLister stands in for circuitbreaker.Breaker.
type stubLister struct {
	broken map[string][]string
	calls  int
}

func (s *stubLister) BrokenDomains(serviceID string) []string {
	s.calls++
	return s.broken[serviceID]
}

func scrape(t *testing.T, c prometheus.Collector) string {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, req)
	return w.Body.String()
}

func TestBreakerCollector_ReportsBrokenDomains(t *testing.T) {
	lister := &stubLister{broken: map[string][]string{
		"eth":  {"bad.example.com"},
		"poly": {"broken1.example.com", "broken2.example.com"},
	}}
	c := NewBreakerCollector(lister, []domain.ServiceID{"eth", "poly"})

	body := scrape(t, c)
	for _, want := range []string{
		`sage_circuit_breaker_state{domain="bad.example.com",service_id="eth"} 1`,
		`sage_circuit_breaker_state{domain="broken1.example.com",service_id="poly"} 1`,
		`sage_circuit_breaker_state{domain="broken2.example.com",service_id="poly"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q; got:\n%s", want, body)
		}
	}
}

// Absence means healthy. A recovered domain stops being reported rather than
// lingering at 0 — which is what keeps the series count bounded by what is
// broken right now, instead of by every domain ever broken.
func TestBreakerCollector_HealthyDomainsAreAbsent(t *testing.T) {
	lister := &stubLister{broken: map[string][]string{}}
	c := NewBreakerCollector(lister, []domain.ServiceID{"eth", "poly"})

	if body := scrape(t, c); strings.Contains(body, "sage_circuit_breaker_state{") {
		t.Errorf("a healthy gateway must report no series; got:\n%s", body)
	}
}

// The value is derived on scrape, not pushed — which is the point: breaks
// expire lazily, so a pushed gauge would stay at 1 until the domain next broke.
func TestBreakerCollector_ReadsAtScrapeTime(t *testing.T) {
	lister := &stubLister{broken: map[string][]string{"eth": {"bad.example.com"}}}
	c := NewBreakerCollector(lister, []domain.ServiceID{"eth"})

	if body := scrape(t, c); !strings.Contains(body, "bad.example.com") {
		t.Fatal("expected the broken domain on the first scrape")
	}

	// The break expires; the very next scrape must reflect it with no event.
	lister.broken["eth"] = nil
	if body := scrape(t, c); strings.Contains(body, "sage_circuit_breaker_state{") {
		t.Errorf("an expired break must vanish on the next scrape; got:\n%s", body)
	}
	if lister.calls < 2 {
		t.Errorf("lister consulted %d times, want one read per scrape", lister.calls)
	}
}

func TestBreakerCollector_NilListerIsSafe(t *testing.T) {
	c := NewBreakerCollector(nil, []domain.ServiceID{"eth"})
	if body := scrape(t, c); strings.Contains(body, "sage_circuit_breaker_state{") {
		t.Error("a nil lister must report nothing rather than panic")
	}
}
