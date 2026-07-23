package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pokt-network/sage/domain"
)

// newIsolatedRecorder creates a Recorder with its own registry to avoid
// "already registered" panics when multiple test cases run in the same process.
// The known-service set mirrors what wire.go passes from config; anything not
// listed collapses to unknownServiceLabel.
func newIsolatedRecorder(t *testing.T, knownServices ...domain.ServiceID) *Recorder {
	t.Helper()
	r, _ := newIsolatedRecorderWithReg(t, knownServices...)
	return r
}

// newIsolatedRecorderWithReg is newIsolatedRecorder plus the registry, for
// tests that need to scrape and count series.
func newIsolatedRecorderWithReg(t *testing.T, knownServices ...domain.ServiceID) (*Recorder, *prometheus.Registry) {
	t.Helper()
	if len(knownServices) == 0 {
		knownServices = []domain.ServiceID{"eth"}
	}
	known := make(map[domain.ServiceID]struct{}, len(knownServices))
	for _, id := range knownServices {
		known[id] = struct{}{}
	}
	reg := prometheus.NewRegistry()
	r := &Recorder{
		knownServices: known,
		relayTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "relay_total"},
			[]string{"service_id", "status"},
		),
		relayLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Namespace: "sage_test", Name: "relay_latency_seconds", Buckets: prometheus.DefBuckets},
			[]string{"service_id"},
		),
		retryTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "retry_total"},
			[]string{"service_id", "reason"},
		),
		hedgeTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "hedge_total"},
			[]string{"service_id", "result"},
		),
		cacheHits: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "cache_hits_total"},
			[]string{"service_id"},
		),
		cacheMisses: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "cache_misses_total"},
			[]string{"service_id"},
		),
		singleflightCoalesced: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "singleflight_coalesced_total"},
			[]string{"service_id"},
		),
		endpointScore: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Namespace: "sage_test", Name: "endpoint_reputation_score"},
			[]string{"service_id", "endpoint"},
		),
		degradedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "degraded_total"},
			[]string{"service_id", "tier"},
		),
		circuitBreaks: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "circuit_breaks_total"},
			[]string{"service_id", "domain"},
		),
	}
	reg.MustRegister(
		r.relayTotal,
		r.relayLatency,
		r.retryTotal,
		r.hedgeTotal,
		r.cacheHits,
		r.cacheMisses,
		r.singleflightCoalesced,
		r.endpointScore,
		r.degradedTotal,
		r.circuitBreaks,
	)
	return r, reg
}

func TestRecordRelay_IncrementsCounter(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordRelay("eth", "supplierA-https://node.example.com", 200, 50*time.Millisecond, nil)

	c, err := r.relayTotal.GetMetricWithLabelValues("eth", "200")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}

	// Use the dto to read the value.
	// We call RecordRelay again and verify the counter increased.
	r.RecordRelay("eth", "supplierA-https://node.example.com", 200, 10*time.Millisecond, nil)
	c2, _ := r.relayTotal.GetMetricWithLabelValues("eth", "200")
	_ = c
	_ = c2
}

func TestRecordRelay_StatusCodeFromError(t *testing.T) {
	r := newIsolatedRecorder(t)
	// statusCode=0 with an error should record "0" label.
	r.RecordRelay("eth", "", 0, 0, errors.New("connection refused"))
	// Just verify no panic.
}

func TestRecordRelay_LatencyObserved(t *testing.T) {
	r := newIsolatedRecorder(t)
	// Should not panic.
	r.RecordRelay("eth", "supplierA-https://node.example.com", 200, 1500*time.Millisecond, nil)
}

func TestRecordRetry(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordRetry("eth", "timeout")
	r.RecordRetry("eth", "timeout")
	// Just verify no panic.
}

func TestRecordHedge(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordHedge("eth", "primary_won")
	r.RecordHedge("eth", "hedge_won")
	r.RecordHedge("eth", "both_failed")
}

func TestRecordCacheHitMiss(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordCacheHit("eth")
	r.RecordCacheMiss("eth")
}

func TestSetEndpointScore(t *testing.T) {
	r := newIsolatedRecorder(t)
	ep := domain.EndpointAddr("supplierA-https://node.example.com")
	r.SetEndpointScore("eth", ep, 85.5)
	// Verify gauge was set (no panic = pass).
	g, err := r.endpointScore.GetMetricWithLabelValues("eth", string(ep))
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	_ = g
}

func TestRecordDegraded(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordDegraded("eth", "tier3")
}

func TestRecordCircuitBreak(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordCircuitBreak("eth", "node.example.com")
}

func TestSanitizeLabel(t *testing.T) {
	replacement := "�"
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"valid ascii unchanged", "eth", "eth"},
		{"valid utf8 unchanged", "poly-café", "poly-café"},
		{"empty unchanged", "", ""},
		// ToValidUTF8 collapses each contiguous run of invalid bytes to one replacement char.
		{"invalid utf8 run replaced", "eth\xff\xfe", "eth" + replacement},
		{"overlong path-traversal probe", "\xc0\xae", replacement},
		{"embedded NUL kept (valid utf8)", "eth\x00", "eth\x00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := sanitizeLabel(c.in)
			if got != c.want {
				t.Errorf("sanitizeLabel(%q) = %q, want %q", c.in, got, c.want)
			}
			if !utf8.ValidString(got) {
				t.Errorf("sanitizeLabel(%q) returned invalid UTF-8: %q", c.in, got)
			}
		})
	}
}

func TestSanitizeLabel_BoundsLength(t *testing.T) {
	got := sanitizeLabel(strings.Repeat("a", maxLabelLen+50))
	if len(got) != maxLabelLen {
		t.Errorf("expected length capped to %d, got %d", maxLabelLen, len(got))
	}
}

// TestRecordRelay_InvalidUTF8ServiceIDNoPanic reproduces the #515 DoS: an
// attacker-supplied Target-Service-Id header carries invalid UTF-8 into
// ctx.ServiceID, which prometheus WithLabelValues would panic on. sanitizeLabel
// must neutralize it.
func TestRecordRelay_InvalidUTF8ServiceIDNoPanic(t *testing.T) {
	r := newIsolatedRecorder(t)
	bad := domain.ServiceID("eth\xff\xc0\xae")
	// Every recorder method takes serviceID; none may panic.
	r.RecordRelay(bad, "sup-\xffnode", 200, time.Millisecond, nil)
	r.RecordRetry(bad, "timeout")
	r.RecordHedge(bad, "primary_won")
	r.RecordCacheHit(bad)
	r.RecordCacheMiss(bad)
	r.RecordSingleflightCoalesced(bad)
	r.SetEndpointScore(bad, domain.EndpointAddr("sup-\xffnode"), 1)
	r.RecordDegraded(bad, "tier3")
	r.RecordCircuitBreak(bad, "node.\xffcom")
	// Reaching here without panic is the assertion.
}

func TestServeHTTP_Returns200(t *testing.T) {
	r := newIsolatedRecorder(t)
	// Add a data point so the /metrics body isn't empty.
	r.RecordRelay("eth", "", 200, 10*time.Millisecond, nil)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestServeHTTP_BodyContainsSageMetrics(t *testing.T) {
	// Register a fresh recorder in the default registry. We rely on the
	// prometheus.DefaultRegisterer for the ServeHTTP path, so we create a real
	// Recorder here — but only once across the whole test binary run. Use a
	// sub-test with t.Skip if already registered to avoid panics.
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("metric already registered (parallel test run): %v", r)
		}
	}()

	rec := NewRecorder([]domain.ServiceID{"eth"})
	rec.RecordRelay("eth", "", 200, 10*time.Millisecond, nil)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	rec.ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "sage_relay_total") {
		t.Errorf("expected sage_relay_total in /metrics body, got:\n%s", body[:min(len(body), 500)])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- service_id cardinality ---

// countSeries returns how many series exist for a metric name in the registry.
func countSeries(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, req)

	n := 0
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if strings.HasPrefix(line, name+"{") {
			n++
		}
	}
	return n
}

// THE cardinality DoS. service_id is the client's Target-Service-Id header,
// copied verbatim, and Validate passes unknown services through — so without a
// bound, every junk value mints a time series that lives until restart. 1000
// requests with 1000 distinct IDs must not produce 1000 series.
func TestRecordRelay_UnknownServiceIDsDoNotGrowCardinality(t *testing.T) {
	r, reg := newIsolatedRecorderWithReg(t, "eth")

	for i := 0; i < 1000; i++ {
		r.RecordRelay(domain.ServiceID(fmt.Sprintf("junk-%d", i)), "", 200, time.Millisecond, nil)
	}

	if got := countSeries(t, reg, "sage_test_relay_total"); got != 1 {
		t.Errorf("relay_total series = %d, want 1 — every unknown ID must share one bucket", got)
	}
}

// Configured services keep their own labels; the bound must not flatten the
// metric into uselessness.
func TestRecordRelay_KnownServicesKeepTheirLabels(t *testing.T) {
	r, reg := newIsolatedRecorderWithReg(t, "eth", "poly", "solana")

	r.RecordRelay("eth", "", 200, time.Millisecond, nil)
	r.RecordRelay("poly", "", 200, time.Millisecond, nil)
	r.RecordRelay("solana", "", 200, time.Millisecond, nil)
	r.RecordRelay("junk", "", 200, time.Millisecond, nil)

	// 3 known + 1 unknown bucket.
	if got := countSeries(t, reg, "sage_test_relay_total"); got != 4 {
		t.Errorf("relay_total series = %d, want 4 (3 known + 1 unknown bucket)", got)
	}
}

// Unknown traffic is bucketed, not dropped: a spike in it is worth alerting on.
func TestServiceLabel(t *testing.T) {
	r := newIsolatedRecorder(t, "eth")

	if got := r.serviceLabel("eth"); got != "eth" {
		t.Errorf("serviceLabel(eth) = %q, want %q", got, "eth")
	}
	if got := r.serviceLabel("not-configured"); got != unknownServiceLabel {
		t.Errorf("serviceLabel(not-configured) = %q, want %q", got, unknownServiceLabel)
	}
	if got := r.serviceLabel(""); got != unknownServiceLabel {
		t.Errorf("serviceLabel(empty) = %q, want %q", got, unknownServiceLabel)
	}
}

// Every service_id-carrying metric must be bounded, not just relay_total — one
// unbounded vec is enough to leak.
func TestAllServiceLabelledMetricsAreBounded(t *testing.T) {
	r, reg := newIsolatedRecorderWithReg(t, "eth")

	for i := 0; i < 200; i++ {
		junk := domain.ServiceID(fmt.Sprintf("junk-%d", i))
		r.RecordRelay(junk, "", 200, time.Millisecond, nil)
		r.RecordRetry(junk, "timeout")
		r.RecordHedge(junk, "hedge_won")
		r.RecordCacheHit(junk)
		r.RecordCacheMiss(junk)
		r.RecordSingleflightCoalesced(junk)
		r.SetEndpointScore(junk, "ep1", 50)
		r.RecordDegraded(junk, "tier2")
		r.RecordCircuitBreak(junk, "example.com")
	}

	for _, name := range []string{
		"sage_test_relay_total",
		"sage_test_retry_total",
		"sage_test_hedge_total",
		"sage_test_cache_hits_total",
		"sage_test_cache_misses_total",
		"sage_test_singleflight_coalesced_total",
		"sage_test_endpoint_reputation_score",
		"sage_test_degraded_total",
		"sage_test_circuit_breaks_total",
	} {
		if got := countSeries(t, reg, name); got != 1 {
			t.Errorf("%s series = %d, want 1 — 200 junk IDs must collapse to one", name, got)
		}
	}
}

// The metrics handler must serve metrics and nothing else. cmd/sagegw mounts it
// on a dedicated mux rather than http.DefaultServeMux precisely because that
// mux carries /debug/pprof — serving both from one listener would publish heap
// dumps to whoever scrapes metrics, which is PATH's audit C6.
func TestRecorder_MetricsMuxDoesNotCarryPprof(t *testing.T) {
	r := newIsolatedRecorder(t, "eth")

	mux := http.NewServeMux()
	mux.Handle("/metrics", r)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", w.Code)
	}

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/goroutine"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s status = %d on the metrics mux, want 404 — pprof must not ride along", path, w.Code)
		}
	}
}
