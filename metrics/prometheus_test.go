package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
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
// listed collapses to unknownLabel.
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
	reg := prometheus.NewRegistry()
	r := &Recorder{
		services: allowedLabel(knownServices),
		relayTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "relay_total"},
			[]string{"service_id", "status", "request_type"},
		),
		relayLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Namespace: "sage_test", Name: "relay_latency_seconds", Buckets: prometheus.DefBuckets},
			[]string{"service_id", "request_type"},
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
		degradedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "degraded_total"},
			[]string{"service_id", "tier"},
		),
		circuitBreaks: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "circuit_breaks_total"},
			[]string{"service_id", "domain"},
		),
		healthCheckSkipped: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "health_check_skipped_total"},
			[]string{"service_id"},
		),
		circuitBreakerOutcome: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "circuit_breaker_outcome_total"},
			[]string{"service_id", "domain", "outcome"},
		),
		methodBlockEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "method_block_events_total"},
			[]string{"service_id", "method", "event"},
		),
		reputationAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "sage_test", Name: "reputation_attempts_total"},
			[]string{"service_id", "rpc_type", "signal", "probe"},
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
		r.degradedTotal,
		r.circuitBreaks,
		r.reputationAttempts,
		r.healthCheckSkipped,
	)
	// Mirrors NewRecorder: the skipped counter is pre-registered at zero so
	// its absence is never mistaken for not skipping.
	r.initHealthCheckSkipped(knownServices)
	return r, reg
}

func TestRecordRelay_IncrementsCounter(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordRelay("eth", "supplierA-https://node.example.com", 200, 50*time.Millisecond, nil)
	r.RecordRelay("eth", "supplierA-https://node.example.com", 200, 10*time.Millisecond, nil)

	c, err := r.relayTotal.GetMetricWithLabelValues("eth", "200", "client")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	if got := value(t, c); got != 2 {
		t.Errorf("relay_total{status=200,request_type=client} = %v, want 2", got)
	}
}

// A health-check probe is a paid relay, so it belongs in the same counter as a
// client attempt — and it is not client traffic, so it must not be mixed into
// the series a client-facing error rate is built from.
func TestRecordProbeRelay_SeparateFromClientAttempts(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordRelay("eth", "supplierA-https://node.example.com", 200, 10*time.Millisecond, nil)
	r.RecordProbeRelay("eth", "supplierA-https://node.example.com", 200, 20*time.Millisecond, nil)
	r.RecordProbeRelay("eth", "supplierA-https://node.example.com", 200, 30*time.Millisecond, nil)

	client, err := r.relayTotal.GetMetricWithLabelValues("eth", "200", "client")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues(client): %v", err)
	}
	if got := value(t, client); got != 1 {
		t.Errorf("relay_total{request_type=client} = %v, want 1 — probes must not land here", got)
	}

	probe, err := r.relayTotal.GetMetricWithLabelValues("eth", "200", "probe")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues(probe): %v", err)
	}
	if got := value(t, probe); got != 2 {
		t.Errorf("relay_total{request_type=probe} = %v, want 2", got)
	}

	// The histogram splits the same way, which is what lets a latency panel
	// exclude synthetic checks.
	if _, err := r.relayLatency.GetMetricWithLabelValues("eth", "probe"); err != nil {
		t.Fatalf("relay_latency_seconds has no probe series: %v", err)
	}
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
	// The real construction path, not the isolated builder: a counter that is
	// read as a ratio has to be exported before it is ever incremented, and
	// nothing here has skipped a health check.
	if !strings.Contains(body, `sage_health_check_skipped_total{service_id="eth"} 0`) {
		t.Error("sage_health_check_skipped_total is not exported at zero; a query for it returns empty, not 0")
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
// scrapeValues is countSeries when the value matters, not just the count: it
// returns each exported series of name keyed by its label set, exactly as the
// scrape renders it. It reads the registry rather than the vec so it cannot
// mint the series it is asked about.
func scrapeValues(t *testing.T, reg *prometheus.Registry, name string) map[string]float64 {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, req)

	out := make(map[string]float64)
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		labels, value, ok := strings.Cut(strings.TrimPrefix(line, name), " ")
		if !ok {
			t.Fatalf("unparseable scrape line: %q", line)
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatalf("value in %q: %v", line, err)
		}
		out[labels] = v
	}
	return out
}

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

	if got := r.services.serviceValue("eth"); got != "eth" {
		t.Errorf("serviceValue(eth) = %q, want %q", got, "eth")
	}
	if got := r.services.serviceValue("not-configured"); got != unknownLabel {
		t.Errorf("serviceValue(not-configured) = %q, want %q", got, unknownLabel)
	}
	if got := r.services.serviceValue(""); got != unknownLabel {
		t.Errorf("serviceValue(empty) = %q, want %q", got, unknownLabel)
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

func TestRecordCircuitBreakerOutcome(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordCircuitBreakerOutcome("eth", "node.example.com", "success")
	r.RecordCircuitBreakerOutcome("eth", "node.example.com", "failure")
}

func TestRecordMethodBlockEvent(t *testing.T) {
	r := newIsolatedRecorder(t)
	r.RecordMethodBlockEvent("eth", "eth_getLogs", "mark")
}

// One recorded reputation signal is one series increment, and probe traffic is
// separable from client traffic — the whole point of the probe label is that
// "40000 attempts, all of them health checks" reads differently from "40000
// attempts from real requests".
func TestRecorder_ReputationAttempts(t *testing.T) {
	r, reg := newIsolatedRecorderWithReg(t, "svc")

	r.RecordReputationAttempt("svc", "json_rpc", "critical_error", false)
	r.RecordReputationAttempt("svc", "json_rpc", "success", true)

	values := scrapeValues(t, reg, "sage_test_reputation_attempts_total")
	if n := len(values); n != 2 {
		t.Fatalf("reputation_attempts_total series = %d, want 2: %v", n, values)
	}
	for _, tc := range []struct{ labels string }{
		{`{probe="false",rpc_type="json_rpc",service_id="svc",signal="critical_error"}`},
		{`{probe="true",rpc_type="json_rpc",service_id="svc",signal="success"}`},
	} {
		if got, ok := values[tc.labels]; !ok || got != 1 {
			t.Errorf("series %s = %v (present=%v), want 1", tc.labels, got, ok)
		}
	}
}

// A counter read as a ratio against a baseline must exist before it is
// non-zero. Prometheus exports no child of a CounterVec until one is
// incremented, so "no series" would be indistinguishable from "not skipping" —
// and an alert shaped on sum(...) == 0 would never match. Reported from the
// canary on 2026-09-03, where the post-roll check for this metric was
// unmeasurable for exactly this reason.
func TestRecorder_HealthCheckSkippedSeriesExistAtZero(t *testing.T) {
	services := []domain.ServiceID{"eth", "poly"}
	rec, reg := newIsolatedRecorderWithReg(t, services...)

	got := scrapeValues(t, reg, "sage_test_health_check_skipped_total")
	if len(got) != len(services) {
		t.Fatalf("scraped %d series before any skip, want one per configured service (%d): %v",
			len(got), len(services), got)
	}
	for labels, v := range got {
		if v != 0 {
			t.Errorf("%s = %v at startup, want 0", labels, v)
		}
	}

	// And it still counts.
	rec.RecordHealthCheckSkipped("eth")
	after := scrapeValues(t, reg, "sage_test_health_check_skipped_total")
	if after[`{service_id="eth"}`] != 1 {
		t.Errorf("after one skip: %v, want eth at 1", after)
	}
	if after[`{service_id="poly"}`] != 0 {
		t.Errorf("poly moved on eth's skip: %v", after)
	}
}
