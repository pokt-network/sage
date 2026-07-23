// Package metrics provides Prometheus-backed metric recording for the SAGE
// relay pipeline. The Recorder type implements relay/middleware.MetricsRecorder.
package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pokt-network/sage/domain"
)

// maxLabelLen bounds every externally-derived Prometheus label value. Service
// IDs and endpoint addresses are short in practice; the cap is a defensive
// ceiling, not an expected length.
const maxLabelLen = 128

// sanitizeLabel makes an externally-derived string safe to pass to prometheus
// WithLabelValues. client_golang panics inside WithLabelValues when a label
// value is not valid UTF-8, and SAGE copies the attacker-controlled
// Target-Service-Id header verbatim into ctx.ServiceID (see relay/middleware/
// parse.go) — an invalid byte sequence or embedded NUL would otherwise crash
// the request goroutine. Bound length first (byte-level truncation can split a
// multibyte rune) then replace any invalid sequence with the Unicode
// replacement char. Final safety net on every externally-derived label value.
func sanitizeLabel(s string) string {
	if len(s) > maxLabelLen {
		s = s[:maxLabelLen]
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	return s
}

// unknownServiceLabel replaces any service_id that is not configured.
//
// A Prometheus label value must come from a bounded set or it is a memory leak
// with a network interface. service_id does not qualify on its own: it is the
// client's Target-Service-Id header, copied verbatim in relay/middleware/
// parse.go, and Validate deliberately passes unknown services through rather
// than rejecting them. The Metrics middleware also sits outside Validate in the
// chain, so it records before any rejection could happen. Every distinct junk
// value would otherwise mint a new time series that lives until restart.
//
// Collapsing to one bucket keeps the cardinality of service_id at
// len(configured services) + 1, and keeps the traffic visible rather than
// dropping it: a spike here is worth an alert.
const unknownServiceLabel = "__unknown__"

// serviceLabel bounds the service_id label to the configured set.
//
// sanitizeLabel is still applied to known IDs: it caps length and repairs
// invalid UTF-8, which client_golang panics on. That guards against a hostile
// header; this guards against an unbounded one. They are different problems and
// the first fix did not address the second.
func (r *Recorder) serviceLabel(serviceID domain.ServiceID) string {
	if _, ok := r.knownServices[serviceID]; ok {
		return sanitizeLabel(string(serviceID))
	}
	return unknownServiceLabel
}

// Recorder records relay pipeline metrics to Prometheus.
// It is safe for concurrent use.
type Recorder struct {
	// knownServices bounds the service_id label. Nil means no service is
	// configured, so every ID collapses to unknownServiceLabel — which is the
	// honest reading, not a reason to trust the input.
	knownServices map[domain.ServiceID]struct{}

	relayTotal            *prometheus.CounterVec
	relayLatency          *prometheus.HistogramVec
	retryTotal            *prometheus.CounterVec
	hedgeTotal            *prometheus.CounterVec
	cacheHits             *prometheus.CounterVec
	cacheMisses           *prometheus.CounterVec
	singleflightCoalesced *prometheus.CounterVec
	endpointScore         *prometheus.GaugeVec
	degradedTotal         *prometheus.CounterVec
	circuitBreaks         *prometheus.CounterVec
}

// NewRecorder creates a Recorder and registers all metrics with
// prometheus.DefaultRegisterer. Panics if registration fails (indicates a
// duplicate registration bug).
//
// knownServices is the set of configured service IDs, and is what bounds the
// service_id label — see unknownServiceLabel. Pass every service the gateway
// serves; anything else is treated as unknown.
func NewRecorder(knownServices []domain.ServiceID) *Recorder {
	known := make(map[domain.ServiceID]struct{}, len(knownServices))
	for _, id := range knownServices {
		known[id] = struct{}{}
	}

	r := &Recorder{
		knownServices: known,
		relayTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "relay_total",
				Help:      "Total relay attempts, partitioned by service and HTTP status.",
			},
			[]string{"service_id", "status"},
		),
		relayLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "relay_latency_seconds",
				Help:      "Relay latency in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"service_id"},
		),
		retryTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "retry_total",
				Help:      "Total relay retries, partitioned by service and reason.",
			},
			[]string{"service_id", "reason"},
		),
		hedgeTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "hedge_total",
				Help:      "Hedge race outcomes (primary_won, hedge_won, both_failed).",
			},
			[]string{"service_id", "result"},
		),
		cacheHits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "cache_hits_total",
				Help:      "Total response cache hits.",
			},
			[]string{"service_id"},
		),
		cacheMisses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "cache_misses_total",
				Help:      "Total response cache misses.",
			},
			[]string{"service_id"},
		),
		singleflightCoalesced: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "singleflight_coalesced_total",
				Help:      "Total requests coalesced by the singleflight deduplicator.",
			},
			[]string{"service_id"},
		),
		endpointScore: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "sage",
				Name:      "endpoint_reputation_score",
				Help:      "Current reputation score for an endpoint.",
			},
			[]string{"service_id", "endpoint"},
		),
		degradedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "degraded_total",
				Help:      "Total requests served in degraded mode, by service and tier.",
			},
			[]string{"service_id", "tier"},
		),
		circuitBreaks: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "circuit_breaks_total",
				Help:      "Total circuit breaker open events, by service and domain.",
			},
			[]string{"service_id", "domain"},
		),
	}

	prometheus.MustRegister(
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

	return r
}

// RecordRelay satisfies relay/middleware.MetricsRecorder.
// statusCode 0 is recorded as "0" (unknown/connection-level error).
func (r *Recorder) RecordRelay(
	serviceID domain.ServiceID,
	_ domain.EndpointAddr,
	statusCode int,
	latency time.Duration,
	_ error,
) {
	sid := r.serviceLabel(serviceID)
	status := strconv.Itoa(statusCode)

	r.relayTotal.WithLabelValues(sid, status).Inc()
	r.relayLatency.WithLabelValues(sid).Observe(latency.Seconds())
}

// RecordRetry increments the retry counter for a service with a given reason.
func (r *Recorder) RecordRetry(serviceID domain.ServiceID, reason string) {
	r.retryTotal.WithLabelValues(r.serviceLabel(serviceID), reason).Inc()
}

// RecordHedge records the outcome of a hedge race (primary_won, hedge_won,
// or both_failed).
func (r *Recorder) RecordHedge(serviceID domain.ServiceID, result string) {
	r.hedgeTotal.WithLabelValues(r.serviceLabel(serviceID), result).Inc()
}

// RecordCacheHit increments the cache hit counter for a service.
func (r *Recorder) RecordCacheHit(serviceID domain.ServiceID) {
	r.cacheHits.WithLabelValues(r.serviceLabel(serviceID)).Inc()
}

// RecordCacheMiss increments the cache miss counter for a service.
func (r *Recorder) RecordCacheMiss(serviceID domain.ServiceID) {
	r.cacheMisses.WithLabelValues(r.serviceLabel(serviceID)).Inc()
}

// RecordSingleflightCoalesced increments the singleflight coalesced counter.
func (r *Recorder) RecordSingleflightCoalesced(serviceID domain.ServiceID) {
	r.singleflightCoalesced.WithLabelValues(r.serviceLabel(serviceID)).Inc()
}

// SetEndpointScore updates the gauge for an endpoint's reputation score.
func (r *Recorder) SetEndpointScore(serviceID domain.ServiceID, endpoint domain.EndpointAddr, score float64) {
	r.endpointScore.WithLabelValues(r.serviceLabel(serviceID), sanitizeLabel(string(endpoint))).Set(score)
}

// RecordDegraded increments the degraded counter for a service and tier label.
func (r *Recorder) RecordDegraded(serviceID domain.ServiceID, tier string) {
	r.degradedTotal.WithLabelValues(r.serviceLabel(serviceID), tier).Inc()
}

// RecordCircuitBreak increments the circuit break counter for a domain.
func (r *Recorder) RecordCircuitBreak(serviceID domain.ServiceID, domain string) {
	r.circuitBreaks.WithLabelValues(r.serviceLabel(serviceID), sanitizeLabel(domain)).Inc()
}

// ServeHTTP returns a standard Prometheus HTTP handler suitable for mounting
// at /metrics.
func (r *Recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	promhttp.Handler().ServeHTTP(w, req)
}
