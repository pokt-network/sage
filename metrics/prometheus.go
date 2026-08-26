// Package metrics provides Prometheus-backed metric recording for the SAGE
// relay pipeline. The Recorder type implements relay/middleware.MetricsRecorder.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
)

// Recorder records relay pipeline metrics to Prometheus.
// It is safe for concurrent use.
type Recorder struct {
	// services bounds the service_id label to the configured set. An empty
	// configuration collapses every ID to __unknown__, which is the honest
	// reading, not a reason to trust the input.
	services *labelPolicy

	relayTotal            *prometheus.CounterVec
	relayLatency          *prometheus.HistogramVec
	retryTotal            *prometheus.CounterVec
	hedgeTotal            *prometheus.CounterVec
	cacheHits             *prometheus.CounterVec
	cacheMisses           *prometheus.CounterVec
	singleflightCoalesced *prometheus.CounterVec
	degradedTotal         *prometheus.CounterVec
	circuitBreaks         *prometheus.CounterVec
	circuitBreakerOutcome *prometheus.CounterVec
	supplierBlacklists    *prometheus.CounterVec
	relayMinerErrors      *prometheus.CounterVec

	// codespaces bounds the relay miner error codespace label, which is a
	// string chosen by the supplier's relay miner.
	codespaces *labelPolicy
}

// NewRecorder creates a Recorder and registers all metrics with
// prometheus.DefaultRegisterer. Panics if registration fails (indicates a
// duplicate registration bug).
//
// knownServices is the set of configured service IDs, and is what bounds the
// service_id label — see labelPolicy. Pass every service the gateway
// serves; anything else is treated as unknown.
func NewRecorder(knownServices []domain.ServiceID) *Recorder {
	r := &Recorder{
		services: allowedLabel(knownServices),
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
		// Two labels by design, not three. PATH's sibling metric carried
		// service × domain × reason × event and reached 233k series — the cross
		// product, not a leaking label. A two-value outcome keeps this at
		// roughly a twelfth of that; domain is a supplier hostname, which is
		// stable across sessions unlike supplier addresses.
		circuitBreakerOutcome: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "circuit_breaker_outcome_total",
				Help:      "Relay outcomes as counted by the circuit breaker's failure-rate gate, keyed on the HOSTNAME the gate uses. outcome is success or failure: numerator and denominator of the rate that decides a break. Only what the gate sees — a broken domain is absent, not healthy.",
			},
			[]string{"service_id", "domain", "outcome"},
		),
		supplierBlacklists: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "supplier_blacklists_total",
				Help:      "Total supplier blacklist events from relay response validation, by service and reason.",
			},
			[]string{"service_id", "reason"},
		),
		relayMinerErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "relay_miner_errors_total",
				Help:      "Total relay responses carrying a RelayMinerError, by service and miner error codespace.",
			},
			[]string{"service_id", "codespace"},
		),
		codespaces: cappedLabel(maxCodespaceLabels),
	}

	prometheus.MustRegister(
		r.relayTotal,
		r.relayLatency,
		r.retryTotal,
		r.hedgeTotal,
		r.cacheHits,
		r.cacheMisses,
		r.singleflightCoalesced,
		r.degradedTotal,
		r.circuitBreaks,
		r.circuitBreakerOutcome,
		r.supplierBlacklists,
		r.relayMinerErrors,
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
	sid := r.services.serviceValue(serviceID)
	status := strconv.Itoa(statusCode)

	r.relayTotal.WithLabelValues(sid, status).Inc()
	r.relayLatency.WithLabelValues(sid).Observe(latency.Seconds())
}

// RecordRetry increments the retry counter for a service with a given reason.
func (r *Recorder) RecordRetry(serviceID domain.ServiceID, reason string) {
	r.retryTotal.WithLabelValues(r.services.serviceValue(serviceID), reason).Inc()
}

// RecordHedge records the outcome of a hedge race (primary_won, hedge_won,
// or both_failed).
func (r *Recorder) RecordHedge(serviceID domain.ServiceID, result string) {
	r.hedgeTotal.WithLabelValues(r.services.serviceValue(serviceID), result).Inc()
}

// RecordCacheHit increments the cache hit counter for a service.
func (r *Recorder) RecordCacheHit(serviceID domain.ServiceID) {
	r.cacheHits.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordCacheMiss increments the cache miss counter for a service.
func (r *Recorder) RecordCacheMiss(serviceID domain.ServiceID) {
	r.cacheMisses.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordSingleflightCoalesced increments the singleflight coalesced counter.
func (r *Recorder) RecordSingleflightCoalesced(serviceID domain.ServiceID) {
	r.singleflightCoalesced.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordDegraded increments the degraded counter for a service and tier label.
func (r *Recorder) RecordDegraded(serviceID domain.ServiceID, tier string) {
	r.degradedTotal.WithLabelValues(r.services.serviceValue(serviceID), tier).Inc()
}

// RecordCircuitBreak increments the circuit break counter for a domain.
func (r *Recorder) RecordCircuitBreak(serviceID domain.ServiceID, domain string) {
	r.circuitBreaks.WithLabelValues(r.services.serviceValue(serviceID), sanitizeLabel(domain)).Inc()
}

// RecordCircuitBreakerOutcome records one outcome the breaker's failure-rate
// gate counted against a domain. It exposes the gate's OWN inputs: the gate
// keys on the full hostname while every relay counter keys on service alone,
// so without this an operator running several relay miners under one domain
// reports one blended rate — a domain whose hosts range from 50% to 80% is
// indistinguishable from one where every host sits at 65%, and those call for
// opposite responses. outcome comes from circuitbreaker's closed set.
func (r *Recorder) RecordCircuitBreakerOutcome(serviceID domain.ServiceID, domain, outcome string) {
	r.circuitBreakerOutcome.WithLabelValues(r.services.serviceValue(serviceID), sanitizeLabel(domain), outcome).Inc()
}

// RecordSupplierBlacklist increments the supplier blacklist counter.
//
// reason comes from a closed set defined in protocol/shannon, not from the
// network, so it needs no bounding.
func (r *Recorder) RecordSupplierBlacklist(serviceID domain.ServiceID, reason string) {
	r.supplierBlacklists.WithLabelValues(r.services.serviceValue(serviceID), reason).Inc()
}

// RecordRelayMinerError increments the counter of relay responses that carried
// an error report from the supplier's relay miner.
//
// codespace is written by that miner, so it is bounded here — see boundedLabel.
func (r *Recorder) RecordRelayMinerError(serviceID domain.ServiceID, codespace string) {
	r.relayMinerErrors.WithLabelValues(r.services.serviceValue(serviceID), r.codespaces.value(codespace)).Inc()
}

// ServeHTTP returns a standard Prometheus HTTP handler suitable for mounting
// at /metrics.
func (r *Recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	promhttp.Handler().ServeHTTP(w, req)
}

// NewPanicCollector exposes safego's recovered-panic count as
// sage_recovered_panics_total.
//
// A CounterFunc rather than a counter the recovery path increments: safego must
// not import this package, because the metrics code itself runs under safego.
// Reading the value at scrape time keeps the dependency pointing one way.
//
// Any non-zero value deserves an alert. Nothing here is expected to panic, and a
// recovered one means a relay or a background task was abandoned partway — the
// gateway stayed up, which is the point, but something is broken.
func NewPanicCollector() prometheus.Collector {
	return prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "sage",
			Name:      "recovered_panics_total",
			Help:      "Panics recovered on background goroutines and hedge/batch arms since start. Non-zero means a bug was contained, not that nothing happened.",
		},
		func() float64 { return float64(safego.Panics()) },
	)
}
