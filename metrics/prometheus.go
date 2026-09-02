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
	clientRequestsTotal   *prometheus.CounterVec
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
	methodBlockEvents     *prometheus.CounterVec
	reputationAttempts    *prometheus.CounterVec
	healthCheckResults    *prometheus.CounterVec

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
				Help:      "Upstream relay attempts by service, HTTP status and request_type (client|probe): one count per attempt inside retry, hedge and batch, so a retried, hedged or batched request counts more than once. request_type=\"probe\" is a health-check relay, which is paid for like any other but is not client traffic — filter on request_type=\"client\" for client-facing rates. Cache hits and coalesced requests make no attempt and are absent. One count per client request is sage_client_requests_total.",
			},
			[]string{"service_id", "status", "request_type"},
		),
		clientRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "client_requests_total",
				Help:      "Client-facing relay requests by service and the HTTP status returned to the client. Unlike relay_total (per relay attempt), this is one count per client request and matches what an edge or client sees — a JSON-RPC error is an HTTP 200 here.",
			},
			[]string{"service_id", "status"},
		),
		relayLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "relay_latency_seconds",
				Help:      "Upstream relay attempt latency in seconds — selection through response, one observation per attempt, split by request_type (client|probe). Not client-facing latency: a request that retried or hedged is several observations, none of them its total, and a health-check probe is not a client request at all.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"service_id", "request_type"},
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
		// No domain label on purpose: the gauge above names the host, and a
		// counter keyed on host is the series growth PATH's cardinality
		// incident was about. method is the plugin catalogue; event is a
		// closed set from relay/middleware.
		methodBlockEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "method_block_events_total",
				Help:      "Method-block events by service and method. event is mark (a host was blocked for a method), escalate (a host was blocked for every method), or bypass (every host was blocked for the method, or no surviving host was vouched for by reputation — a recorded score at or above the probation threshold — so the unfiltered pool was used). mark also counts an attempt that landed no block (empty host, or marking disabled by TTL <= 0) — it counts the middleware's attempt to mark, not that a mark landed.",
			},
			[]string{"service_id", "method", "event"},
		),
		// No key label: reputation keys are backend URLs, which is the
		// unbounded dimension. rpc_type and signal are closed sets and probe
		// is a boolean, so the series count per service is fixed.
		reputationAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "reputation_attempts_total",
				Help:      "Reputation signals recorded, by service, RPC type, signal type and whether the signal came from a health-check probe (probe=true) or client traffic (probe=false). One signal is one relay attempt, or one batch collapsed to its worst outcome per endpoint; client-attributed outcomes are not recorded and so are not counted.",
			},
			[]string{"service_id", "rpc_type", "signal", "probe"},
		),
	}

	r.healthCheckResults = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sage",
			Name:      "health_check_results_total",
			Help:      "Health-check probe results applied on this replica, by service and source: probe (this replica sent the relay — the leader) or stream (another replica sent it and published the result). On a healthy fleet only the leader shows probe; the stream count is the relay saving made visible.",
		},
		[]string{"service_id", "source"},
	)

	prometheus.MustRegister(
		r.healthCheckResults,
		r.relayTotal,
		r.clientRequestsTotal,
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
		r.methodBlockEvents,
		r.reputationAttempts,
	)

	return r
}

// Relay attempt kinds, the request_type label on relay_total and
// relay_latency_seconds. Health-check probes are billed relays like any
// other, so they belong in the same counter; they are not client traffic, so
// they must be separable from it. PATH splits the same way, via
// path_relays_total{request_type}.
const (
	requestTypeClient = "client"
	requestTypeProbe  = "probe"
)

// RecordRelay satisfies relay/middleware.MetricsRecorder: one upstream
// attempt on the client path. The metrics middleware sits inside
// retry/hedge/batch and outside select_endpoint (relay/chain_order.go), which
// is what makes this per attempt. statusCode 0 is recorded as "0"
// (unknown/connection-level error).
func (r *Recorder) RecordRelay(
	serviceID domain.ServiceID,
	_ domain.EndpointAddr,
	statusCode int,
	latency time.Duration,
	_ error,
) {
	r.recordRelayAttempt(serviceID, statusCode, latency, requestTypeClient)
}

// RecordProbeRelay records one health-check relay attempt into the same
// counter and histogram as client attempts, under request_type="probe".
//
// Probes do not run through the middleware chain — healthcheck.Executor calls
// protocol.SendRelay directly — so without this they appear in no relay
// metric at all, and the probe share of what the gateway spends on relays is
// invisible. Only the probing replica records: a follower applying another
// pod's streamed result sent nothing.
func (r *Recorder) RecordProbeRelay(
	serviceID domain.ServiceID,
	_ domain.EndpointAddr,
	statusCode int,
	latency time.Duration,
	_ error,
) {
	r.recordRelayAttempt(serviceID, statusCode, latency, requestTypeProbe)
}

func (r *Recorder) recordRelayAttempt(
	serviceID domain.ServiceID,
	statusCode int,
	latency time.Duration,
	requestType string,
) {
	sid := r.services.serviceValue(serviceID)
	status := strconv.Itoa(statusCode)

	r.relayTotal.WithLabelValues(sid, status, requestType).Inc()
	r.relayLatency.WithLabelValues(sid, requestType).Observe(latency.Seconds())
}

// RecordClientRequest records the client-facing HTTP status of one relay
// request — one count per request, matching what a client or edge dashboard
// sees. A JSON-RPC error is HTTP 200 here; only a real HTTP-level failure is
// 4xx/5xx. Distinct from RecordRelay, which counts each relay ATTEMPT.
func (r *Recorder) RecordClientRequest(serviceID domain.ServiceID, status int) {
	r.clientRequestsTotal.WithLabelValues(r.services.serviceValue(serviceID), strconv.Itoa(status)).Inc()
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

// RecordHealthCheckResult counts one applied probe result. source is the
// closed set healthcheck.ResultSource.
func (r *Recorder) RecordHealthCheckResult(serviceID domain.ServiceID, source string) {
	r.healthCheckResults.WithLabelValues(r.services.serviceValue(serviceID), source).Inc()
}

// RecordMethodBlockEvent counts one method-block event. method comes from
// the plugin's bounded catalogue and event from a closed set, so neither
// needs bounding here.
func (r *Recorder) RecordMethodBlockEvent(serviceID domain.ServiceID, method, event string) {
	r.methodBlockEvents.WithLabelValues(r.services.serviceValue(serviceID), method, event).Inc()
}

// RecordReputationAttempt counts one recorded reputation signal. signal is
// the closed set of reputation.SignalType values; rpcType the closed
// domain.RPCType set. Neither needs bounding here.
func (r *Recorder) RecordReputationAttempt(serviceID domain.ServiceID, rpcType, signal string, probe bool) {
	r.reputationAttempts.WithLabelValues(
		r.services.serviceValue(serviceID),
		rpcType,
		signal,
		strconv.FormatBool(probe),
	).Inc()
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
