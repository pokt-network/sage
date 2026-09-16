// Package metrics provides Prometheus-backed metric recording for the SAGE
// relay pipeline. The Recorder type implements relay/middleware.MetricsRecorder.
package metrics

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
)

// relayLatencyBuckets extends the default buckets past ten seconds.
//
// prometheus.DefBuckets tops out at 10s, so every slower attempt lands in +Inf
// and nothing distinguishes eleven seconds from three hundred. On the mainnet
// canary 4.8% of observations were in that bucket over a 17h window, which put
// the merged p99 above 10s and left it unimprovable: a p99 sitting in the
// overflow bucket cannot be moved by any change, because no change to it is
// measurable. Raised by ops on 2026-09-02.
//
// The added edges are the ones that mean something here rather than a round
// series: 15s and 20s bracket where a slow supplier stops being usable, 30s is
// the default relay timeout so the bucket below it is "made it, barely" while
// anything above can only be a hedge or retry outliving its own deadline, and
// 60s catches an attempt running with no deadline at all.
var relayLatencyBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 60,
}

// Recorder records relay pipeline metrics to Prometheus.
// It is safe for concurrent use.
type Recorder struct {
	// services bounds the service_id label to the configured set. An empty
	// configuration collapses every ID to __unknown__, which is the honest
	// reading, not a reason to trust the input.
	services *labelPolicy

	// clientRequestHook, when set, is told every client-facing status as it is
	// counted. Wire time only; the auto-drain engine reads it so its gate sees
	// what callers saw rather than what one attempt did.
	clientRequestHook atomic.Pointer[func(domain.ServiceID, int)]

	relayTotal            *prometheus.CounterVec
	clientRequestsTotal   *prometheus.CounterVec
	rpcTypeTotal          *prometheus.CounterVec
	rpcTypeMismatchTotal  *prometheus.CounterVec
	relayLatency          *prometheus.HistogramVec
	retryTotal            *prometheus.CounterVec
	retryResolutionTotal  *prometheus.CounterVec
	hedgeTotal            *prometheus.CounterVec
	cacheHits             *prometheus.CounterVec
	cacheMisses           *prometheus.CounterVec
	singleflightCoalesced *prometheus.CounterVec
	degradedTotal         *prometheus.CounterVec
	circuitBreaks         *prometheus.CounterVec
	circuitBreakerOutcome *prometheus.CounterVec
	supplierBlacklists    *prometheus.CounterVec
	relayMinerErrors      *prometheus.CounterVec
	oversizedResponses    *prometheus.CounterVec
	autoDrains            *prometheus.CounterVec
	methodBlockEvents     *prometheus.CounterVec
	reputationAttempts    *prometheus.CounterVec
	heuristicVerdicts     *prometheus.CounterVec
	externalSourceFails   *prometheus.CounterVec
	clientLatency         *prometheus.HistogramVec
	stageSeconds          *prometheus.CounterVec
	healthCheckResults    *prometheus.CounterVec
	healthCheckSkipped    *prometheus.CounterVec
	healthCheckCycle      prometheus.Histogram
	healthCheckLastCycle  *prometheus.GaugeVec
	healthCheckOverruns   prometheus.Counter

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
		rpcTypeTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "rpc_type_total",
				Help:      "Client requests by the RPC type SAGE settled on and how: source=\"header\" when the client declared it with RPC-Type, \"detected\" when Parse inferred it from the verb, path and body. One count per request that reached classification; a request refused before that (no Target-Service-Id, oversized body, unparseable RPC-Type value) is absent. The source=\"detected\" share is the traffic whose routing rests on detection alone.",
			},
			[]string{"service_id", "rpc_type", "source"},
		),
		rpcTypeMismatchTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "rpc_type_mismatch_total",
				Help:      "Client requests whose RPC type classification was contradicted by a better informed party, by reason. header: the client sent RPC-Type=actual and detection would have said rpc_type — the one place detection is graded against ground truth. plugin: the service's QoS plugin parsed the payload as actual, not the rpc_type Parse settled on, so the request was validated and pooled as one surface and sent as another (a JSON-RPC body carrying a CometBFT method on a cosmos service, for one). unsupported: the service does not declare rpc_type, so Validate refused the request with 400 (actual=\"none\"). Non-zero is a detection rule, a plugin table or a service's rpc_types to fix; the log line \"rpc type not declared by service\" names the path for the last case.",
			},
			[]string{"service_id", "rpc_type", "actual", "reason"},
		),
		relayLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "relay_latency_seconds",
				Help:      "Upstream relay attempt latency in seconds — selection through response, one observation per attempt, split by request_type (client|probe). Not client-facing latency: a request that retried or hedged is several observations, none of them its total, and a health-check probe is not a client request at all. Buckets run past the default 10s to 60s, so a p99 in the tail is a number rather than +Inf.",
				Buckets:   relayLatencyBuckets,
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
		retryResolutionTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "retry_resolution_total",
				Help:      "How relay retries ended, by the verdict that caused them: recovered (a later attempt succeeded) or exhausted.",
			},
			[]string{"service_id", "reason", "outcome"},
		),
		hedgeTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "hedge_total",
				Help:      "Hedge race outcomes (primary_won, hedge_won, both_failed), plus suppressed_large_batch: an item of a batch over retry_config.hedge_max_batch_size that ran unhedged.",
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
				Help:      "Total requests served in degraded mode, by service and tier: reputation_pool_collapse when the pool-collapse guard served a below-floor endpoint, response when an answer went out with X-Degraded set.",
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
		autoDrains: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "auto_drain_total",
				Help:      "Auto-drain engine decisions, by service, RPC type and outcome: drained, shadow (would have drained), suppressed, rate_limited, capped, no_vouched_alternative, manual_drain. One per decision change, not per evaluation tick. The evidence behind each is in GET /admin/auto-drain/events.",
			},
			[]string{"service_id", "rpc_type", "outcome"},
		),
		oversizedResponses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "oversized_responses_total",
				Help:      "Total supplier responses abandoned for exceeding the response ceiling (router.max_response_body_bytes, knob relay.max_response_mb), by service.",
			},
			[]string{"service_id"},
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
				Help:      "Method-block events by service and method. event is mark (a host was blocked for a method), family (a -32601 on a catalogued method blocked the host for the whole family the plugin named, e.g. every EVM method on the EVM face of a Cosmos chain), escalate (a host was blocked for every method), or bypass (every host was blocked for the method, or no surviving host was vouched for by reputation — a recorded score at or above the probation threshold — so the unfiltered pool was used). mark also counts an attempt that landed no block (empty host, or marking disabled by TTL <= 0) — it counts the middleware's attempt to mark, not that a mark landed.",
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
		// reason and attribution are closed sets fixed in the heuristic
		// package; rpc_type likewise. Client attempts only: probes do not
		// run through the middleware chain (see RecordProbeRelay).
		heuristicVerdicts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "heuristic_verdicts_total",
				Help:      "Heuristic verdicts on upstream answers to client relay attempts, by service, RPC type, the reason the verdict settled on (success, internal_error, http_408, transport_timeout, ...) and the side it attributed the outcome to (supplier, blockchain, client, unknown; none on success). One verdict per attempt, so a retried request counts once per attempt. This is the complete account of what the gateway concluded about every answer; reputation_attempts_total counts only what scoring kept and retry_total only what retried.",
			},
			[]string{"service_id", "rpc_type", "reason", "attribution"},
		),
		externalSourceFails: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "external_block_source_failures_total",
				Help:      "Polls of a service's external_block_sources that produced no height (every configured source failed that tick), by service. While this rises the service's external floor under the perceived head is not lifted; relays are unaffected. A steady rate on one service is a dead or misconfigured source.",
			},
			[]string{"service_id"},
		),
		clientLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "client_latency_seconds",
				Help:      "Client-facing latency in seconds: from the request reaching the router to the response written (or the client leaving), one observation per client request, by service and the status the client saw. This is what the caller waits, retries and hedges included; relay_latency_seconds is per upstream attempt. Same buckets, to 60s.",
				Buckets:   relayLatencyBuckets,
			},
			[]string{"service_id", "status"},
		),
		// stage is the registered middleware name (relay/chain_order.go) or
		// router_write: a closed set.
		stageSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "stage_seconds_total",
				Help:      "Seconds spent in each middleware stage, exclusive of the stages nested inside it, summed over client requests, by service and stage (the registered middleware name, or router_write for the response write). Divide by sage_client_requests_total for the mean per request. send_relay is the upstream call and send_relay.prepare / .sign / .http / .verify are its breakdown (not additions); everything else is SAGE's own time — the split the per-attempt relay latency cannot show.",
			},
			[]string{"service_id", "stage"},
		),
	}

	r.healthCheckResults = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sage",
			Name:      "health_check_results_total",
			Help:      "Health-check probe results applied on this replica, by service and source: probe (this replica sent the relay — the leader), stream (another replica sent it and published the result) or peer (another SAGE instance sent it, read through active_health_checks.peer_probe_stream). On a healthy fleet only the leader shows probe; stream and peer are the relay saving made visible.",
		},
		[]string{"service_id", "source"},
	)

	r.healthCheckSkipped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sage",
			Name:      "health_check_skipped_total",
			Help:      "Health checks not sent because client traffic had already graded the backend this cycle (the traffic_informed_probing flag). Every client attempt records a reputation signal, so a busy backend is graded continuously and its probe would buy a second copy of the same fact. Against sage_health_check_results_total{source=\"probe\"} this is the relay saving; it stays at zero while the flag is off and while the pod is not yet warm.",
		},
		[]string{"service_id"},
	)

	r.healthCheckCycle = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "sage",
			Name:      "health_check_cycle_seconds",
			Help:      "Wall time for one health-check cycle: every configured service walked and every due probe dispatched. This is the fleet's REAL probe cadence, which is the longer of active_health_checks.interval and this — the cycle runs on the ticker goroutine and dispatch blocks on a fixed worker pool, so a cycle that overruns its tick simply delays the next one and the configured interval is not achieved. Compare against the interval before trusting any per-service probe rate.",
			Buckets:   []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200},
		},
	)

	r.healthCheckLastCycle = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "health_check_last_cycle_probes",
			Help:      "Probes issued for this service in the last COMPLETED health-check cycle. A gauge rather than a rate because probes arrive as a burst: with a short cycle inside a long interval, every probe for a service lands within a second or two and any rate over a window shorter than the interval alternates between the whole burst and zero. Divide by sage_health_check_cycle_seconds' period for a rate that means something, or read this directly for what one pass actually costs.",
		},
		[]string{"service_id"},
	)

	r.healthCheckOverruns = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "sage",
			Name:      "health_check_cycle_overruns_total",
			Help:      "Health-check cycles that took longer than the tick they were scheduled on, so the next tick was dropped. Non-zero means the configured interval is not the cadence being achieved and probes are arriving in bursts one cycle apart; the fix is more workers or a faster probe path, not a shorter interval.",
		},
	)

	prometheus.MustRegister(
		r.healthCheckResults,
		r.healthCheckSkipped,
		r.healthCheckCycle,
		r.healthCheckLastCycle,
		r.healthCheckOverruns,
		r.relayTotal,
		r.clientRequestsTotal,
		r.rpcTypeTotal,
		r.rpcTypeMismatchTotal,
		r.relayLatency,
		r.retryTotal,
		r.retryResolutionTotal,
		r.hedgeTotal,
		r.cacheHits,
		r.cacheMisses,
		r.singleflightCoalesced,
		r.degradedTotal,
		r.circuitBreaks,
		r.circuitBreakerOutcome,
		r.supplierBlacklists,
		r.relayMinerErrors,
		r.oversizedResponses,
		r.autoDrains,
		r.methodBlockEvents,
		r.reputationAttempts,
		r.heuristicVerdicts,
		r.externalSourceFails,
		r.clientLatency,
		r.stageSeconds,
	)

	r.initHealthCheckSkipped(knownServices)
	r.initHealthCheckLastCycle(knownServices)

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
	if fn := r.clientRequestHook.Load(); fn != nil {
		(*fn)(serviceID, status)
	}
}

// SetClientRequestHook installs a callback run on every client-facing status,
// after it is counted. Wire time only: it is read on the response path of every
// request, so it must not block.
func (r *Recorder) SetClientRequestHook(fn func(domain.ServiceID, int)) {
	if fn == nil {
		r.clientRequestHook.Store(nil)
		return
	}
	r.clientRequestHook.Store(&fn)
}

// RecordRPCType counts one client request by the RPC type it was classified
// as and whether the client declared it or SAGE detected it. Satisfies
// router.ClientMetrics.
func (r *Recorder) RecordRPCType(serviceID domain.ServiceID, rpcType domain.RPCType, source string) {
	r.rpcTypeTotal.WithLabelValues(r.services.serviceValue(serviceID), string(rpcType), source).Inc()
}

// RecordRPCTypeMismatch counts a client request whose classification was
// contradicted: rpcType is what detection produced, actual what the
// contradicting party said, reason which party (header, plugin,
// unsupported). Satisfies router.ClientMetrics.
func (r *Recorder) RecordRPCTypeMismatch(serviceID domain.ServiceID, rpcType, actual domain.RPCType, reason string) {
	r.rpcTypeMismatchTotal.WithLabelValues(r.services.serviceValue(serviceID), string(rpcType), string(actual), reason).Inc()
}

// RecordRetry increments the retry counter for a service with a given reason.
func (r *Recorder) RecordRetry(serviceID domain.ServiceID, reason string) {
	r.retryTotal.WithLabelValues(r.services.serviceValue(serviceID), reason).Inc()
}

// RecordRetryResolution records how a retried request ended: "recovered" when
// a later attempt succeeded, "exhausted" when none did. Counted once per
// retried request, against the reason that caused the last retry.
//
// This is the counter that says whether retrying a given failure is worth
// anything. A status share cannot: on a low-volume service it moves by tens of
// points on window placement alone, and on a busy one the effect is diluted
// below the day-to-day band. A count of recoveries is neither a share nor a
// rate, so it needs no matched window and no baseline band to read.
func (r *Recorder) RecordRetryResolution(serviceID domain.ServiceID, reason, outcome string) {
	r.retryResolutionTotal.WithLabelValues(r.services.serviceValue(serviceID), reason, outcome).Inc()
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

// RecordAutoDrain counts one auto-drain engine decision.
func (r *Recorder) RecordAutoDrain(serviceID domain.ServiceID, rpcType, outcome string) {
	r.autoDrains.WithLabelValues(r.services.serviceValue(serviceID), rpcType, outcome).Inc()
}

// RecordOversizedResponse increments the counter of supplier responses
// abandoned for exceeding the response ceiling.
func (r *Recorder) RecordOversizedResponse(serviceID domain.ServiceID) {
	r.oversizedResponses.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordHealthCheckResult counts one applied probe result. source is the
// closed set healthcheck.ResultSource.
func (r *Recorder) RecordHealthCheckResult(serviceID domain.ServiceID, source string) {
	r.healthCheckResults.WithLabelValues(r.services.serviceValue(serviceID), source).Inc()
}

// RecordHealthCheckSkipped counts one health check not sent because client
// traffic had already graded the backend.
func (r *Recorder) RecordHealthCheckSkipped(serviceID domain.ServiceID) {
	r.healthCheckSkipped.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordHealthCheckCycle records one completed health-check cycle and whether
// it overran the tick it was scheduled on.
func (r *Recorder) RecordHealthCheckCycle(d time.Duration, tick time.Duration) {
	r.healthCheckCycle.Observe(d.Seconds())
	if tick > 0 && d > tick {
		r.healthCheckOverruns.Inc()
	}
}

// RecordHealthCheckCycleProbes publishes how many probes one completed cycle
// issued per service, replacing the previous cycle's figures.
//
// Services absent from the map are set to zero rather than left alone: a
// service that stopped being probed is the thing worth seeing, and a stale
// non-zero gauge would say the opposite. Services never probed at all are
// absent entirely, the same as every other per-service series here.
func (r *Recorder) RecordHealthCheckCycleProbes(perService map[domain.ServiceID]int) {
	seen := make(map[string]struct{}, len(perService))
	for serviceID, n := range perService {
		label := r.services.serviceValue(serviceID)
		seen[label] = struct{}{}
		r.healthCheckLastCycle.WithLabelValues(label).Set(float64(n))
	}
	for _, known := range r.services.values() {
		if _, ok := seen[known]; !ok {
			r.healthCheckLastCycle.WithLabelValues(known).Set(0)
		}
	}
}

// initHealthCheckLastCycle creates the per-cycle probe gauge at zero for every
// configured service, so the metric exists before the first cycle completes.
//
// Same reasoning as initHealthCheckSkipped and the same lesson learned twice:
// without it the series appears only after a cycle finishes, and on a
// deployment whose cycle is minutes long an operator scraping in between sees
// nothing and cannot tell "no cycle yet" from "probing is dead". That cost a
// minute on the canary on 2026-09-03 — less than the skipped counter cost,
// because that one had already taught everybody to suspect it.
func (r *Recorder) initHealthCheckLastCycle(knownServices []domain.ServiceID) {
	for _, serviceID := range knownServices {
		r.healthCheckLastCycle.WithLabelValues(r.services.serviceValue(serviceID)).Set(0)
	}
}

// initHealthCheckSkipped creates the skipped-probe series at zero for every
// configured service.
//
// Prometheus does not export a CounterVec child that has never been
// incremented, so without this the metric has no series at all until the first
// skip happens — and "no series" is not "zero". A query like
// sum(sage_health_check_skipped_total) returns empty rather than 0, an alert
// shaped on it never matches, and an operator cannot tell traffic-informed
// probing being off from the metric being missing. That distinction is the
// whole point of this counter, which exists to be compared against
// sage_health_check_results_total.
//
// Only this counter gets the treatment. The rest of the recorder's series are
// read as rates, where absence and zero mean the same thing; this one is read
// as a ratio against a baseline, where they do not.
func (r *Recorder) initHealthCheckSkipped(knownServices []domain.ServiceID) {
	for _, serviceID := range knownServices {
		r.healthCheckSkipped.WithLabelValues(r.services.serviceValue(serviceID)).Add(0)
	}
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

// RecordVerdict satisfies relay/middleware.MetricsRecorder: one heuristic
// verdict on one client relay attempt. reason and attribution come from
// heuristic.AnalysisResult, both closed sets, so neither is bounded here.
func (r *Recorder) RecordVerdict(serviceID domain.ServiceID, rpcType domain.RPCType, reason, attribution string) {
	r.heuristicVerdicts.WithLabelValues(
		r.services.serviceValue(serviceID),
		string(rpcType),
		reason,
		attribution,
	).Inc()
}

// RecordClientLatency satisfies router.ClientMetrics: one client request's
// wall time, by the status the client saw.
func (r *Recorder) RecordClientLatency(serviceID domain.ServiceID, status int, latency time.Duration) {
	r.clientLatency.WithLabelValues(r.services.serviceValue(serviceID), strconv.Itoa(status)).Observe(latency.Seconds())
}

// RecordStageTime satisfies router.ClientMetrics: one request's exclusive
// time in one stage.
func (r *Recorder) RecordStageTime(serviceID domain.ServiceID, stage string, d time.Duration) {
	if d <= 0 {
		return
	}
	r.stageSeconds.WithLabelValues(r.services.serviceValue(serviceID), stage).Add(d.Seconds())
}

// RecordExternalSourceFailure satisfies healthcheck.ExternalSourceFailureRecorder:
// one poll of a service's external block sources that produced no height.
func (r *Recorder) RecordExternalSourceFailure(serviceID domain.ServiceID) {
	r.externalSourceFails.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
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
