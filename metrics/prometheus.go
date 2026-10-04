// Package metrics provides Prometheus-backed metric recording for the SAGE
// relay pipeline. The Recorder type implements relay/middleware.MetricsRecorder.
package metrics

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
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

	relayTotal             *prometheus.CounterVec
	clientRequestsTotal    *prometheus.CounterVec
	rpcTypeTotal           *prometheus.CounterVec
	rpcTypeMismatchTotal   *prometheus.CounterVec
	relayLatency           *prometheus.HistogramVec
	retryTotal             *prometheus.CounterVec
	retryResolutionTotal   *prometheus.CounterVec
	hedgeTotal             *prometheus.CounterVec
	cacheHits              *prometheus.CounterVec
	cacheMisses            *prometheus.CounterVec
	singleflightCoalesced  *prometheus.CounterVec
	degradedTotal          *prometheus.CounterVec
	circuitBreaks          *prometheus.CounterVec
	circuitBreakerOutcome  *prometheus.CounterVec
	supplierBlacklists     *prometheus.CounterVec
	relayMinerErrors       *prometheus.CounterVec
	overServedExclusions   *prometheus.CounterVec
	keyMismatches          *prometheus.CounterVec
	sessionFetches         *prometheus.CounterVec
	oversizedResponses     *prometheus.CounterVec
	responseBytes          *prometheus.HistogramVec
	batchPayloads          *prometheus.HistogramVec
	batchCapped            *prometheus.CounterVec
	batchRejected          *prometheus.CounterVec
	batchSeconds           *prometheus.HistogramVec
	batchDisconnects       *prometheus.CounterVec
	quorumRequests         *prometheus.CounterVec
	selectionTiers         *prometheus.CounterVec
	staleAnswers           *prometheus.CounterVec
	invalidResults         *prometheus.CounterVec
	answerHeadLag          *prometheus.HistogramVec
	reputationWriteDrops   *prometheus.CounterVec
	quorumDissent          *prometheus.CounterVec
	batchSubRelays         prometheus.Gauge
	batchResponseBytes     prometheus.Gauge
	autoDrains             *prometheus.CounterVec
	methodBlockEvents      *prometheus.CounterVec
	reputationAttempts     *prometheus.CounterVec
	operatorFailures       *prometheus.CounterVec
	unclassified           *prometheus.CounterVec
	heuristicVerdicts      *prometheus.CounterVec
	operatorAttempts       *prometheus.CounterVec
	operatorMethodAttempts *prometheus.CounterVec
	operatorLatency        *prometheus.HistogramVec
	externalSourceFails    *prometheus.CounterVec
	clientLatency          *prometheus.HistogramVec
	stageSeconds           *prometheus.CounterVec
	healthCheckResults     *prometheus.CounterVec
	healthCheckSkipped     *prometheus.CounterVec
	healthCheckCycle       prometheus.Histogram
	healthCheckLastCycle   *prometheus.GaugeVec
	healthCheckOverruns    prometheus.Counter

	// codespaces bounds the relay miner error codespace label, which is a
	// string chosen by the supplier's relay miner.
	codespaces *labelPolicy
	// failureReasons bounds sage_operator_failures_total's reason label.
	failureReasons *labelPolicy
	// unclassifiedMessages bounds sage_unclassified_errors_total's message.
	unclassifiedMessages *labelPolicy

	// operators bounds the operator label: a registrable domain taken from
	// staked URLs, a set other people choose.
	operators *labelPolicy
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
				Help:      "Hedged relays by outcome: primary_before_delay (the primary answered inside the hedge delay, so no hedge was sent), and, once the hedge was sent, primary_won, hedge_won or both_failed; plus suppressed_large_batch, an item of a batch over retry_config.hedge_max_batch_size that ran unhedged; and suppressed_stale, a hedge not sent because every host left for it was far behind the chain head while the primary was not. The share of races where a hedge fired is (primary_won + hedge_won + both_failed) over the sum of the first four. Before 2026-09-27 primary_before_delay was counted as primary_won.",
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
		overServedExclusions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "over_served_exclusions_total",
				Help:      "Suppliers excluded for the rest of a session after refusing a relay for over-servicing (the application's relay allocation for that supplier and session is spent: the poktroll relay miner's relayer_proxy code 7, the HA relay miner's 429 \"session relay limit reached\"), by service. One count per supplier and session. The supplier is not penalized and serves again in the next session; other suppliers behind the same URL keep serving.",
			},
			[]string{"service_id"},
		),
		keyMismatches: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "reputation_key_mismatch_total",
				Help:      "Relays whose reputation key names a URL other than the one the relay dialed, by service and RPC type. A failure is scored against the key, so each count is a host's score moved by a relay it never received. Expected to stay at zero: a count is a bug in endpoint identity, the class of the cross-service endpoint lookup fixed in 5fd0d96.",
			},
			[]string{"service_id", "rpc_type"},
		),
		relayMinerErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "relay_miner_errors_total",
				Help:      "Total relay responses carrying a RelayMinerError, by service and miner error codespace.",
			},
			[]string{"service_id", "codespace"},
		),
		sessionFetches: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "session_fetches_total",
				Help:      "Session fetches from the full node, one per coalesced GetSession, by service, path (background: during the grace period, off the request path; sync: past the grace period, a request waits on it; websocket: past the session's end, a WebSocket dial, rebind or probe waits on it, since a WebSocket is signed for the session at the current height; cold: nothing cached yet) and outcome (ok; error; same_session: the answer ends no later than the session already cached, so the next session is still not held). Steady sync fetches mean the background refresh never landed inside grace; same_session through grace is what keeps WebSocket rebinds waiting until grace has elapsed.",
			},
			[]string{"service_id", "path", "outcome"},
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
		responseBytes: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "response_bytes",
				Help:      "Supplier relay response body size in bytes, by service, observed for every body read including one about to be rejected as oversized. The ceiling (router.max_response_body_bytes, knob relay.max_response_mb) bounds one response; nothing bounds how many large ones a pod reads at once, and sage_oversized_responses_total stays zero while a legal response costs gigabytes. This is the distribution that says where the ceiling belongs.",
				Buckets: []float64{
					1 << 10, 8 << 10, 64 << 10, 512 << 10,
					4 << 20, 16 << 20, 64 << 20, 256 << 20,
				},
			},
			[]string{"service_id"},
		),
		batchPayloads: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "batch_payloads",
				Help:      "Payloads per multi-payload client request, by service, observed before the concurrency_config.max_batch_payloads cap so a refused batch is counted too. One batch fans out into this many upstream relays, each with its own retry and hedge; this is the distribution that says where the cap belongs.",
				Buckets:   []float64{2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
			},
			[]string{"service_id"},
		),
		reputationWriteDrops: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "reputation_writes_dropped_total",
				Help:      "Reputation state writes that never reached storage, by reason: storage_error (storage refused the write, e.g. Redis unreachable), counted per key in the failed flush. A dropped write leaves storage behind this replica until the key's next flush and, if it keeps happening, lets a key's stored stamp age past the 1h idle TTL, so the next pod's warm-up skips it as stale. Only the leader writes; a follower holds nothing. (queue_full, a write the old fixed queue had no room for, ended with the per-key write-behind on 2026-10-02.)",
			},
			[]string{"reason"},
		),
		selectionTiers: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "qos_selection_tier_total",
				Help:      "Endpoint selections by the height tier the service's QoS plugin settled on: 1 within sync_allowance of the perceived head, 2 within twice it, 3 with the height filter abandoned and the candidates ranked least-stale. One count per selection, which is one per relay attempt (every retry, hedge arm, batch item and quorum arm selects), not one per client request. rate(tier=\"3\") over the sum across tiers is the share of attempts sent without a height guarantee. Counted by the plugins that filter on height: evm (tron included), cosmos, solana and the JSON-height chains; a service on the passthrough plugin has no series. Reads compare heights projected to the head's moment, so a reading one probe cycle old is not counted as behind.",
			},
			[]string{"service_id", "tier"},
		),
		staleAnswers: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "stale_answers_total",
				Help:      "Answers naming the chain head (eth_blockNumber, eth_getBlockByNumber(\"latest\"), Solana getEpochInfo, getBlockHeight, getLatestBlockhash; CometBFT status and block with no height and the REST latest-block route, graded by the block's time; NEAR block for a finality and Sui's latest checkpoint; and, with state_canary on, the health checks' eth_call_canary and rest_head_canary, graded by block time against the clock, and near_canary, graded by the height its state was read at) that lagged the head this pod expected at receive time by more than max(2 blocks, 10 seconds of blocks), by service, party (the owner when its domain is dedicated, else the operator) and method. The expected head is the perceived height advanced at the chain's block rate since it last moved. A response cache in front of a node serves such answers fast; the height filter cannot see it while the lag stays inside the sync allowance. Counted whatever the flags say; with stale_response on, the same answers are also graded stale_response and retried.",
			},
			[]string{"service_id", "party", "method"},
		),
		invalidResults: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "invalid_results_total",
				Help:      "Successful answers whose result no node produces for the method (an EVM DATA result such as eth_call's that is not \"0x\" followed by an even number of hex digits, like \"0x0\"), by service, party (the owner when its domain is dedicated, else the operator) and method. Counted whatever the flags say; with invalid_result on, the same answers are graded invalid_result and retried.",
			},
			[]string{"service_id", "party", "method"},
		),
		answerHeadLag: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "answer_head_lag_blocks",
				Help:      "How many blocks an answer naming the chain head lagged the head this pod expected at receive time, by service and party; 0 for an answer at or ahead of it. Same answers and same expected head as sage_stale_answers_total, every one of them rather than only the stale.",
				Buckets:   []float64{0, 1, 2, 5, 10, 25, 50, 100, 250, 1000},
			},
			[]string{"service_id", "party"},
		),
		quorumRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "quorum_requests_total",
				Help:      "Client requests that asked for a quorum (Target-Quorum-Count or Target-Quorum-Mode), by service and outcome: majority (consensus answered with the agreed answer), no_majority (consensus fell back to the collect envelope), collect (collect mode as asked), collect_not_immutable (consensus asked for a request the plugin cannot vouch is immutable, answered in collect mode), timeout (the deadline ended the wait), skipped_disabled / skipped_batch / skipped_rpc_type (answered as an ordinary request). Each non-skipped request cost up to X-Quorum-Count relays, which sage_relay_total counts one by one.",
			},
			[]string{"service_id", "outcome"},
		),
		quorumDissent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "quorum_dissent_total",
				Help:      "Quorum answers that disagreed with the majority a consensus request was answered with, by service, among those in by the time the majority formed. Counted and not scored: each answer's own attempt was already graded on its merits. Rising on one service is a supplier serving different data than its peers for the same immutable request.",
			},
			[]string{"service_id"},
		),
		batchRejected: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "batch_rejected_total",
				Help:      "JSON-RPC batches refused whole for carrying more payloads than the service's max_batch_payloads (the batch.max_payloads tuning knob, per service, else concurrency.max_batch_payloads), by service. The client gets HTTP 413 and a -32600 naming the limit.",
			},
			[]string{"service_id"},
		),
		batchCapped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "batch_concurrency_capped_total",
				Help:      "Batch requests with more payloads than concurrency_config.max_batch_concurrency, by service and payload-count bucket (size: le32, le128, le512, gt512, the same buckets as sage_batch_seconds). Such a batch relays that many payloads at a time and waits on its own ceiling for the rest, so it is answered later than it would be uncapped; against sage_batch_seconds_count of the same size this is the share of batches of that size the ceiling slows.",
			},
			[]string{"service_id", "size"},
		),
		batchSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "batch_seconds",
				Help:      "Wall time of a batch request, by service and payload-count bucket (size: le32, le128, le512, gt512 payloads, counted before any cap), from the batch middleware's entry to the last byte of its answer written. Batches are streamed to the client, so this includes writing and any time a slow client held the stream back; a batch whose client disconnected is observed up to the disconnect (sage_batch_client_disconnects_total). A batch refused over max_batch_payloads is not observed (sage_batch_payloads counts it). This is the evidence for tuning concurrency_config.max_batch_concurrency: a lower cap bounds memory and shows up here as latency on the larger sizes.",
				Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30},
			},
			[]string{"service_id", "size"},
		),
		batchDisconnects: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "batch_client_disconnects_total",
				Help:      "Streamed batch requests whose client went away before the last answer was written, by service. The batch stops starting payloads, cancels the ones in flight and releases what it held; the answers already written reached a client that did not read the rest.",
			},
			[]string{"service_id"},
		),
		batchSubRelays: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "batch_subrelays_in_flight",
			Help:      "Batch sub-relays running now, across every service. Each holds a slot of the process-wide concurrency_config.max_concurrent_relays budget while that budget is on, so this against the configured value is the budget's occupancy. Single-payload requests do not count.",
		}),
		batchResponseBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "batch_response_bytes_in_flight",
			Help:      "Sub-relay answer bytes finished and not yet written to the client, across every service. A streamed batch releases each answer as it is written, so one batch holds at most (concurrency_config.max_batch_concurrency + max_batch_window) answers here. A batch whose writer cannot stream (none in production) keeps every answer until it merges, and that merged copy is not counted.",
		}),
		codespaces:           cappedLabel(maxCodespaceLabels),
		failureReasons:       cappedLabel(maxFailureReasonLabels),
		unclassifiedMessages: cappedLabel(maxFailureReasonLabels),
		operators:            cappedLabel(maxOperatorLabels),
		// Per operator, the registrable domain, never per host: an operator
		// is a handful of values per service and stays put, where hosts
		// rotate with every session and are the series growth the
		// method-block counter below avoids. These two are what a supplier
		// quality view reads — who carries a service, how often their
		// answers fail and whose fault that is, and how fast they are —
		// which no other sage_* series can say: relay_total and
		// relay_latency_seconds carry no endpoint.
		operatorAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "operator_attempts_total",
				Help:      "Client relay attempts, by service, operator (the registrable domain of the endpoint's URL), RPC type, the side the heuristic attributed the outcome to (none: a good answer; blockchain or client: the answer was the chain's or the request's fault, not the supplier's; supplier or unknown: the supplier failed) and attempt: first, retry, hedge or probation. Only attempt=\"first\" is a fair sample of an operator: retries arrive with less budget after another host failed, and an operator reputation has demoted is sent mostly those. It is also sent few first attempts, and its probation ones run on a quarter of the budget, so require a minimum count before reading an operator from this, and fall back to health checks where it has none. Operators past the first 128 seen collapse to __other__.",
			},
			[]string{"service_id", "operator", "rpc_type", "attribution", "attempt"},
		),
		operatorMethodAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "operator_method_attempts_total",
				Help:      "First client relay attempts only, by service, operator, method class (light: heads, chain ids, health; heavy: log scans, calls, traces, program-account scans; standard: the rest) and outcome (good: not the supplier's fault; bad: supplier or unknown). An operator that answers the light calls and stalls the heavy ones shows here and nowhere else.",
			},
			[]string{"service_id", "operator", "method_class", "outcome"},
		),
		operatorLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "operator_attempt_seconds",
				Help:      "Client relay attempt latency in seconds, by service, operator (the registrable domain of the endpoint's URL) and RPC type: selection through response, one observation per attempt, whatever its outcome. Same buckets as relay_latency_seconds.",
				Buckets:   relayLatencyBuckets,
			},
			[]string{"service_id", "operator", "rpc_type"},
		),
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
		unclassified: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "unclassified_errors_total",
				Help:      "Answers whose JSON-RPC error the heuristic could not place (server_error: a -32000..-32099 wording that is neither a known chain answer, a client error nor a supplier failure; unknown_error_code: a code outside the spec), by service, reason and message: the error text with numbers and hex values masked, cut at 60 bytes. These are retried and not scored, so each wording here is either a chain or client answer retried for nothing, or a supplier failure scored as nothing: the list to catalogue in heuristic/protocol.go. Messages past the first 64 collapse to __other__.",
			},
			[]string{"service_id", "reason", "message"},
		),
		operatorFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "operator_failures_total",
				Help:      "Failures reputation recorded against an operator (the registrable domain of the endpoint's URL), by service, RPC type and reason: the heuristic verdict for client traffic and health checks (transport_timeout, http_5xx, stale_response, …) and the WebSocket path's own (ws_probe_dial_failed, ws_endpoint_lost, …), cut at the first ':' so an error text does not become a label. The outcome an operator can act on, without its score. Operators past the first 128 seen collapse to __other__, reasons past the first 64 to __other__.",
			},
			[]string{"service_id", "operator", "rpc_type", "reason"},
		),
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
		r.overServedExclusions,
		r.keyMismatches,
		r.sessionFetches,
		r.oversizedResponses,
		r.responseBytes,
		r.batchPayloads,
		r.batchCapped,
		r.batchRejected,
		r.batchSeconds,
		r.batchDisconnects,
		r.quorumRequests,
		r.selectionTiers,
		r.staleAnswers,
		r.invalidResults,
		r.answerHeadLag,
		r.reputationWriteDrops,
		r.quorumDissent,
		r.batchSubRelays,
		r.batchResponseBytes,
		r.autoDrains,
		r.methodBlockEvents,
		r.reputationAttempts,
		r.operatorFailures,
		r.unclassified,
		r.heuristicVerdicts,
		r.operatorAttempts,
		r.operatorMethodAttempts,
		r.operatorLatency,
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

// RecordResponseSize observes one supplier response body's size in bytes.
// Satisfies the protocol's supplier metrics.
func (r *Recorder) RecordResponseSize(serviceID domain.ServiceID, bytes int) {
	r.responseBytes.WithLabelValues(r.services.serviceValue(serviceID)).Observe(float64(bytes))
}

// RecordBatchPayloads observes one batch's payload count. Satisfies
// middleware.BatchRecorder.
func (r *Recorder) RecordBatchPayloads(serviceID domain.ServiceID, n int) {
	r.batchPayloads.WithLabelValues(r.services.serviceValue(serviceID)).Observe(float64(n))
}

// RecordReputationWriteDropped counts one reputation write that never reached
// storage. Wire installs it as the reputation service's write drop hook.
func (r *Recorder) RecordReputationWriteDropped(reason string) {
	r.reputationWriteDrops.WithLabelValues(reason).Inc()
}

// RecordAnswerHead records one answer that named the chain head: its lag
// behind the head expected at receive time, and whether that was stale.
// method is one of the few head methods a plugin reads, so it is bounded.
func (r *Recorder) RecordAnswerHead(serviceID domain.ServiceID, party, method string, lag uint64, stale bool) {
	service := r.services.serviceValue(serviceID)
	// party is an operator domain or an owner address: supplier-chosen, so
	// capped like every other operator label.
	party = r.operators.value(party)
	r.answerHeadLag.WithLabelValues(service, party).Observe(float64(lag))
	if stale {
		r.staleAnswers.WithLabelValues(service, party, method).Inc()
	}
}

// RecordInvalidResult counts one answer whose result no node produces for
// its method. method is a DATA method a plugin names, so it is bounded.
func (r *Recorder) RecordInvalidResult(serviceID domain.ServiceID, party, method string) {
	r.invalidResults.WithLabelValues(r.services.serviceValue(serviceID), r.operators.value(party), method).Inc()
}

// RecordSelectionTier counts one endpoint selection by its height tier. Tiers
// outside 1-3 (an empty candidate list) are not counted.
func (r *Recorder) RecordSelectionTier(serviceID domain.ServiceID, tier int) {
	var label string
	switch tier {
	case 1:
		label = "1"
	case 2:
		label = "2"
	case 3:
		label = "3"
	default:
		return
	}
	r.selectionTiers.WithLabelValues(r.services.serviceValue(serviceID), label).Inc()
}

// RecordQuorum counts one quorum request by outcome. Satisfies
// middleware.QuorumRecorder.
func (r *Recorder) RecordQuorum(serviceID domain.ServiceID, outcome string) {
	r.quorumRequests.WithLabelValues(r.services.serviceValue(serviceID), outcome).Inc()
}

// RecordQuorumDissent counts answers that disagreed with a quorum's majority.
func (r *Recorder) RecordQuorumDissent(serviceID domain.ServiceID, n int) {
	r.quorumDissent.WithLabelValues(r.services.serviceValue(serviceID)).Add(float64(n))
}

// RecordBatchConcurrencyCapped counts a batch of n payloads that runs under
// max_batch_concurrency. Satisfies middleware.BatchRecorder.
func (r *Recorder) RecordBatchConcurrencyCapped(serviceID domain.ServiceID, n int) {
	r.batchCapped.WithLabelValues(r.services.serviceValue(serviceID), batchSize(n)).Inc()
}

// RecordBatchRejected counts one batch refused over max_batch_payloads.
func (r *Recorder) RecordBatchRejected(serviceID domain.ServiceID) {
	r.batchRejected.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordBatchSeconds observes one batch's wall time.
func (r *Recorder) RecordBatchSeconds(serviceID domain.ServiceID, n int, d time.Duration) {
	r.batchSeconds.WithLabelValues(r.services.serviceValue(serviceID), batchSize(n)).Observe(d.Seconds())
}

// RecordBatchClientDisconnect counts a streamed batch cut short by its client.
func (r *Recorder) RecordBatchClientDisconnect(serviceID domain.ServiceID) {
	r.batchDisconnects.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// batchSize is the payload-count bucket batch metrics are labelled with.
func batchSize(n int) string {
	switch {
	case n <= 32:
		return "le32"
	case n <= 128:
		return "le128"
	case n <= 512:
		return "le512"
	}
	return "gt512"
}

// AddBatchSubRelays moves the batch sub-relays in flight gauge.
func (r *Recorder) AddBatchSubRelays(delta int) {
	r.batchSubRelays.Add(float64(delta))
}

// AddBatchResponseBytes moves the batch response bytes in flight gauge.
func (r *Recorder) AddBatchResponseBytes(delta int64) {
	r.batchResponseBytes.Add(float64(delta))
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

// RecordOverServedExclusion counts one supplier excluded for the rest of a
// session after an over-servicing refusal.
func (r *Recorder) RecordOverServedExclusion(serviceID domain.ServiceID) {
	r.overServedExclusions.WithLabelValues(r.services.serviceValue(serviceID)).Inc()
}

// RecordKeyMismatch counts one relay whose reputation key names a URL other
// than the one it dialed.
func (r *Recorder) RecordKeyMismatch(serviceID domain.ServiceID, rpcType domain.RPCType) {
	r.keyMismatches.WithLabelValues(r.services.serviceValue(serviceID), string(rpcType)).Inc()
}

// RecordRelayMinerError increments the counter of relay responses that carried
// an error report from the supplier's relay miner.
//
// codespace is written by that miner, so it is bounded here — see boundedLabel.
func (r *Recorder) RecordRelayMinerError(serviceID domain.ServiceID, codespace string) {
	r.relayMinerErrors.WithLabelValues(r.services.serviceValue(serviceID), r.codespaces.value(codespace)).Inc()
}

// RecordSessionFetch counts one session fetch from the full node. path and
// outcome come from closed sets in protocol/shannon.
func (r *Recorder) RecordSessionFetch(serviceID domain.ServiceID, path, outcome string) {
	r.sessionFetches.WithLabelValues(r.services.serviceValue(serviceID), path, outcome).Inc()
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

// RecordUnclassified counts one answer whose error the heuristic could not
// place, by its masked message (unclassifiedMessage).
func (r *Recorder) RecordUnclassified(serviceID domain.ServiceID, reason, detail string) {
	r.unclassified.WithLabelValues(r.services.serviceValue(serviceID), reason,
		r.unclassifiedMessages.value(unclassifiedMessage(detail))).Inc()
}

// unclassifiedMessage is detail with its "(code N): " lead kept, hex values
// and numbers masked so one wording is one label, cut at 60 bytes.
func unclassifiedMessage(detail string) string {
	detail = maskedValue.ReplaceAllStringFunc(detail, func(v string) string {
		if strings.HasPrefix(v, "0x") {
			return "0x#"
		}
		return "#"
	})
	if len(detail) > 60 {
		detail = detail[:60]
	}
	return detail
}

// maskedValue is a hex value or a number, matched in one pass so a hex
// value's leading 0 is not read as a number.
var maskedValue = regexp.MustCompile(`0x[0-9a-fA-F]+|-?[0-9]+`)

// RecordOperatorFailure counts one failure reputation recorded against an
// operator. reason is cut at its first ':' (a WebSocket reason carries the
// error text after it) and bounded.
func (r *Recorder) RecordOperatorFailure(serviceID domain.ServiceID, operator, rpcType, reason string) {
	reason, _, _ = strings.Cut(reason, ":")
	r.operatorFailures.WithLabelValues(r.services.serviceValue(serviceID), r.operators.value(operator),
		rpcType, r.failureReasons.value(reason)).Inc()
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

// RecordOperatorAttempt counts one client relay attempt against the operator
// that served it, with the heuristic's attribution, the attempt kind and the
// attempt's latency. A first attempt is also counted by method class and
// outcome: the fair sample of how an operator answers each kind of call.
func (r *Recorder) RecordOperatorAttempt(serviceID domain.ServiceID, rpcType domain.RPCType, endpoint domain.EndpointAddr, attribution, kind, methodClass string, latency time.Duration) {
	svc := r.services.serviceValue(serviceID)
	op := r.operators.value(endpoint.Operator())
	r.operatorAttempts.WithLabelValues(svc, op, string(rpcType), attribution, kind).Inc()
	r.operatorLatency.WithLabelValues(svc, op, string(rpcType)).Observe(latency.Seconds())
	if kind != "first" {
		return
	}
	outcome := "good"
	if attribution == "supplier" || attribution == "unknown" {
		outcome = "bad"
	}
	r.operatorMethodAttempts.WithLabelValues(svc, op, methodClass, outcome).Inc()
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

// NewWarmGauges exposes the health-check warm gate, the half of readiness that
// is not the session layer:
//
//	sage_health_check_warm_services_covered <count>
//	sage_health_check_warm_services_needed <count>
//	sage_health_check_warm_services_unprobeable <count>
//
// Covered below needed is a pod answering /ready with 503, and the pair says
// how far short it is. Read at scrape time, so covered climbs as coverage
// arrives; all three are flat once the pod is warm, because the gate latches.
//
// Needed is 75% of the services that can produce coverage at all, which is not
// 75% of the config: one that declares checks but has no endpoint staked for
// their RPC types can never be covered, and counting it made the threshold
// unreachable. Unprobeable is how many were excluded, so the two together say
// what the denominator is and why it moved.
//
// It exists because a held pod was undiagnosable from outside. /ready returns a
// bare status code, the gate's own explanation is a WARN that production log
// levels drop, and the path that usually causes it logs at DEBUG — so a mainnet
// pod that sat at 503 for eleven minutes on 2026-09-17 was read from a
// goroutine dump instead. Needed is 75% of the probeable services; covered is
// what the startup warm-up read credited (sage_reputation_hydrated_services)
// plus what probes have landed since. Covered flat and short is the shape to
// alert on: coverage that has stopped moving, which the warm-up deadline now
// releases rather than waiting out.
//
// Name and Help are literals per gauge because internal/docgen reads them from
// the AST; a metric named by a variable is one docs/metrics.md omits.
func NewWarmGauges(progress func() (covered, needed, unprobeable int)) []prometheus.Collector {
	return []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "health_check_warm_services_covered",
			Help:      "Services the health-check warm gate has applied a result for, from the startup warm-up read or from probes since. Below sage_health_check_warm_services_needed the pod answers /ready with 503 and takes no traffic.",
		}, func() float64 { covered, _, _ := progress(); return float64(covered) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "health_check_warm_services_needed",
			Help:      "Services the warm gate needs covered before readiness: 75% of the services that declare checks AND have an endpoint to send one to, recomputed every cycle. Services with no endpoint are excluded and counted in sage_health_check_warm_services_unprobeable, so this shrinks for a reason rather than mysteriously; it is the threshold actually applied, and the deadline release is judged against it.",
		}, func() float64 { _, needed, _ := progress(); return float64(needed) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "health_check_warm_services_unprobeable",
			Help:      "Services left OUT of sage_health_check_warm_services_needed because they declare checks but have no endpoint staked for any of their RPC types, so no probe can be sent and no coverage can ever arrive. This is why the threshold is lower than 75% of the configured services, and it is recomputed every cycle: a service whose suppliers appear later leaves this count and rejoins the denominator. High and steady means that much of the config has no suppliers on the network.",
		}, func() float64 { _, _, unprobeable := progress(); return float64(unprobeable) }),
	}
}

// NewSessionLayerGauges exposes the other half of readiness, the one the warm
// gate is ANDed with:
//
//	sage_session_layer_ready <0|1>
//	sage_full_node_block_height <height>
//
// Ready is the verdict of the last readiness read — the full node answered a
// block height above zero — and it reads 0 before the first read, which is also
// how a pod nothing has probed reads. It costs no gRPC call: the readiness path
// records its verdict and this reports it.
//
// Together the two say which half of a 503 is failing, and a frozen height
// beside ready 0 separates a full node that is unreachable from one that is
// merely behind. Against the warm gauges, the four cover every way /ready can
// answer 503: a mainnet pod held for eleven minutes on 2026-09-17 with nothing
// to distinguish them, because both halves explain themselves in a WARN and the
// fleet runs at log level error.
//
// Name and Help are literals per gauge because internal/docgen reads them from
// the AST; a metric named by a variable is one docs/metrics.md omits.
func NewSessionLayerGauges(ready func() bool, height func() int64) []prometheus.Collector {
	return []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "session_layer_ready",
			Help:      "Whether the last readiness read found the full node answering a block height above zero: 1 ready, 0 not. Readiness is this ANDed with the health-check warm gate (sage_health_check_warm_services_covered against _needed), so 0 here is a pod answering /ready with 503 for the protocol half rather than the coverage half. Reads 0 until the first readiness probe. A transition to 0 is also logged at ERROR; the recovery is only here, which is why it is a gauge.",
		}, func() float64 {
			if ready() {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "full_node_block_height",
			Help:      "Newest chain head the background block poller has seen, or 0 before its first successful poll. A failed poll leaves the previous height in place, so this freezing rather than dropping is how an unreachable full node reads. Beside sage_session_layer_ready it separates a full node that cannot be reached from one that answers but is behind; it is also what session expiry is judged against, so a frozen height means sessions are being kept past their end.",
		}, func() float64 { return float64(height()) }),
	}
}

// NewLeaderGauge exposes whether this pod holds the health-check leadership as
// sage_health_check_is_leader.
//
// Probing is the leader's job, and exactly one pod in a fleet should read 1. A
// fleet where every pod reads 0 is a fleet that sends no probes at all, which
// is what mainnet did for at least four hours on 2026-09-18: another deployment
// sharing the same Redis database held the lock, so mainnet's own pods never
// probed, and the only trace was sage_health_check_last_cycle_probes sitting at
// zero everywhere — indistinguishable from a config that asks for nothing.
// Summed across a deployment this answers "is anyone leading" in one query.
//
// Two pods reading 1 at once is a split lease, not a tie: the lock is held with
// a TTL and renewed, so it means renewal is losing and results are being
// published twice.
func NewLeaderGauge(isLeader func() bool) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sage",
		Name:      "health_check_is_leader",
		Help:      "Whether this pod currently holds the health-check leader lock: 1 leader, 0 follower. Probing is the leader's job, so exactly one pod per deployment should read 1 — sum it across the deployment and 0 means nobody is probing, which looks identical to an idle config in every other metric. With no Redis every pod is its own leader and reads 1. Two pods at 1 means the lease is splitting and probe results are being published twice.",
	}, func() float64 {
		if isLeader() {
			return 1
		}
		return 0
	})
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
