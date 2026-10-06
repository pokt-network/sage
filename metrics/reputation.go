package metrics

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// ScoreLister reports the current reputation scores for a service, keyed by
// reputation key. reputation.Service satisfies it.
type ScoreLister interface {
	GetScores(ctx context.Context, serviceID domain.ServiceID) (map[string]float64, error)
}

// RecentScoreLister reports the scores of keys that received a signal since a
// time. reputation.Service's implementation satisfies it; the per-operator key
// count uses it so that count means "recently active", not "ever seen".
type RecentScoreLister interface {
	GetScoresSince(ctx context.Context, serviceID domain.ServiceID, since time.Time) map[string]float64
}

// recentKeyWindow is what "recently active" means for the per-operator key
// count: about three sessions.
const recentKeyWindow = time.Hour

// maxScoreSeriesPerService caps how many reputation keys one service may report
// in a single scrape, after the full-score filter below.
//
// At the default per-URL granularity the live key set is backend URLs × RPC
// types — hundreds, not thousands — and this rarely binds. It binds under
// per-endpoint or per-supplier, where the key carries the supplier address: a
// supplier is a staked registration that rotates every session, so the
// distinct set grows with the network rather than with SAGE's traffic. PATH
// measured the equivalent metric at 4,510 keys live in a 10-minute window
// against 74,639 distinct over 7.7 hours on one pod. The mainnet canary
// (2026-09-01, per-supplier, ~50 services) hit the previous cap of 2,000 on
// most services: 104k series from one pod, 2.3% of the Prometheus head.
const maxScoreSeriesPerService = 500

// ScoreCollector exposes reputation scores as a Prometheus gauge:
//
//	sage_endpoint_reputation_score{service_id, endpoint} <score>
//
// The label is the reputation *key*, not an endpoint address. At the default
// per-URL granularity one key covers every supplier fronting that URL, so there
// is no single endpoint to attribute the score to — see reputation/key.go.
//
// A Collector rather than a gauge the reputation service pushes to, for the
// same reason as NewBreakerCollector but a sharper one. A pushed GaugeVec keyed on
// an endpoint identity never evicts: the client library holds every child it
// has ever seen for the process's lifetime, and a supplier address that stopped
// existing three sessions ago keeps costing heap and scrape bytes until the pod
// restarts. Deriving at scrape time means a key that is no longer scored simply
// stops being reported, and Prometheus marks it stale.
//
// Only informative keys are exported: a key sitting at the full score says
// nothing a runbook wants — it is what a miss would answer — and at rotating
// granularities it is most of the set. The same rule bounds the score cache
// itself (reputation.pruneUninformative). The full count, including those
// keys, is on sage_reputation_keys.
//
// When a service has more informative keys than maxScoreSeriesPerService, the
// LOWEST scores are kept. Truncation is reported on
// sage_endpoint_reputation_scores_dropped, so a trimmed scrape is visible
// rather than silently partial — and what survives is what a runbook is
// looking for.
type ScoreCollector struct {
	lister   ScoreLister
	services []domain.ServiceID
	// fullScore is the ceiling; a key at it is not exported.
	fullScore float64

	scoreDesc   *prometheus.Desc
	droppedDesc *prometheus.Desc
	keysDesc    *prometheus.Desc
	opKeysDesc  *prometheus.Desc

	// operators bounds the operator label of the per-operator families.
	operators *labelPolicy
}

// NewScoreCollector returns a collector for the given services. fullScore is
// the reputation ceiling (100, reputation's maximum score); keys at it are not
// exported. It does not register itself; the caller decides which registry it
// belongs to.
func NewScoreCollector(lister ScoreLister, services []domain.ServiceID, fullScore float64) *ScoreCollector {
	return &ScoreCollector{
		lister:    lister,
		services:  services,
		fullScore: fullScore,
		scoreDesc: prometheus.NewDesc(
			"sage_endpoint_reputation_score",
			"Current reputation score, by service and reputation key (see reputation/key.go for what a key covers). Only keys below the full score are exported — a key at the ceiling is what an unknown key would score — and at most 500 per service, lowest first; see sage_endpoint_reputation_scores_dropped and sage_reputation_keys.",
			[]string{"service_id", "endpoint"},
			nil,
		),
		droppedDesc: prometheus.NewDesc(
			"sage_endpoint_reputation_scores_dropped",
			"Reputation keys below the full score omitted from this scrape because the service exceeded the per-scrape cap. Non-zero means sage_endpoint_reputation_score is showing only the lowest-scoring keys.",
			[]string{"service_id"},
			nil,
		),
		// Per operator, over every recently active key including those at
		// the full score, which the per-key family above leaves out. At the
		// default granularity a key is a URL, so this is how many distinct
		// URLs an operator served from: a layout indicator (one host fronting
		// many registrations, or one host each), not a quality score.
		opKeysDesc: prometheus.NewDesc(
			"sage_operator_reputation_keys",
			"Reputation keys that received a signal in the last hour, by service, operator (the registrable domain of the key's URL) and RPC type. At the default per-URL granularity this is how many distinct URLs the operator served from: read it beside sage_session_endpoints as a layout indicator (many registrations behind one host, or one host each), not as a quality score.",
			[]string{"service_id", "operator", "rpc_type"},
			nil,
		),
		operators: cappedLabel(maxOperatorLabels),
		keysDesc: prometheus.NewDesc(
			"sage_reputation_keys",
			"Reputation keys this replica holds a score for, by service — the full count, before the full-score filter and the per-scrape cap on sage_endpoint_reputation_score. At per-URL granularity this tracks the real backend population; at per-supplier or per-endpoint it grows with every session's fresh registrations until the score map's own bound prunes uninformative keys.",
			[]string{"service_id"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *ScoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.scoreDesc
	ch <- c.droppedDesc
	ch <- c.keysDesc
	ch <- c.opKeysDesc
}

// Collect implements prometheus.Collector. Called on scrape, not on the hot
// path.
//
// A service whose scores cannot be read is skipped rather than reported as
// zero: absence is legible as "no data", a zero score is not — it is the worst
// score there is.
func (c *ScoreCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil {
		return
	}

	ctx := context.Background()
	for _, serviceID := range c.services {
		scores, err := c.lister.GetScores(ctx, serviceID)
		if err != nil {
			continue
		}

		total := len(scores)
		keys := make([]string, 0, len(scores))
		for k, score := range scores {
			if score < c.fullScore {
				keys = append(keys, k)
			}
		}

		recent := scores
		if rl, ok := c.lister.(RecentScoreLister); ok {
			recent = rl.GetScoresSince(ctx, serviceID, time.Now().Add(-recentKeyWindow))
		}
		type opKey struct{ op, rpc string }
		byOp := map[opKey]int{}
		for k := range recent {
			if op, rpc, ok := operatorOfKey(k); ok {
				byOp[opKey{c.operators.value(op), rpc}]++
			}
		}

		dropped := 0
		if len(keys) > maxScoreSeriesPerService {
			// Lowest score first, key as the tiebreak so a scrape is stable
			// when many endpoints sit at the initial score.
			sort.Slice(keys, func(i, j int) bool {
				if scores[keys[i]] != scores[keys[j]] {
					return scores[keys[i]] < scores[keys[j]]
				}
				return keys[i] < keys[j]
			})
			dropped = len(keys) - maxScoreSeriesPerService
			keys = keys[:maxScoreSeriesPerService]
		}

		sid := sanitizeLabel(string(serviceID))
		for _, k := range keys {
			ch <- prometheus.MustNewConstMetric(
				c.scoreDesc,
				prometheus.GaugeValue,
				scores[k],
				sid,
				sanitizeLabel(k),
			)
		}

		ch <- prometheus.MustNewConstMetric(
			c.droppedDesc,
			prometheus.GaugeValue,
			float64(dropped),
			sid,
		)
		ch <- prometheus.MustNewConstMetric(
			c.keysDesc,
			prometheus.GaugeValue,
			float64(total),
			sid,
		)
		for k, n := range byOp {
			ch <- prometheus.MustNewConstMetric(c.opKeysDesc, prometheus.GaugeValue, float64(n), sid, k.op, sanitizeLabel(k.rpc))
		}
	}
}

// operatorOfKey splits a reputation key "<identity>|<rpc_type>" into the
// operator of a URL identity and the RPC type. ok is false for an identity
// that is not a URL (per-supplier or per-endpoint granularity), whose
// "operator" would be a rotating supplier address.
func operatorOfKey(key string) (operator, rpcType string, ok bool) {
	i := strings.LastIndexByte(key, '|')
	if i < 0 || !strings.Contains(key[:i], "://") {
		return "", "", false
	}
	op := domain.OperatorOfURL(key[:i])
	return op, key[i+1:], op != ""
}

// NewTimelineKeysGauge exposes the number of distinct keys the reputation
// timeline holds:
//
//	sage_reputation_timeline_keys <count>
//
// The timeline is bounded (reputation.Timeline evicts idle keys and caps the
// total), and this is the gauge that shows the bound working. It was the
// growth that took the mainnet canary to its memory limit on 2026-09-01: keys
// at per-supplier granularity rotate every session and the timeline kept every
// one it had ever seen. A value flat against the cap is a rotating key set,
// not a leak; a value that keeps climbing past it is a bug.
func NewTimelineKeysGauge(keys func() int) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "reputation_timeline_keys",
			Help:      "Distinct keys held by the reputation timeline (the admin API's per-endpoint event log). Bounded by an idle TTL and a hard cap; flat against the cap means the key set rotates every session, climbing past it means a leak.",
		},
		func() float64 { return float64(keys()) },
	)
}

// NewOperatorStatsGauge exposes how many per-operator counters the service
// holds:
//
//	sage_reputation_operator_stats <count>
//
// An operator identity does not rotate with the session draw, so unlike
// sage_reputation_keys this should be small and steady — roughly the operators
// serving the configured services. A count that climbs with time is the signal
// that something is minting operator identities, which would mean the eTLD+1
// extraction is failing and every host is becoming its own operator.
func NewOperatorStatsGauge(count func() int) prometheus.Collector {
	return prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "reputation_operator_stats",
			Help:      "Per-operator failure counters held by the reputation service, one per (service, operator, RPC type). Operator identity does not rotate with the session draw, so this should be small and steady; a climbing count means eTLD+1 extraction is failing and each host is becoming its own operator.",
		},
		func() float64 { return float64(count()) },
	)
}

// NewReputationWriteQueueDepth exposes how many reputation keys are waiting for
// the next write-behind flush, read at scrape time from depth.
func NewReputationWriteQueueDepth(depth func() int) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "sage",
		Name:      "reputation_write_queue_depth",
		Help:      "Reputation keys waiting for this replica's next write-behind flush (once a second), at scrape time: one per key changed since the last flush, however many signals it took. Always 0 on a follower, which does not write.",
	}, func() float64 { return float64(depth()) })
}

// NewHydratedGauges exposes what the startup warm-up read loaded:
//
//	sage_reputation_hydrated_keys <count>
//	sage_reputation_hydrated_services <count>
//	sage_reputation_hydrated_skipped <count>
//
// All three are set once, at startup, and never change — which is the point.
// The only other evidence that hydration ran is a log line, and on the mainnet
// canary (2026-09-02) that line was invisible: the log level suppresses INFO,
// so the first roll carrying hydration had to be confirmed by inferring it
// from sage_reputation_keys being implausibly high for a fresh pod. These say
// it directly, in the place operators already scrape.
//
// Zero keys on a pod that should have inherited state is the signal worth
// alerting on: it means the store was empty, unreachable, or entirely stale,
// and the pod is warming from probes the slow way. Skipped beside keys is the
// other half of that reading: it is the history each roll throws away, and it
// is large when endpoints rotate out of the session faster than the idle TTL
// keeps them.
//
// The Name and Help below are spelled out per gauge rather than passed to a
// shared helper: internal/docgen reads these literals out of the AST to
// generate docs/metrics.md, and a metric named by a variable is a metric the
// reference silently omits.
func NewHydratedGauges(keys, services, skipped int) []prometheus.Collector {
	keysGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "sage",
		Name:      "reputation_hydrated_keys",
		Help:      "Reputation states loaded from storage by the startup warm-up read. Set once at startup and constant thereafter; zero means the pod started cold and is re-learning the pool from probes.",
	})
	keysGauge.Set(float64(keys))

	servicesGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "sage",
		Name:      "reputation_hydrated_services",
		Help:      "Distinct services covered by the startup warm-up read. These are credited to the health-check warm gate, so this is how much of readiness was satisfied by inherited state rather than by this pod's own probing.",
	})
	servicesGauge.Set(float64(services))

	skippedGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "sage",
		Name:      "reputation_hydrated_skipped",
		Help:      "Reputation states read from storage but not adopted by the startup warm-up: stale past the idle TTL, unparseable, or over the per-shard bound. This is how much history each roll discards — high against sage_reputation_hydrated_keys means the pod is re-learning most of the pool, which happens when endpoints rotate out of the session faster than the TTL keeps them.",
	})
	skippedGauge.Set(float64(skipped))

	return []prometheus.Collector{keysGauge, servicesGauge, skippedGauge}
}

// StaleShareCollector exposes each party's stale head share and the penalty
// the stale_share flag charges it, as reputation's last baseline refresh left
// them (reputation.PartyStale). Derived at scrape time, so a party that has
// faded out of the evidence stops being reported.
type StaleShareCollector struct {
	each      func(yield func(serviceID domain.ServiceID, party string, share, penalty float64, peer bool))
	parties   *labelPolicy
	shareDesc *prometheus.Desc
	penDesc   *prometheus.Desc
}

// NewStaleShareCollector returns a collector over each, which calls yield once
// per measured party.
func NewStaleShareCollector(each func(yield func(serviceID domain.ServiceID, party string, share, penalty float64, peer bool))) *StaleShareCollector {
	return &StaleShareCollector{
		each:    each,
		parties: cappedLabel(maxOperatorLabels),
		shareDesc: prometheus.NewDesc(
			"sage_party_stale_share",
			"Share of a party's answers naming the chain head that were stale (sage_stale_answers_total over the answers sage_answer_head_lag_blocks counts), by service and party, over counts halving every 30 minutes. Only parties with at least 50 answers' evidence. Measured whatever the flags say.",
			[]string{"service_id", "party"}, nil,
		),
		penDesc: prometheus.NewDesc(
			"sage_party_stale_penalty",
			"Points the stale_share flag takes off every reputation key of a party: 0 while its stale share is within 15 points of the service's cleanest party, then linear to -40 at 45 points. 0 where the flag is off or the service has one measured party. source is local, or peer where the penalty is a peer instance's borrowed as a floor (active_health_checks.peer_probe_stream.parties).",
			[]string{"service_id", "party", "source"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *StaleShareCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.shareDesc
	ch <- c.penDesc
}

// Collect implements prometheus.Collector.
func (c *StaleShareCollector) Collect(ch chan<- prometheus.Metric) {
	c.each(func(serviceID domain.ServiceID, party string, share, penalty float64, peer bool) {
		sid, p := sanitizeLabel(string(serviceID)), c.parties.value(party)
		if p == otherLabel {
			return // two parties past the cap would collide on one series
		}
		ch <- prometheus.MustNewConstMetric(c.shareDesc, prometheus.GaugeValue, share, sid, p)
		ch <- prometheus.MustNewConstMetric(c.penDesc, prometheus.GaugeValue, penalty, sid, p, penaltySource(peer))
	})
}

// PartyShareCollector exposes each party's share of a kind of bad answer and
// the penalty it is charged (reputation.PartyShare), as the last refresh left
// them: WebSocket repeats (NewDuplicateShareCollector) or throttled relays
// (NewThrottleShareCollector).
type PartyShareCollector struct {
	each      func(yield func(serviceID domain.ServiceID, party string, share, penalty float64))
	parties   *labelPolicy
	shareDesc *prometheus.Desc
	penDesc   *prometheus.Desc
}

// NewDuplicateShareCollector builds the WebSocket repeat-share collector over
// each, which yields every measured party.
func NewDuplicateShareCollector(each func(yield func(serviceID domain.ServiceID, party string, share, penalty float64))) *PartyShareCollector {
	return &PartyShareCollector{
		each:    each,
		parties: cappedLabel(maxOperatorLabels),
		shareDesc: prometheus.NewDesc(
			"sage_party_duplicate_share",
			"Share of a party's WebSocket subscription notifications that repeated one it had already sent on the subscription, by service and party, over counts halving every 30 minutes. Only parties with at least 500 notifications' evidence on this replica. Measured whatever the flags say.",
			[]string{"service_id", "party"}, nil,
		),
		penDesc: prometheus.NewDesc(
			"sage_party_duplicate_penalty",
			"Points the ws_duplicate_share flag takes off every WebSocket reputation key of a party: 0 while its repeat share is within websocket.duplicate_share_floor of the service's cleanest party, then linear to -40 at websocket.duplicate_share_full. 0 where the flag is off or the service has one measured party.",
			[]string{"service_id", "party"}, nil,
		),
	}
}

// NewThrottleShareCollector builds the throttle-share collector over each,
// which yields every measured party.
func NewThrottleShareCollector(each func(yield func(serviceID domain.ServiceID, party string, share, penalty float64))) *PartyShareCollector {
	return &PartyShareCollector{
		each:    each,
		parties: cappedLabel(maxOperatorLabels),
		shareDesc: prometheus.NewDesc(
			"sage_party_throttle_share",
			"Share of a party's first client attempts (not WebSocket) its backend throttled — HTTP 429 or a node's own rate-limit answer — by service and party, over counts halving every 30 minutes, on this replica. Only parties with at least 500 attempts' evidence. The relay miner's own admission refusals and the session cap are not counted. Measured whatever the flags say.",
			[]string{"service_id", "party"}, nil,
		),
		penDesc: prometheus.NewDesc(
			"sage_party_throttle_penalty",
			"Points the throttle_share flag takes off every non-WebSocket reputation key of a party: 0 while its throttle share is within reputation.throttle_share_floor of the service's cleanest party, then linear to -40 at reputation.throttle_share_full. 0 where the flag is off or the service has one measured party.",
			[]string{"service_id", "party"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *PartyShareCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.shareDesc
	ch <- c.penDesc
}

// Collect implements prometheus.Collector.
func (c *PartyShareCollector) Collect(ch chan<- prometheus.Metric) {
	c.each(func(serviceID domain.ServiceID, party string, share, penalty float64) {
		sid, p := sanitizeLabel(string(serviceID)), c.parties.value(party)
		if p == otherLabel {
			return
		}
		ch <- prometheus.MustNewConstMetric(c.shareDesc, prometheus.GaugeValue, share, sid, p)
		ch <- prometheus.MustNewConstMetric(c.penDesc, prometheus.GaugeValue, penalty, sid, p)
	})
}

// TrustCollector exposes each party's trust evidence and the trust penalty in
// force (reputation.PartyTrust), as the last refresh left them.
type TrustCollector struct {
	each     func(yield func(party string, staleServices, refusalServices int, penalty float64, peer bool))
	parties  *labelPolicy
	evidence *prometheus.Desc
	penalty  *prometheus.Desc
}

// NewTrustCollector returns a collector over each, which calls yield once per
// party with evidence or a penalty.
func NewTrustCollector(each func(yield func(party string, staleServices, refusalServices int, penalty float64, peer bool))) *TrustCollector {
	return &TrustCollector{
		each:    each,
		parties: cappedLabel(maxOperatorLabels),
		evidence: prometheus.NewDesc(
			"sage_party_trust_evidence",
			"Services on which a party currently carries trust evidence, by party and kind: stale (stale share more than 15 points over the service's cleanest party, on services with stale_share on; 8 services set the trust penalty) or refusal (10 or more refused_recent verdicts in about the last hour; 2 services set it). Counted whatever the trust_penalty flag says.",
			[]string{"party", "kind"}, nil,
		),
		penalty: prometheus.NewDesc(
			"sage_party_trust_penalty",
			"The trust penalty in force for a party: -30 from when its evidence crossed either bar until 24 hours after it last did, else 0. Charged on services with trust_penalty on, as the larger of it and the party's stale-share penalty. source is local, or peer where the penalty is a peer instance's borrowed as a floor (active_health_checks.peer_probe_stream.parties).",
			[]string{"party", "source"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *TrustCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.evidence
	ch <- c.penalty
}

// Collect implements prometheus.Collector.
func (c *TrustCollector) Collect(ch chan<- prometheus.Metric) {
	c.each(func(party string, staleServices, refusalServices int, penalty float64, peer bool) {
		p := c.parties.value(party)
		if p == otherLabel {
			return // two parties past the cap would collide on one series
		}
		ch <- prometheus.MustNewConstMetric(c.evidence, prometheus.GaugeValue, float64(staleServices), p, "stale")
		ch <- prometheus.MustNewConstMetric(c.evidence, prometheus.GaugeValue, float64(refusalServices), p, "refusal")
		ch <- prometheus.MustNewConstMetric(c.penalty, prometheus.GaugeValue, penalty, p, penaltySource(peer))
	})
}

// PolicyPenaltyCollector exposes the policy penalties set by hand
// (reputation.PolicyPenalty) the last refresh charged, with their reason.
type PolicyPenaltyCollector struct {
	each    func(yield func(party, reason string, penalty float64))
	parties *labelPolicy
	desc    *prometheus.Desc
}

// NewPolicyPenaltyCollector returns a collector over each, which calls yield
// once per policy penalty in force.
func NewPolicyPenaltyCollector(each func(yield func(party, reason string, penalty float64))) *PolicyPenaltyCollector {
	return &PolicyPenaltyCollector{
		each:    each,
		parties: cappedLabel(maxOperatorLabels),
		desc: prometheus.NewDesc(
			"sage_party_policy_penalty",
			"The policy penalty an operator of the gateway set by hand on a party (PUT /admin/reputation/policy/{party}), with the reason given: for conduct SAGE cannot measure, such as reselling a public RPC. Kept apart from sage_party_trust_penalty, which is measured. Charged on services with policy_penalty on, as the largest of the party's penalties. Absent when none is set.",
			[]string{"party", "reason"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *PolicyPenaltyCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector.
func (c *PolicyPenaltyCollector) Collect(ch chan<- prometheus.Metric) {
	c.each(func(party, reason string, penalty float64) {
		p := c.parties.value(party)
		if p == otherLabel {
			return
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, penalty, p, reason)
	})
}

// penaltySource is the source label of a party penalty.
func penaltySource(peer bool) string {
	if peer {
		return "peer"
	}
	return "local"
}

// PartyPenaltiesWebsocketCollector exposes, per service, whether the party
// penalties reach its websocket keys (featureflag.FlagPartyPenaltiesWebsocket),
// read at scrape; the reputation refresh applies a change within 30 s.
type PartyPenaltiesWebsocketCollector struct {
	services []domain.ServiceID
	charged  func(domain.ServiceID) bool
	desc     *prometheus.Desc
}

// NewPartyPenaltiesWebsocketCollector reports charged for each of services.
func NewPartyPenaltiesWebsocketCollector(services []domain.ServiceID, charged func(domain.ServiceID) bool) *PartyPenaltiesWebsocketCollector {
	return &PartyPenaltiesWebsocketCollector{
		services: services,
		charged:  charged,
		desc: prometheus.NewDesc(
			"sage_party_penalties_websocket",
			"1 when a service's WebSocket keys carry their party's trust and stale-share penalties, 0 when they are scored on their own WebSocket grades alone (the party_penalties_websocket flag, applied at the next reputation refresh). HTTP keys carry the penalties either way.",
			[]string{"service_id"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *PartyPenaltiesWebsocketCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector.
func (c *PartyPenaltiesWebsocketCollector) Collect(ch chan<- prometheus.Metric) {
	for _, svc := range c.services {
		v := 0.0
		if c.charged(svc) {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, v, string(svc))
	}
}
