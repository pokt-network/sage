package metrics

import (
	"context"
	"sort"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// ScoreLister reports the current reputation scores for a service, keyed by
// reputation key. reputation.Service satisfies it.
type ScoreLister interface {
	GetScores(ctx context.Context, serviceID domain.ServiceID) (map[string]float64, error)
}

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
// same reason as BreakerCollector but a sharper one. A pushed GaugeVec keyed on
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
}

// NewScoreCollector returns a collector for the given services. fullScore is
// the reputation ceiling (ServiceConfig.MaxScore); keys at it are not
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
	}
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

// NewHydratedGauges exposes what the startup warm-up read loaded:
//
//	sage_reputation_hydrated_keys <count>
//	sage_reputation_hydrated_services <count>
//
// Both are set once, at startup, and never change — which is the point. The
// only other evidence that hydration ran is a log line, and on the mainnet
// canary (2026-09-02) that line was invisible: the log level suppresses INFO,
// so the first roll carrying hydration had to be confirmed by inferring it
// from sage_reputation_keys being implausibly high for a fresh pod. These say
// it directly, in the place operators already scrape.
//
// Zero keys on a pod that should have inherited state is the signal worth
// alerting on: it means the store was empty, unreachable, or entirely stale,
// and the pod is warming from probes the slow way.
// The Name and Help below are spelled out per gauge rather than passed to a
// shared helper: internal/docgen reads these literals out of the AST to
// generate docs/metrics.md, and a metric named by a variable is a metric the
// reference silently omits.
func NewHydratedGauges(keys, services int) []prometheus.Collector {
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

	return []prometheus.Collector{keysGauge, servicesGauge}
}
