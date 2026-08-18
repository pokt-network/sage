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
// in a single scrape.
//
// At the default per-URL granularity the live key set is backend URLs × RPC
// types — hundreds, not thousands — and this never binds. It binds under
// per-endpoint, where the key carries the supplier address: a supplier is a
// staked registration that rotates every session, so the distinct set grows
// with the network rather than with SAGE's traffic. PATH measured the
// equivalent metric at 4,510 keys live in a 10-minute window against 74,639
// distinct over 7.7 hours on one pod — a 16.5× multiple that a cap alone would
// not have caught, because each key sat well inside any cap while it existed.
const maxScoreSeriesPerService = 2000

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
// When a service has more keys than maxScoreSeriesPerService, the LOWEST scores
// are kept. Truncation is reported on sage_endpoint_reputation_scores_dropped,
// so a trimmed scrape is visible rather than silently partial — and what
// survives is what a runbook is looking for.
type ScoreCollector struct {
	lister   ScoreLister
	services []domain.ServiceID

	scoreDesc   *prometheus.Desc
	droppedDesc *prometheus.Desc
}

// NewScoreCollector returns a collector for the given services. It does not
// register itself; the caller decides which registry it belongs to.
func NewScoreCollector(lister ScoreLister, services []domain.ServiceID) *ScoreCollector {
	return &ScoreCollector{
		lister:   lister,
		services: services,
		scoreDesc: prometheus.NewDesc(
			"sage_endpoint_reputation_score",
			"Current reputation score, by service and reputation key (see reputation/key.go for what a key covers).",
			[]string{"service_id", "endpoint"},
			nil,
		),
		droppedDesc: prometheus.NewDesc(
			"sage_endpoint_reputation_scores_dropped",
			"Reputation keys omitted from this scrape because the service exceeded the per-scrape cap. Non-zero means sage_endpoint_reputation_score is showing only the lowest-scoring keys.",
			[]string{"service_id"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *ScoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.scoreDesc
	ch <- c.droppedDesc
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

		keys := make([]string, 0, len(scores))
		for k := range scores {
			keys = append(keys, k)
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
	}
}
