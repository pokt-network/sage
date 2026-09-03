package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// ChainViewSource hands the collector one service's chain view. qos.Registry
// satisfies it through the plugins it holds.
type ChainViewSource interface {
	ChainViewFor(serviceID domain.ServiceID) (qos.ChainView, bool)
}

// ChainViewCollector exports what each service believes about its chain:
//
//	sage_chain_view_height{service_id}             perceived height
//	sage_chain_view_spread_blocks{service_id}      highest minus lowest endpoint
//	sage_chain_view_endpoints{service_id}          endpoints inside the window
//	sage_chain_view_staleness_seconds{service_id}  age of the newest observation
//
// None of this was exported before. SAGE published 31 metrics and not one was
// about block height, consensus or QoS state, so the mechanism endpoint
// selection tiers on could not be seen from outside the process at all — which
// is how traffic-informed probing shipped able to starve a service of its
// height source without anything showing it (mainnet canary, 2026-09-03).
//
// Staleness is the one to alert on. A service being probed refreshes every
// cycle; a service relying on sampled client traffic for its height refreshes
// only when a client happens to ask for it, and the gap between those two is
// invisible in every other series. Spread is the second: it is how far apart
// the pool is, which is what the height filters act on.
//
// A Collector rather than pushed gauges, matching BreakerCollector: the values
// are derived at scrape time from state that ages on its own, so they cannot
// go stale, cost nothing on the relay path, and a service that stops reporting
// stops being exported rather than freezing at its last value.
//
// A service whose plugin tracks no block height is absent entirely, not zero.
type ChainViewCollector struct {
	source   ChainViewSource
	services []domain.ServiceID
	now      func() time.Time

	height        *prometheus.Desc
	spread        *prometheus.Desc
	endpoints     *prometheus.Desc
	staleness     *prometheus.Desc
	spreadSeconds *prometheus.Desc
}

// NewChainViewCollector returns a collector for the given services. It does not
// register itself; the caller decides which registry it belongs to.
func NewChainViewCollector(source ChainViewSource, services []domain.ServiceID) *ChainViewCollector {
	label := []string{"service_id"}
	return &ChainViewCollector{
		source:   source,
		services: services,
		now:      time.Now,
		height: prometheus.NewDesc(
			"sage_chain_view_height",
			"Block height consensus settled on for this service — the number endpoint selection filters against. Absent for a service whose plugin tracks no height.",
			label, nil,
		),
		spread: prometheus.NewDesc(
			"sage_chain_view_spread_blocks",
			"Blocks between the highest and lowest endpoint inside the consensus window. Zero means agreement OR no observations at all; read it with sage_chain_view_endpoints, which separates the two.",
			label, nil,
		),
		endpoints: prometheus.NewDesc(
			"sage_chain_view_endpoints",
			"Distinct endpoints that reported a height inside the consensus window. One is not a consensus; zero means the service is selecting on a height nothing currently confirms.",
			label, nil,
		),
		spreadSeconds: prometheus.NewDesc(
			"sage_chain_view_spread_seconds",
			"The block spread expressed as time, using a block rate derived from how fast this chain's perceived height moves. This is the figure to compare ACROSS services: blocks are not comparable between chains, so 534 blocks on a quarter-second chain and 11 blocks on a twelve-second chain are the same 133 seconds. Absent when the chain has not moved enough to derive a rate — a stalled chain has no rate, and guessing one would report a confident wrong number.",
			label, nil,
		),
		staleness: prometheus.NewDesc(
			"sage_chain_view_staleness_seconds",
			"Age of the newest block-height observation for this service. A probed service refreshes every health-check cycle; one whose probes are skipped refreshes only when client traffic happens to carry a height, so this is what shows a chain view going stale. Absent when there is no observation in the window — sage_chain_view_endpoints reads 0 there.",
			label, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *ChainViewCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.height
	ch <- c.spread
	ch <- c.endpoints
	ch <- c.staleness
	ch <- c.spreadSeconds
}

// Collect implements prometheus.Collector.
func (c *ChainViewCollector) Collect(ch chan<- prometheus.Metric) {
	now := c.now()
	for _, serviceID := range c.services {
		view, ok := c.source.ChainViewFor(serviceID)
		if !ok {
			continue
		}
		id := string(serviceID)
		ch <- prometheus.MustNewConstMetric(c.height, prometheus.GaugeValue, float64(view.Perceived), id)
		ch <- prometheus.MustNewConstMetric(c.spread, prometheus.GaugeValue, float64(view.Spread()), id)
		ch <- prometheus.MustNewConstMetric(c.endpoints, prometheus.GaugeValue, float64(view.Endpoints), id)
		if secs, ok := view.SpreadSeconds(); ok {
			ch <- prometheus.MustNewConstMetric(c.spreadSeconds, prometheus.GaugeValue, secs, id)
		}
		// Emitted only when there is something to be stale: a zero timestamp
		// would otherwise export the age of the Unix epoch, which reads as a
		// catastrophically stale chain rather than as no data.
		if !view.Newest.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.staleness, prometheus.GaugeValue, now.Sub(view.Newest).Seconds(), id)
		}
	}
}
