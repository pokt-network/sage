package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// BrokenDomainLister reports which domains are currently circuit-broken for a
// service. circuitbreaker.Breaker satisfies it.
type BrokenDomainLister interface {
	BrokenDomains(serviceID string) []string
}

// BreakerCollector exposes circuit-breaker state as a Prometheus gauge:
//
//	sage_circuit_breaker_state{service_id, domain} 1
//
// It answers the question the error-rate graphs cannot — "which domains are
// circuit-broken right now?" — which previously needed the admin API or a look
// in Redis.
//
// A Collector rather than a gauge the breaker pushes to, because SAGE expires
// breaks lazily: nothing fires when a break runs out, so a pushed gauge would
// sit at 1 until the domain happened to break again. Deriving the value at
// scrape time means it cannot be stale, costs nothing on the relay path, and
// leaves no series behind — a domain that recovers stops being reported rather
// than lingering at 0 forever. (PATH pushes on transition and re-asserts from
// Redis on read; SAGE's Redis is optional, so that would not hold here.)
//
// Absence means healthy. sum(sage_circuit_breaker_state) is the count of broken
// domains, and the metric's cardinality is bounded by what is broken right now.
type BreakerCollector struct {
	lister   BrokenDomainLister
	services []domain.ServiceID
	desc     *prometheus.Desc
}

// NewBreakerCollector returns a collector for the given services. It does not
// register itself; the caller decides which registry it belongs to.
func NewBreakerCollector(lister BrokenDomainLister, services []domain.ServiceID) *BreakerCollector {
	return &BreakerCollector{
		lister:   lister,
		services: services,
		desc: prometheus.NewDesc(
			"sage_circuit_breaker_state",
			"1 while a domain is circuit-broken for this service (locked out of selection). Absent when healthy.",
			[]string{"service_id", "domain"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *BreakerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector. Called on scrape, not on the hot
// path.
func (c *BreakerCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil {
		return
	}
	for _, serviceID := range c.services {
		for _, brokenDomain := range c.lister.BrokenDomains(string(serviceID)) {
			ch <- prometheus.MustNewConstMetric(
				c.desc,
				prometheus.GaugeValue,
				1,
				sanitizeLabel(string(serviceID)),
				sanitizeLabel(brokenDomain),
			)
		}
	}
}
