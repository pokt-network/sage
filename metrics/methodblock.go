package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// MethodBlock is one active method block as the collector reads it. Method is
// "" for a host-level block.
type MethodBlock struct {
	Host   string
	Method string
}

// MethodBlockLister reports the live method blocks for a service.
// methodblock.Store satisfies it through an adapter in wire.go.
type MethodBlockLister interface {
	ActiveMethodBlocks(serviceID string) []MethodBlock
}

// MethodBlockCollector exposes method blocks as a gauge:
//
//	sage_method_blocks{service_id, domain, method} 1
//
// Derived at scrape time, like BreakerCollector: blocks expire lazily, so a
// pushed gauge would never clear. Absent means no block. method is the
// plugin's catalogued name (bounded) or "" for a host-level block.
type MethodBlockCollector struct {
	lister   MethodBlockLister
	services []domain.ServiceID
	desc     *prometheus.Desc
}

// NewMethodBlockCollector returns a collector for the given services. It does
// not register itself.
func NewMethodBlockCollector(lister MethodBlockLister, services []domain.ServiceID) *MethodBlockCollector {
	return &MethodBlockCollector{
		lister:   lister,
		services: services,
		desc: prometheus.NewDesc(
			"sage_method_blocks",
			"1 while a host is blocked from receiving a method for this service (method empty = blocked for every method). Absent when nothing is blocked.",
			[]string{"service_id", "domain", "method"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *MethodBlockCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector. Called on scrape, not on the hot path.
func (c *MethodBlockCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil {
		return
	}
	for _, serviceID := range c.services {
		for _, b := range c.lister.ActiveMethodBlocks(string(serviceID)) {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1,
				sanitizeLabel(string(serviceID)), sanitizeLabel(b.Host), b.Method)
		}
	}
}
