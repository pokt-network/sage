package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// DrainEntry is one active operator drain as the collector reads it. RPCType
// is "" for a drain that covers every RPC type.
type DrainEntry struct {
	Domain  string
	RPCType string
}

// DrainLister reports the live operator drains for a service.
// drain.Store satisfies it through an adapter in wire.go.
type DrainLister interface {
	ActiveDrains(serviceID string) []DrainEntry
}

// DrainCollector exposes operator drains as a gauge:
//
//	sage_drained_operators{service_id, domain, rpc_type} 1
//
// Derived at scrape time, like BreakerCollector and MethodBlockCollector:
// drains expire lazily, so a pushed gauge would never clear. Absent means no
// drain. rpc_type is "all" for a drain that covers every RPC type, since an
// empty label value reads as "unset" rather than "every type" on a dashboard.
type DrainCollector struct {
	lister   DrainLister
	services []domain.ServiceID
	desc     *prometheus.Desc
}

// NewDrainCollector returns a collector for the given services. It does not
// register itself.
func NewDrainCollector(lister DrainLister, services []domain.ServiceID) *DrainCollector {
	return &DrainCollector{
		lister:   lister,
		services: services,
		desc: prometheus.NewDesc(
			"sage_drained_operators",
			"1 while an operator is drained from a service (rpc_type \"all\" = every RPC type). Absent when nothing is drained.",
			[]string{"service_id", "domain", "rpc_type"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *DrainCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector. Called on scrape, not on the hot path.
func (c *DrainCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil {
		return
	}
	for _, serviceID := range c.services {
		for _, d := range c.lister.ActiveDrains(string(serviceID)) {
			rpcType := d.RPCType
			if rpcType == "" {
				rpcType = "all"
			}
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1,
				sanitizeLabel(string(serviceID)), sanitizeLabel(d.Domain), rpcType)
		}
	}
}
