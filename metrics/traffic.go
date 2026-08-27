package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// TrafficSummaryLister reports a service's previous (complete) request-shape
// window. traffic.Sampler satisfies it through an adapter in wire.go. ok is
// false when the service has no completed window yet — either it has never
// been observed, or its current window has not rolled over once.
type TrafficSummaryLister interface {
	PreviousWindow(serviceID string) (distinctRatio, top1Share float64, ok bool)
}

// TrafficCollector exposes request-shape sampling as two gauges:
//
//	sage_request_sample_distinct_ratio{service_id}
//	sage_request_sample_top_share{service_id}
//
// Both are read from the PREVIOUS (complete) window, not the one still
// filling — a window in progress understates diversity for any client who
// hasn't sent every fingerprint yet, so reporting it would make a healthy
// service look more cache-friendly than it is until the window happens to
// close. Derived at scrape time, like BreakerCollector: a service with no
// completed window is absent from both series, not zero.
type TrafficCollector struct {
	lister   TrafficSummaryLister
	services []domain.ServiceID

	distinctRatioDesc *prometheus.Desc
	top1ShareDesc     *prometheus.Desc
}

// NewTrafficCollector returns a collector for the given services. It does not
// register itself.
func NewTrafficCollector(lister TrafficSummaryLister, services []domain.ServiceID) *TrafficCollector {
	return &TrafficCollector{
		lister:   lister,
		services: services,
		distinctRatioDesc: prometheus.NewDesc(
			"sage_request_sample_distinct_ratio",
			"Distinct fingerprints divided by sampled requests, for the previous complete request-sample window. Absent when the service has no completed window yet.",
			[]string{"service_id"},
			nil,
		),
		top1ShareDesc: prometheus.NewDesc(
			"sage_request_sample_top_share",
			"Share of sampled requests taken by the single most common request fingerprint, for the previous complete request-sample window. Absent when the service has no completed window yet.",
			[]string{"service_id"},
			nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *TrafficCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.distinctRatioDesc
	ch <- c.top1ShareDesc
}

// Collect implements prometheus.Collector. Called on scrape, not on the hot path.
func (c *TrafficCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil {
		return
	}
	for _, serviceID := range c.services {
		distinctRatio, top1Share, ok := c.lister.PreviousWindow(string(serviceID))
		if !ok {
			continue
		}
		sid := sanitizeLabel(string(serviceID))
		ch <- prometheus.MustNewConstMetric(c.distinctRatioDesc, prometheus.GaugeValue, distinctRatio, sid)
		ch <- prometheus.MustNewConstMetric(c.top1ShareDesc, prometheus.GaugeValue, top1Share, sid)
	}
}
