package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// PresenceCollector exposes a gauge that is 1 while a row is present and
// absent otherwise:
//
//	<name>{service_id, <labels>...} 1
//
// It backs the circuit-breaker, drain and method-block gauges, which all
// answer "what is in force right now?" for state that expires lazily.
//
// A Collector rather than a gauge the owner pushes to, because nothing fires
// when such state runs out, so a pushed gauge would sit at 1 until the next
// event. Deriving the value at scrape time means it cannot be stale, costs
// nothing on the relay path, and leaves no series behind — a row that clears
// stops being reported rather than lingering at 0 forever. (PATH pushes on
// transition and re-asserts from Redis on read; SAGE's Redis is optional, so
// that would not hold here.)
//
// Absence means nothing is in force, so cardinality is bounded by what is in
// force right now. service_id comes from the configured services only, and
// every label value is sanitized.
type PresenceCollector struct {
	desc     *prometheus.Desc
	services []domain.ServiceID
	rows     PresenceRows
}

// PresenceRows yields the label values, after service_id, of each row present
// for serviceID.
type PresenceRows func(serviceID string, yield func(labels ...string))

// Describe implements prometheus.Collector.
func (c *PresenceCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector. Called on scrape, not on the hot
// path.
func (c *PresenceCollector) Collect(ch chan<- prometheus.Metric) {
	if c.rows == nil {
		return
	}
	for _, serviceID := range c.services {
		c.rows(string(serviceID), func(labels ...string) {
			values := make([]string, 0, len(labels)+1)
			values = append(values, sanitizeLabel(string(serviceID)))
			for _, l := range labels {
				values = append(values, sanitizeLabel(l))
			}
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1, values...)
		})
	}
}

// NewBreakerCollector reports which domains are circuit-broken right now:
//
//	sage_circuit_breaker_state{service_id, domain} 1
//
// broken yields each broken domain of a service. It answers the question the
// error-rate graphs cannot without the admin API or a look in Redis;
// sum(sage_circuit_breaker_state) is the count of broken domains. Like every
// constructor here it does not register itself.
func NewBreakerCollector(services []domain.ServiceID, broken PresenceRows) *PresenceCollector {
	return &PresenceCollector{
		desc: prometheus.NewDesc(
			"sage_circuit_breaker_state",
			"1 while a domain is circuit-broken for this service (locked out of selection). Absent when healthy.",
			[]string{"service_id", "domain"},
			nil,
		),
		services: services,
		rows:     broken,
	}
}

// NewDrainCollector reports operator drains:
//
//	sage_drained_operators{service_id, domain, rpc_type} 1
//
// drains yields (domain, rpcType) for each live drain of a service, rpcType ""
// for a drain that covers every RPC type. That is reported as "all", since an
// empty label value reads as "unset" rather than "every type" on a dashboard.
func NewDrainCollector(services []domain.ServiceID, drains PresenceRows) *PresenceCollector {
	c := &PresenceCollector{
		desc: prometheus.NewDesc(
			"sage_drained_operators",
			"1 while an operator is drained from a service (rpc_type \"all\" = every RPC type). Absent when nothing is drained.",
			[]string{"service_id", "domain", "rpc_type"},
			nil,
		),
		services: services,
	}
	if drains != nil {
		c.rows = func(serviceID string, yield func(labels ...string)) {
			drains(serviceID, func(labels ...string) {
				if len(labels) == 2 && labels[1] == "" {
					labels[1] = "all"
				}
				yield(labels...)
			})
		}
	}
	return c
}

// NewMethodBlockCollector reports method blocks:
//
//	sage_method_blocks{service_id, domain, method} 1
//
// blocks yields (host, method) for each live block of a service. method is the
// plugin's catalogued name (bounded) or "" for a host-level block.
func NewMethodBlockCollector(services []domain.ServiceID, blocks PresenceRows) *PresenceCollector {
	return &PresenceCollector{
		desc: prometheus.NewDesc(
			"sage_method_blocks",
			"1 while a host is blocked from receiving a method for this service (method empty = blocked for every method). Absent when nothing is blocked.",
			[]string{"service_id", "domain", "method"},
			nil,
		),
		services: services,
		rows:     blocks,
	}
}
