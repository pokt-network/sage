package metrics

import "github.com/prometheus/client_golang/prometheus"

// NewBlockedDomainsGauge exposes how many domains are banned through the
// admin API (package blocklist), on top of the config list:
//
//	sage_blocked_domains_admin <count>
//
// Config entries are not counted — they are in the file. A non-zero value on
// a fresh replica is a ban it inherited through Redis.
func NewBlockedDomainsGauge(count func() int) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "blocked_domains_admin",
			Help:      "Domains banned through the admin API (PUT /admin/blocked-domains), in force on this replica, not counting gateway_config.blocked_domains. Non-zero on a fresh replica is a ban inherited through Redis.",
		},
		func() float64 { return float64(count()) },
	)
}
