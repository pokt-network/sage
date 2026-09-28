package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
)

// SessionEndpointLister reports every registration in a service's current
// session that serves an RPC type, excluded or not. protocol/shannon.Protocol
// satisfies it.
type SessionEndpointLister interface {
	RegisteredEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error)
}

// EndpointScorer reports an endpoint's reputation score and whether one has
// been recorded. reputation.Service's implementation satisfies it.
type EndpointScorer interface {
	ScoreOf(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) (float64, bool)
}

// SessionCollector exposes, per service, operator and RPC type, what the
// current session holds:
//
//	sage_session_endpoints{service_id, operator, rpc_type}       registrations
//	sage_session_endpoints_low{service_id, operator, rpc_type}   scored below tier 1
//	sage_operator_reputation_mean{service_id, operator, rpc_type} mean score of the scored ones
//
// The unit is the registration, not the URL or the reputation key: every
// operator is counted the same way whether it fronts all its registrations
// with one host or gives each its own, so these compare across operators. It
// is PATH's endpoint count. An unscored registration counts toward the total
// only; charging it InitialScore would raise the mean of an operator whose
// registrations are new rather than good.
//
// Derived at scrape time from the session cache and the score map, so a
// registration that left the session stops being reported.
type SessionCollector struct {
	lister    SessionEndpointLister
	scorer    EndpointScorer
	services  map[domain.ServiceID][]domain.RPCType
	tier1     float64
	operators *labelPolicy

	endpointsDesc *prometheus.Desc
	lowDesc       *prometheus.Desc
	meanDesc      *prometheus.Desc
}

// NewSessionCollector returns a collector for the given services and the RPC
// types each is configured for. tier1 is the selector's tier-1 threshold.
func NewSessionCollector(lister SessionEndpointLister, scorer EndpointScorer, services map[domain.ServiceID][]domain.RPCType, tier1 float64) *SessionCollector {
	labels := []string{"service_id", "operator", "rpc_type"}
	return &SessionCollector{
		lister:    lister,
		scorer:    scorer,
		services:  services,
		tier1:     tier1,
		operators: cappedLabel(maxOperatorLabels),
		endpointsDesc: prometheus.NewDesc(
			"sage_session_endpoints",
			"Registrations in the service's current session that serve the RPC type, by operator (the registrable domain of the registration's URL), whether or not they are currently excluded. The unit is the registration, so operators compare regardless of how many hosts they front them with.",
			labels, nil,
		),
		lowDesc: prometheus.NewDesc(
			"sage_session_endpoints_low",
			"Registrations in the current session whose reputation score for the RPC type is below the tier-1 threshold, by operator. Unscored registrations are not counted.",
			labels, nil,
		),
		meanDesc: prometheus.NewDesc(
			"sage_operator_reputation_mean",
			"Mean reputation score of an operator's registrations in the service's current session for the RPC type, over the registrations that have a score. Two registrations behind one URL share a key and count twice: they are two slots in the pool.",
			labels, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *SessionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.endpointsDesc
	ch <- c.lowDesc
	ch <- c.meanDesc
}

// Collect implements prometheus.Collector. A service whose session cannot be
// read is skipped: absence reads as "no data", zero would read as "none".
func (c *SessionCollector) Collect(ch chan<- prometheus.Metric) {
	if c.lister == nil || c.scorer == nil {
		return
	}
	ctx := context.Background()
	type agg struct {
		n, scored, low int
		sum            float64
	}
	for serviceID, rpcTypes := range c.services {
		sid := sanitizeLabel(string(serviceID))
		for _, rpcType := range rpcTypes {
			eps, err := c.lister.RegisteredEndpoints(ctx, serviceID, rpcType)
			if err != nil {
				continue
			}
			byOp := map[string]*agg{}
			for _, ep := range eps {
				op := c.operators.value(ep.Operator())
				a := byOp[op]
				if a == nil {
					a = &agg{}
					byOp[op] = a
				}
				a.n++
				if score, ok := c.scorer.ScoreOf(serviceID, ep, rpcType); ok {
					a.scored++
					a.sum += score
					if score < c.tier1 {
						a.low++
					}
				}
			}
			rpc := sanitizeLabel(string(rpcType))
			for op, a := range byOp {
				ch <- prometheus.MustNewConstMetric(c.endpointsDesc, prometheus.GaugeValue, float64(a.n), sid, op, rpc)
				ch <- prometheus.MustNewConstMetric(c.lowDesc, prometheus.GaugeValue, float64(a.low), sid, op, rpc)
				if a.scored > 0 {
					ch <- prometheus.MustNewConstMetric(c.meanDesc, prometheus.GaugeValue, a.sum/float64(a.scored), sid, op, rpc)
				}
			}
		}
	}
}
