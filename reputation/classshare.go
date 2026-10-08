package reputation

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
)

// One score per key mixes every method a key answers, and the cheap ones are
// most of the traffic: a party that answers heads and health checks and fails
// the calls that cost a node something keeps a score its cheap traffic earns.
// Each method class (Signal.Class) keeps its own failure share per party,
// priced against the service's cleanest party in that class, and charged only
// when selecting for a request of that class (featureflag.FlagClassShare).
const (
	// classShareHalfLife ages the counts by the clock, as throttleShareHalfLife.
	classShareHalfLife = 30 * time.Minute
	// classShareMinAttempts is the decayed evidence a party needs in a class
	// to be measured there, or to set the class's baseline.
	classShareMinAttempts = 200
	// classSharePenalty is the most the term takes off a score.
	classSharePenalty = -40.0
	// DefaultClassShareFloor is the excess over the cleanest party charged
	// nothing, and DefaultClassShareFull the excess charged the whole
	// penalty: the bases of the reputation.class_share_* knobs.
	DefaultClassShareFloor = 0.05
	DefaultClassShareFull  = 0.30
)

// Method classes, by what a method costs a node to answer.
const (
	ClassLight    = "light"
	ClassStandard = "standard"
	ClassHeavy    = "heavy"
)

// classRule prices a class's failures.
var classRule = shareRule{minEvidence: classShareMinAttempts, most: classSharePenalty, hold: classShareHalfLife,
	floor: DefaultClassShareFloor, full: DefaultClassShareFull}

// methodClassKey carries a request's method class to the selector's score
// lookup.
type methodClassKey struct{}

// WithMethodClass returns ctx carrying class, for a SelectBest call to rank by
// the class's penalties as well as the key's own. Only the selection call
// should carry it: a vouched or ruled-out check is about the key.
func WithMethodClass(ctx context.Context, class string) context.Context {
	return context.WithValue(ctx, methodClassKey{}, class)
}

// MethodClassFrom is the method class WithMethodClass put on ctx, "" when
// none: what a Service's score lookup ranks a selection by.
func MethodClassFrom(ctx context.Context) string {
	class, _ := ctx.Value(methodClassKey{}).(string)
	return class
}

// SetClassShare turns on the class-share penalty per service behind gate;
// limits gives a service's floor and full excess. Both are read on each
// refresh. Call at wire time.
func (s *serviceImpl) SetClassShare(gate func(domain.ServiceID) bool, limits func(domain.ServiceID) (floor, full float64)) {
	s.classGate.Store(&gate)
	s.classLimits.Store(&limits)
}

// PartyClassShares reports every party measured in each class, for the
// metrics collector.
func (s *serviceImpl) PartyClassShares() map[string][]PartyShare {
	if v := s.chronic.Load(); v != nil {
		return v.classes
	}
	return nil
}

// recordClass counts one graded attempt toward its party's share in the
// request's class: first client attempts only, as recordThrottle, weighted by
// FailureWeight so a minor verdict (a throttle) is not a failure here.
func (s *serviceImpl) recordClass(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, signal Signal, now time.Time) {
	if signal.Type == "" || signal.Probe || signal.Leftover || rpcType == domain.RPCTypeWebSocket {
		return
	}
	t := s.classes[signal.Class]
	party := endpoint.Party()
	if t == nil || party == "" {
		return
	}
	t.recordN(opID{svc: serviceID, op: party}, 1, FailureWeight(signal.Type), now)
}

// priceClasses measures and prices every class tracker into v.
func (s *serviceImpl) priceClasses(v *chronicView, prev *chronicView, now time.Time) {
	var limits func(domain.ServiceID) (float64, float64)
	if lp := s.classLimits.Load(); lp != nil {
		limits = *lp
	}
	v.classes = make(map[string][]PartyShare, len(s.classes))
	for class, t := range s.classes {
		var held []PartyShare
		if prev != nil {
			held = prev.classes[class]
		}
		shares := priceShares(t.snapshot(now), classRule, gateOf(&s.classGate), limits, held, now)
		v.classes[class] = shares
		for _, p := range shares {
			if p.Penalty < 0 {
				if v.classPen == nil {
					v.classPen = map[string]map[opID]float64{}
				}
				if v.classPen[class] == nil {
					v.classPen[class] = map[opID]float64{}
				}
				v.classPen[class][opID{svc: p.ServiceID, op: p.Party}] = p.Penalty
			}
		}
	}
}

// classPenalty is what a key's party's failures in class cost it in a
// service: 0 for a WebSocket key or an unclassed request.
func (v *chronicView) classPenalty(svc domain.ServiceID, key, class string) float64 {
	if v == nil || class == "" || len(v.classPen[class]) == 0 || rpcOfKey(key) == string(domain.RPCTypeWebSocket) {
		return 0
	}
	party, ok := v.keyParty[key]
	if !ok {
		party = partyOfKey(key)
	}
	return v.classPen[class][opID{svc: svc, op: party}]
}
