package reputation

import (
	"strings"
	"time"

	"github.com/pokt-network/sage/domain"
)

// The chronic term reads a rate per key, and a key is one backend URL. That
// makes the measurement depend on how an operator spreads its traffic, which
// is not a property of how well it answers:
//
//   - An EWMA starting at zero has only reached 1-e^(-lambda*n) of the true
//     rate after n attempts. At the default half-life (20,000 attempts) a key
//     with 300 attempts shows ~1% of its real failure rate, and the 0.02%
//     onset cannot be crossed however badly it answers.
//   - So an operator spreading one service over 90 URLs keeps every key young
//     and every rate near zero, while one concentrating the same traffic on 7
//     URLs shows its rate in full and pays the whole pool's penalty.
//
// Mainnet sei, 2026-09-16, is the measurement: one operator's 7 keys at
// 41k-105k attempts showed 3.5-4.5% and scored 10-39, while the other's ~90
// keys at 200-6,000 attempts showed 0.06-1.6% and scored 90-100. Corrected for
// warm-up the second operator was failing 5-9.5% of the time — worse, not
// better. The pool baseline was one of its youngest keys, so the first
// operator paid the difference against a rate nothing actually exhibited.
//
// The fix is to measure the rate per operator, on counters kept against the
// operator itself rather than derived from its keys (opstats.go).

// opID is one operator's slice of a (service, RPC type) pool.
type opID struct {
	svc domain.ServiceID
	op  string
	rpc string
}

// OperatorRateView is what one operator's keys add up to in a pool.
type OperatorRateView struct {
	// Rate is the operator's failure rate from its decayed counters
	// (opstats.go), forgiven on a recovery.
	Rate float64
	// Attempts is how much evidence that rate rests on.
	Attempts uint64
}

// OperatorStatsLen is how many per-operator counters are held, for the gauge.
func (s *serviceImpl) OperatorStatsLen() int { return s.ops.len() }

// chronicView is everything the chronic term reads, rebuilt every
// baselineRefresh and swapped in whole. Reading it costs one atomic load and
// one map lookup, which is what the relay path can afford: penaltyFor runs per
// candidate endpoint per relay.
type chronicView struct {
	// byKey is the operator rate to charge a key, for services where the
	// operator term is on. Absent means charge the key's own rate.
	byKey map[keyID]float64
	// baseline is the pool's best rate, in whichever basis the pool is
	// measured in. Absent means charge from zero.
	baseline map[poolID]float64
	// opOn records which services had the operator term on at refresh time, so
	// the flag is not read per relay.
	opOn map[domain.ServiceID]bool
	// stalePen is the stale-share penalty of each priced (service, party);
	// stale is every measured party, for the metrics (staleshare.go).
	stalePen map[opID]float64
	stale    []PartyStale
	// trustPen is the trust penalty of each distrusted party, charged in a
	// service where trustOn (read at refresh) or, for a service with no key
	// at refresh, trustGate says so; trust is every party with evidence
	// (trust.go).
	trustPen  map[string]float64
	trustOn   map[domain.ServiceID]bool
	trustGate func(domain.ServiceID) bool
	trust     []PartyTrust
	// keyParty is each known key's party, so the lookup per candidate does
	// not parse a URL; an unknown key's is computed (partyOfKey).
	keyParty map[string]string
}

// partyPenalties returns the stale-share and trust penalties a key's party
// carries in a service. They are charged as the larger of the two, not the
// sum (partyPenalty): both rest on the same evidence where both apply.
func (v *chronicView) partyPenalties(svc domain.ServiceID, key string) (stale, trust float64) {
	if v == nil || (len(v.stalePen) == 0 && len(v.trustPen) == 0) {
		return 0, 0
	}
	party, ok := v.keyParty[key]
	if !ok {
		party = partyOfKey(key)
	}
	stale = v.stalePen[opID{svc: svc, op: party}]
	if pen, ok := v.trustPen[party]; ok {
		on, known := v.trustOn[svc]
		if !known && v.trustGate != nil {
			on = v.trustGate(svc)
		}
		if on {
			trust = pen
		}
	}
	return stale, trust
}

// partyPenalty is what a key's party costs it in a service.
func (v *chronicView) partyPenalty(svc domain.ServiceID, key string) float64 {
	stale, trust := v.partyPenalties(svc, key)
	return min(stale, trust)
}

// partyOfKey is the party a reputation key belongs to.
func partyOfKey(key string) string {
	return domain.PartyOfOperator(operatorOfKey(key))
}

// keyID addresses one reputation key without building a string.
type keyID struct {
	svc domain.ServiceID
	key string
}

// operatorOfKey is the operator a reputation key belongs to. Keys are
// "<identity>|<rpc_type>", and the identity is a URL at the default
// granularity and a supplier address or hostname at the others — a
// single-label identity is its own operator, which is what computeOperator
// already answers for a host it cannot reduce.
func operatorOfKey(key string) string {
	i := strings.LastIndexByte(key, '|')
	if i < 0 {
		return ""
	}
	identity := key[:i]
	if op := domain.OperatorOfURL(identity); op != "" {
		return op
	}
	return identity
}

// OperatorRate reports an operator's corrected failure rate in one pool, as of
// the last refresh. The auto-drain engine reads it to see an operator whose
// per-key rates are diluted below every threshold.
func (s *serviceImpl) OperatorRate(serviceID domain.ServiceID, rpcType domain.RPCType, operator string) (OperatorRateView, bool) {
	// Read the tracker rather than the 30s view: the auto-drain engine
	// evaluates on its own minute and there is no reason to hand it a stale
	// copy of something an atomic read away.
	if rpcType == domain.RPCTypeWebSocket {
		return OperatorRateView{}, false // not measured per operator; see RecordSignal
	}
	now := time.Now()
	st, ok := s.ops.get(opID{serviceID, operator, string(rpcType)}, now)
	if !ok {
		return OperatorRateView{}, false
	}
	rate := st.RateAt(now)
	if rate == 0 {
		return OperatorRateView{}, false
	}
	return OperatorRateView{Rate: rate, Attempts: uint64(st.Attempts)}, true
}

// SetOperatorChronic turns on the per-operator chronic rate, per service,
// behind gate. Call at wire time; the gate is read on each refresh.
func (s *serviceImpl) SetOperatorChronic(gate func(domain.ServiceID) bool) {
	s.operatorGate.Store(&gate)
}
