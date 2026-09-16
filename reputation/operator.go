package reputation

import (
	"math"
	"strings"

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
// The fix is to measure the rate per operator, over every key it holds in the
// pool, each key corrected for its own warm-up before it is weighted in.

// opID is one operator's slice of a (service, RPC type) pool.
type opID struct {
	svc domain.ServiceID
	op  string
	rpc string
}

// OperatorRateView is what one operator's keys add up to in a pool.
type OperatorRateView struct {
	// Rate is the warm-up-corrected failure rate, attempt-weighted across the
	// operator's keys.
	Rate float64
	// Attempts is how much evidence that rate rests on.
	Attempts uint64
}

// minOpSamples is the evidence a single key needs before it joins its
// operator's rate. Below it the warm-up correction divides by a number small
// enough that one failure reads as a double-digit rate.
const minOpSamples = 50

// chronicView is everything the chronic term reads, rebuilt every
// baselineRefresh and swapped in whole. Reading it costs one atomic load and
// one map lookup, which is what the relay path can afford: penaltyFor runs per
// candidate endpoint per relay.
type chronicView struct {
	// byKey is the operator rate to charge a key, for services where the
	// operator term is on. Absent means charge the key's own rate.
	byKey map[keyID]float64
	// byOp is the same numbers addressed by operator, for readers outside
	// scoring (the auto-drain engine, the admin listing).
	byOp map[opID]OperatorRateView
	// baseline is the pool's best rate, in whichever basis the pool is
	// measured in. Absent means charge from zero.
	baseline map[poolID]float64
	// opOn records which services had the operator term on at refresh time, so
	// the flag is not read per relay.
	opOn map[domain.ServiceID]bool
}

// keyID addresses one reputation key without building a string.
type keyID struct {
	svc domain.ServiceID
	key string
}

// correctedRate removes an EWMA's warm-up bias: a rate that has only seen n
// attempts is a fraction 1-e^(-lambda*n) of the rate it is converging to.
// Dividing by that fraction reports what the key is actually doing, so a young
// key and an old one exhibiting the same behaviour are charged the same.
func correctedRate(lambda, rate float64, n uint64) float64 {
	if lambda <= 0 || rate <= 0 || n == 0 {
		return 0
	}
	warm := 1 - math.Exp(-lambda*float64(n))
	if warm <= 0 {
		return 0
	}
	return min(rate/warm, 1)
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
	v := s.chronic.Load()
	if v == nil {
		return OperatorRateView{}, false
	}
	r, ok := v.byOp[opID{serviceID, operator, string(rpcType)}]
	return r, ok
}

// SetOperatorChronic turns on the per-operator chronic rate, per service,
// behind gate. Call at wire time; the gate is read on each refresh.
func (s *serviceImpl) SetOperatorChronic(gate func(domain.ServiceID) bool) {
	s.operatorGate.Store(&gate)
}
