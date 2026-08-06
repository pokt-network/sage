package reputation

import (
	"math/rand/v2"
	"sync"

	"github.com/pokt-network/sage/domain"
)

// Per-operator concentration cap.
//
// Selection picks uniformly at random inside the winning reputation tier, so an
// operator's share of a service's traffic is its share of that tier's
// endpoints. That is the right default — an endpoint is a supplier
// registration, and a registration is what the chain settles against, so
// weighting by registrations held is weighting by what a provider can actually
// serve. What it does not do is bound the blast radius: a provider holding 40
// of a service's 50 registrations receives 80% of its traffic, and when that
// provider degrades, so does the service.
//
// The cap bounds any single operator's share and water-fills the excess across
// the others. Two constraints keep it from creating a worse problem than it
// solves:
//
//   - The displacement ceiling. A provider handed far more traffic than its
//     registrations entitle it to cannot serve it — each registration carries
//     its own per-session allowance, and exceeding it produces 429s and a
//     retry, not a served relay. Receivers are therefore capped at a multiple
//     of their own entitlement, and excess that nobody can absorb stays with
//     the operator it was taken from. Moving it anyway would only convert
//     concentration into failure.
//
//   - Two-operator pools keep a looser cap. With m operators the tightest
//     feasible cap is 1/m, so at m=2 a 0.50 cap is exactly the infeasibility
//     boundary and could only ever force a 50/50 split regardless of how the
//     registrations actually fall. Those pools keep the looser value instead.
const (
	// DefaultMaxOperatorShare bounds any one operator's share of selections in
	// a pool of three or more operators.
	DefaultMaxOperatorShare = 0.50
	// DefaultTwoOperatorMaxShare is the cap for two-operator pools, where
	// DefaultMaxOperatorShare would be the infeasibility boundary.
	DefaultTwoOperatorMaxShare = 0.65
	// DefaultDisplacementCeiling is the multiple of its own entitlement an
	// operator may be displaced up to when absorbing another's excess.
	DefaultDisplacementCeiling = 3.0
)

// OperatorCapConfig tunes the per-operator concentration cap.
//
// Zero means "use the default" for every field; a negative value disables that
// mechanism. So the zero value is the shipped, production-shaped cap, and
// turning a piece of it off has to be spelled out.
type OperatorCapConfig struct {
	// MaxShare is the cap for pools of three or more operators.
	MaxShare float64
	// TwoOperatorMaxShare is the cap for two-operator pools.
	TwoOperatorMaxShare float64
	// DisplacementCeiling is the multiple of entitlement a receiving operator
	// may be pushed to. Negative disables the ceiling (receivers are then
	// bounded only by the cap itself).
	DisplacementCeiling float64
}

// resolve fills in defaults and reports whether the cap is enabled at all.
func (c OperatorCapConfig) resolve() (maxShare, twoOpShare, ceiling float64, enabled bool) {
	maxShare = c.MaxShare
	if maxShare == 0 {
		maxShare = DefaultMaxOperatorShare
	}
	if maxShare < 0 {
		return 0, 0, 0, false
	}
	twoOpShare = c.TwoOperatorMaxShare
	if twoOpShare == 0 {
		twoOpShare = DefaultTwoOperatorMaxShare
	}
	if twoOpShare < 0 {
		twoOpShare = maxShare
	}
	ceiling = c.DisplacementCeiling
	if ceiling == 0 {
		ceiling = DefaultDisplacementCeiling
	}
	if ceiling < 0 {
		ceiling = 0 // 0 means "no ceiling" internally
	}
	return maxShare, twoOpShare, ceiling, true
}

// opGroup is one operator's slice of a candidate list.
type opGroup struct {
	operator string
	count    int
	// pick is a uniformly-sampled member of this operator's endpoints,
	// reservoir-sampled while grouping so no second pass over the candidates is
	// needed once an operator is chosen.
	pick domain.EndpointAddr
	// entitlement is the operator's share of the candidates: what it would
	// receive with no cap.
	entitlement float64
	// weight is its share after the cap is applied.
	weight float64
}

// groupBuf is the scratch space one capped selection needs. Pooled because
// selection runs once per relay and the cap would otherwise allocate on the
// hot path.
type groupBuf struct {
	groups []opGroup
}

var groupBufPool = sync.Pool{New: func() any { return &groupBuf{} }}

// tierBufPool hands out the per-endpoint tier scratch the capped selection path
// needs. Pooled for the same reason as groupBuf: it exists only for the length
// of one selection, and selection runs once per relay.
var tierBufPool = sync.Pool{New: func() any { return new([]int8) }}

func getTierBuf(n int) *[]int8 {
	p := tierBufPool.Get().(*[]int8)
	if cap(*p) < n {
		*p = make([]int8, n)
	}
	*p = (*p)[:n]
	return p
}

func putTierBuf(p *[]int8) { tierBufPool.Put(p) }

// cappedPick chooses an operator with probability proportional to its capped
// weight and returns both the operator and a uniformly-sampled endpoint of
// that operator.
//
// ok is false when the cap cannot apply — a disabled config, an empty list, or
// a pool holding fewer than two operators — and the caller should fall back to
// its usual selection. Callers that only need an endpoint use pick; callers
// that select within the operator themselves (the WebSocket path spreads by
// connection load) use operator.
// keep, when non-nil, restricts the pool to the candidates at the indices it
// accepts — the relay path uses it to cap within the winning reputation tier
// rather than across the whole session. It takes an index rather than an
// address so the caller can answer from a classification it already computed,
// instead of scoring every endpoint a second time on the hot path.
func cappedPick(
	cfg OperatorCapConfig,
	candidates domain.EndpointAddrList,
	keep func(i int) bool,
) (operator string, pick domain.EndpointAddr, ok bool) {
	maxShare, twoOpShare, ceiling, enabled := cfg.resolve()
	if !enabled || len(candidates) < 2 {
		return "", "", false
	}

	buf := groupBufPool.Get().(*groupBuf)
	defer func() {
		buf.groups = buf.groups[:0]
		groupBufPool.Put(buf)
	}()

	// Group by operator, reservoir-sampling a representative per group. Linear
	// search over the groups is deliberate: a service pool holds tens of
	// endpoints across a handful of operators, and a linear scan over a slice
	// beats a map at that size without allocating.
	kept := 0
	for i, ep := range candidates {
		if keep != nil && !keep(i) {
			continue
		}
		kept++
		op := ep.Operator()
		idx := -1
		for i := range buf.groups {
			if buf.groups[i].operator == op {
				idx = i
				break
			}
		}
		if idx < 0 {
			buf.groups = append(buf.groups, opGroup{operator: op, count: 1, pick: ep})
			continue
		}
		g := &buf.groups[idx]
		g.count++
		if rand.IntN(g.count) == 0 {
			g.pick = ep
		}
	}

	m := len(buf.groups)
	if m < 2 {
		// One operator holds the whole pool. There is nothing to redistribute
		// to; capping here would just mean serving nobody.
		return "", "", false
	}

	capShare := maxShare
	if m == 2 {
		capShare = twoOpShare
	}

	total := float64(kept)
	for i := range buf.groups {
		g := &buf.groups[i]
		g.entitlement = float64(g.count) / total
		g.weight = g.entitlement
	}

	waterFill(buf.groups, capShare, ceiling)

	g := pickByWeight(buf.groups)
	if g == nil {
		return "", "", false
	}
	return g.operator, g.pick, true
}

// waterFill redistributes any share above capShare onto the operators that have
// room for it, and returns whatever nobody can absorb to the operators it came
// from.
//
// One pass suffices: receivers are never pushed past capShare, so the
// redistribution cannot create a new violation. The only way a weight ends up
// above the cap afterwards is the deliberate return of unabsorbable excess.
func waterFill(groups []opGroup, capShare, ceiling float64) {
	const eps = 1e-9

	var excess, overEntitlement float64
	for i := range groups {
		if groups[i].weight > capShare+eps {
			excess += groups[i].weight - capShare
			groups[i].weight = capShare
			overEntitlement += groups[i].entitlement
		}
	}
	if excess <= eps {
		return
	}

	// Headroom is bounded by the cap and, if enabled, by the displacement
	// ceiling — how far past its own entitlement an operator may be pushed.
	limit := func(g opGroup) float64 {
		lim := capShare
		if ceiling > 0 {
			if c := ceiling * g.entitlement; c < lim {
				lim = c
			}
		}
		return lim
	}

	var headroom float64
	for i := range groups {
		if h := limit(groups[i]) - groups[i].weight; h > 0 {
			headroom += h
		}
	}

	give := excess
	if give > headroom {
		give = headroom
	}
	if give > eps {
		for i := range groups {
			if h := limit(groups[i]) - groups[i].weight; h > 0 {
				groups[i].weight += give * h / headroom
			}
		}
	}

	// Whatever is left has no home. Give it back pro rata to the operators it
	// was taken from rather than dropping it — a dropped share is a share of
	// selections that resolves to no endpoint at all.
	if leftover := excess - give; leftover > eps && overEntitlement > 0 {
		for i := range groups {
			if groups[i].weight >= capShare-eps && groups[i].entitlement > capShare {
				groups[i].weight += leftover * groups[i].entitlement / overEntitlement
			}
		}
	}
}

// pickByWeight chooses an operator with probability proportional to its weight.
func pickByWeight(groups []opGroup) *opGroup {
	var total float64
	for i := range groups {
		if groups[i].weight > 0 {
			total += groups[i].weight
		}
	}
	if total <= 0 {
		return nil
	}

	r := rand.Float64() * total
	var cum float64
	for i := range groups {
		if groups[i].weight <= 0 {
			continue
		}
		cum += groups[i].weight
		if r < cum {
			return &groups[i]
		}
	}
	// Floating-point rounding safety net.
	return &groups[len(groups)-1]
}
