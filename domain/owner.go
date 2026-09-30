package domain

import (
	"sync"
	"sync/atomic"
)

// Owners: the identity an operator cannot rotate.
//
// An operator (eTLD+1) is infrastructure — who runs the box — and it is what
// failure is measured against. It is not who is paid. A supplier's on-chain
// owner is: the stake, which moves only by unstaking. One owner can run
// several brands (on mainnet, 2026-09-26, one owner ran 218 suppliers under two
// dedicated domains), and one operator hosts many owners (one provider hosted
// 86). So neither identity subsumes the other, and grouping by both
// transitively would chain most of the network into one group through owners
// staked with several providers.
//
// So an owner links two operators only where it IS the operator: a domain
// counts as dedicated to an owner when that owner holds at least
// dedicatedShare of the registrations seen on it (and at least
// dedicatedMinSeen of them). Two endpoints are affiliated when they share an
// operator, or when both sit on domains dedicated to the same owner - one
// owner's own brands. A provider hosting many owners is never dedicated, so an
// owner staked with two such providers keeps them independent: they are two
// infrastructures, and a retry, a hedge or a vote that treated them as one
// would lose real diversity (on mainnet, 2026-09-26, 12 owners were staked
// across two independent providers; one owner ran two brands of its own).
//
// The owner is not in an EndpointAddr ("supplier-url"). The protocol layer,
// which reads it from each session's supplier list, records it here, and
// readers ask by address. The table is bounded the way operatorCache is:
// cleared wholesale past the cap, and refilled by the next session read.
//
// Owner is identity, not a scoring key: bans, attribution and "is this a
// different provider". Failure is measured per operator, which catches a new
// brand within its first few hundred attempts on its own.

const ownerCacheMax = 16384

// A domain is dedicated to an owner holding at least this share of the
// registrations seen on it, once at least dedicatedMinSeen have been seen.
const (
	dedicatedShare   = 0.9
	dedicatedMinSeen = 5
)

var (
	// ownerBySupplier maps a supplier operator address to its owner address.
	ownerBySupplier    sync.Map
	ownerBySupplierLen atomic.Int64

	// tenancy counts, per operator, the owner of every supplier seen staking
	// on it; dedicatedTo is the result read on the relay path, one map load.
	tenancyMu   sync.Mutex
	tenancy     = map[string]map[string]string{} // operator -> supplier -> owner
	tenancyLen  int
	dedicatedTo sync.Map // operator -> owner, only for dedicated domains
)

// RecordOwner records that supplier, staking on operator, is owned by owner.
// An empty supplier or owner is ignored; an empty operator records the owner
// without counting toward any domain's tenancy.
func RecordOwner(supplier, owner, operator string) {
	if supplier == "" || owner == "" {
		return
	}
	if _, loaded := ownerBySupplier.Swap(supplier, owner); !loaded && ownerBySupplierLen.Add(1) > ownerCacheMax {
		ownerBySupplier.Clear()
		ownerBySupplierLen.Store(0)
	}
	if operator == "" {
		return
	}
	tenancyMu.Lock()
	defer tenancyMu.Unlock()
	sups := tenancy[operator]
	if sups == nil {
		sups = map[string]string{}
		tenancy[operator] = sups
	}
	if prev, seen := sups[supplier]; seen && prev == owner {
		return
	} else if !seen {
		if tenancyLen++; tenancyLen > ownerCacheMax {
			clear(tenancy)
			dedicatedTo.Clear()
			tenancyLen = 1
			sups = map[string]string{}
			tenancy[operator] = sups
		}
	}
	sups[supplier] = owner
	refreshDedicated(operator, sups)
}

// refreshDedicated recomputes whether operator is dedicated to one owner.
// Caller holds tenancyMu. Runs per session read, not per relay.
func refreshDedicated(operator string, sups map[string]string) {
	counts := map[string]int{}
	for _, owner := range sups {
		counts[owner]++
	}
	for owner, n := range counts {
		if len(sups) >= dedicatedMinSeen && float64(n) >= dedicatedShare*float64(len(sups)) {
			dedicatedTo.Store(operator, owner)
			return
		}
	}
	dedicatedTo.Delete(operator)
}

// dedicatedOwner is the owner operator is dedicated to, or "".
func dedicatedOwner(operator string) string {
	if v, ok := dedicatedTo.Load(operator); ok {
		return v.(string)
	}
	return ""
}

// Owner returns the on-chain owner of the endpoint's supplier, or "" when no
// session has named it yet.
func (e EndpointAddr) Owner() string {
	if v, ok := ownerBySupplier.Load(e.Supplier()); ok {
		return v.(string)
	}
	return ""
}

// Affiliates is a set of endpoints' operators and owners: what "the same
// provider" means when a retry, a hedge or a rebind wants a different one.
// Membership is pairwise, never transitive — see the comment at the top of
// this file.
type Affiliates struct {
	operators map[string]bool
	owners    map[string]bool
}

// AffiliatesOf returns the affiliates of the given endpoints.
func AffiliatesOf(eps ...EndpointAddr) Affiliates {
	a := Affiliates{operators: make(map[string]bool, len(eps)), owners: make(map[string]bool, len(eps))}
	for _, ep := range eps {
		a.Add(ep)
	}
	return a
}

// Party is who stands behind an endpoint when counting independent sources:
// its owner when the endpoint's domain is dedicated to that owner (two brands
// of one owner are one party, as for Affiliates), otherwise its operator.
// Empty when the address has no operator.
func (e EndpointAddr) Party() string {
	op := e.Operator()
	if owner := dedicatedOwner(op); owner != "" && owner == e.Owner() {
		return owner
	}
	return op
}

// PartyOfOperator is Party for an endpoint known only by its operator, as a
// reputation key is: the owner the operator's domain is dedicated to, else the
// operator. It differs from Party for an endpoint on a dedicated domain whose
// supplier is not the dedicated owner's: a minority tenant, or one whose owner
// is not named yet. Party says the operator; this says the owner, so such a
// key is charged what the owner is charged. It runs on the same domain, which
// is the operator's infrastructure, so that is accepted.
func PartyOfOperator(operator string) string {
	if owner := dedicatedOwner(operator); owner != "" {
		return owner
	}
	return operator
}

// Add adds one endpoint's operator, and its owner when the endpoint's domain
// is dedicated to that owner.
func (a *Affiliates) Add(ep EndpointAddr) {
	if a.operators == nil {
		a.operators, a.owners = map[string]bool{}, map[string]bool{}
	}
	op := ep.Operator()
	if op != "" {
		a.operators[op] = true
	}
	if owner := dedicatedOwner(op); owner != "" && owner == ep.Owner() {
		a.owners[owner] = true
	}
}

// Contains reports whether ep shares an operator with the set, or sits on a
// domain dedicated to an owner whose dedicated domain is already in the set.
func (a Affiliates) Contains(ep EndpointAddr) bool {
	op := ep.Operator()
	if a.operators[op] {
		return true
	}
	owner := dedicatedOwner(op)
	return owner != "" && a.owners[owner] && owner == ep.Owner()
}

// ExcludeAffiliates returns the list without any endpoint affiliated with a.
// It never empties a non-empty list: when every
// candidate is affiliated, the input is returned unchanged. Reaching an
// independent provider is a preference, not a reason to have nowhere to send.
func (l EndpointAddrList) ExcludeAffiliates(a Affiliates) EndpointAddrList {
	if len(l) == 0 || (len(a.operators) == 0 && len(a.owners) == 0) {
		return l
	}
	out := make(EndpointAddrList, 0, len(l))
	for _, ep := range l {
		if !a.Contains(ep) {
			out = append(out, ep)
		}
	}
	if len(out) == 0 {
		return l
	}
	return out
}
