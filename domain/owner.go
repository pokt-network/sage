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
// staked with several providers. Owner is used pairwise instead: two endpoints
// are affiliated when they share an operator OR an owner (see Affiliates).
//
// The owner is not in an EndpointAddr ("supplier-url"). The protocol layer,
// which reads it from each session's supplier list, records it here; readers
// ask by address or by URL. Both tables are bounded the way operatorCache is:
// cleared wholesale past the cap, and refilled by the next session read.

const ownerCacheMax = 16384

var (
	// ownerBySupplier maps a supplier operator address to its owner address.
	ownerBySupplier    sync.Map
	ownerBySupplierLen atomic.Int64

	// ownerByURL maps a staked URL to the one owner staking it, or to ""
	// when more than one owner stakes the same URL (a shared backend).
	ownerByURL    sync.Map
	ownerByURLLen atomic.Int64
)

// RecordOwner records that supplier is owned by owner and stakes urls. An
// empty owner is ignored.
func RecordOwner(supplier, owner string, urls ...string) {
	if supplier == "" || owner == "" {
		return
	}
	if _, loaded := ownerBySupplier.Swap(supplier, owner); !loaded && ownerBySupplierLen.Add(1) > ownerCacheMax {
		ownerBySupplier.Clear()
		ownerBySupplierLen.Store(0)
	}
	for _, url := range urls {
		if url == "" {
			continue
		}
		prev, loaded := ownerByURL.LoadOrStore(url, owner)
		switch {
		case !loaded:
			if ownerByURLLen.Add(1) > ownerCacheMax {
				ownerByURL.Clear()
				ownerByURLLen.Store(0)
			}
		case prev != owner:
			ownerByURL.Store(url, "") // shared by several owners: no single one
		}
	}
}

// Owner returns the on-chain owner of the endpoint's supplier, or "" when no
// session has named it yet.
func (e EndpointAddr) Owner() string {
	if v, ok := ownerBySupplier.Load(e.Supplier()); ok {
		return v.(string)
	}
	return ""
}

// OwnerOfURL returns the one owner staking url, or "" when none is known or
// several owners share it.
func OwnerOfURL(url string) string {
	if v, ok := ownerByURL.Load(url); ok {
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

// Add adds one endpoint's operator and owner.
func (a *Affiliates) Add(ep EndpointAddr) {
	if a.operators == nil {
		a.operators, a.owners = map[string]bool{}, map[string]bool{}
	}
	if op := ep.Operator(); op != "" {
		a.operators[op] = true
	}
	if owner := ep.Owner(); owner != "" {
		a.owners[owner] = true
	}
}

// Contains reports whether ep shares an operator or an owner with the set.
func (a Affiliates) Contains(ep EndpointAddr) bool {
	if a.operators[ep.Operator()] {
		return true
	}
	owner := ep.Owner()
	return owner != "" && a.owners[owner]
}

// ExcludeAffiliates returns the list without any endpoint affiliated with a.
// Like ExcludeOperators it never empties a non-empty list: when every
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
