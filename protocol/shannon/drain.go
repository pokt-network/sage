package shannon

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
)

// SetDrains installs the operator-drain store that AvailableEndpoints
// consults. A Protocol built without calling SetDrains — including one built
// as a struct literal, which tests do — keeps drains at nil, and the
// AvailableEndpoints nil check skips the feature entirely rather than
// panicking.
//
// Not safe to call concurrently with relays; call it once at wire time, the
// same convention SetMetrics follows.
func (p *Protocol) SetDrains(store drain.Store) {
	p.drains = store
}

// operatorOfCacheMax bounds the memoization table, mirroring
// domain.operatorCacheMax and maxBlocklistCacheEntries: endpoint URLs come
// from on-chain sessions, so the live working set is hundreds to low
// thousands. The cap is a backstop against a pathological session, not an
// expected size.
const operatorOfCacheMax = 16384

// operatorOfCache memoizes URL -> operator (registrable domain, eTLD+1), the
// same shape as domainBlocklist's matchKey cache: the check runs once per
// candidate endpoint per selection, so recomputing the public-suffix
// derivation on every relay would be wasted work.
var (
	operatorOfCache    sync.Map // string (url) -> string (operator)
	operatorOfCacheLen atomic.Int64
)

// operatorOf returns the lowercased operator identity (registrable domain,
// eTLD+1) of rawURL's host, for comparison against a drain.Key.Operator.
//
// It reuses domain.EndpointAddr.Operator() for the actual eTLD+1 derivation
// rather than re-implementing public-suffix matching — the same trick
// domainBlocklist.computeMatchKey uses: rawURL is wrapped as a supplier-less
// EndpointAddr ("-" + rawURL) purely to borrow its host/eTLD+1 parsing.
func operatorOf(rawURL string) string {
	if v, ok := operatorOfCache.Load(rawURL); ok {
		return v.(string)
	}

	op := strings.ToLower(domain.EndpointAddr("-" + rawURL).Operator())

	// Bound the table. Clearing wholesale rather than evicting one entry keeps
	// this lock-free; see domain.EndpointAddr.Operator's identical rationale.
	if operatorOfCacheLen.Add(1) > operatorOfCacheMax {
		operatorOfCache.Clear()
		operatorOfCacheLen.Store(1)
	}
	operatorOfCache.Store(rawURL, op)
	return op
}
