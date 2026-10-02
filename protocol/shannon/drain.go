package shannon

import (
	"strings"

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

// operatorOf returns the lowercased operator identity (registrable domain,
// eTLD+1) of rawURL's host, for comparison against a drain.Key.Operator.
//
// It reuses domain.EndpointAddr.Operator() for the eTLD+1 derivation and its
// memoization rather than re-implementing either — the same trick
// domainBlocklist.computeMatchKey uses: rawURL is wrapped as a supplier-less
// EndpointAddr ("-" + rawURL) purely to borrow its host/eTLD+1 parsing.
func operatorOf(rawURL string) string {
	return strings.ToLower(domain.EndpointAddr("-" + rawURL).Operator())
}
