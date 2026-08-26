package middleware

import "github.com/pokt-network/sage/domain"

// filterEndpoints returns the endpoints keep accepts. It returns eps itself
// when nothing is removed — the common case, and free — and a fresh slice
// otherwise. It never writes into eps: relay.Context.Clone is shallow, so a
// hedge arm and its parent share one backing array, and an in-place compaction
// on one is a data race with the other's read.
func filterEndpoints(eps domain.EndpointAddrList, keep func(domain.EndpointAddr) bool) domain.EndpointAddrList {
	for i, ep := range eps {
		if keep(ep) {
			continue
		}
		// First removal: copy what survived so far, then continue filtering.
		out := make(domain.EndpointAddrList, 0, len(eps)-1)
		out = append(out, eps[:i]...)
		for _, rest := range eps[i+1:] {
			if keep(rest) {
				out = append(out, rest)
			}
		}
		return out
	}
	return eps
}
