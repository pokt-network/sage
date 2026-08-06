package domain

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/net/publicsuffix"
)

// EndpointAddr uniquely identifies an endpoint (format: "supplier-url").
type EndpointAddr string

// EndpointAddrList is an ordered list of endpoint addresses.
type EndpointAddrList []EndpointAddr

// Supplier extracts the supplier address from the endpoint addr.
// Format: "pokt1abc...-https://example.com"
func (e EndpointAddr) Supplier() string {
	s := string(e)
	idx := strings.Index(s, "-")
	if idx < 0 {
		return s
	}
	return s[:idx]
}

// URL extracts the URL from the endpoint addr.
func (e EndpointAddr) URL() (string, error) {
	s := string(e)
	idx := strings.Index(s, "-")
	if idx < 0 || idx+1 >= len(s) {
		return "", fmt.Errorf("invalid endpoint addr format: %s", s)
	}
	return s[idx+1:], nil
}

// Domain extracts the domain/host from the endpoint URL.
func (e EndpointAddr) Domain() string {
	url, err := e.URL()
	if err != nil {
		return ""
	}
	// Strip scheme
	if idx := strings.Index(url, "://"); idx >= 0 {
		url = url[idx+3:]
	}
	// Strip path
	if idx := strings.IndexByte(url, '/'); idx >= 0 {
		url = url[:idx]
	}
	// Strip port
	if idx := strings.LastIndexByte(url, ':'); idx >= 0 {
		url = url[:idx]
	}
	return url
}

// Operator returns the endpoint's operator identity: the registrable domain
// (eTLD+1) of its URL host. It is the unit at which infrastructure is actually
// shared — "who runs this box", as opposed to Domain(), which is "which
// hostname answers".
//
// The distinction is load-bearing wherever the goal is reaching *different*
// infrastructure. A provider fronting a service with rpc-1.example.net,
// rpc-2.example.net and rpc-3.example.net is three domains and one operator: a
// retry that merely avoids the failed hostname can land on the same rack, the
// same upstream, the same outage. Concentration limits have the same problem in
// reverse — capping per hostname caps nothing when one operator holds ten of
// them.
//
// Falls back to the full host when there is no registrable domain to extract
// (IP literals, single-label hosts like "localhost"). Those are their own
// operator, which is the correct answer for them.
// Memoized on the address rather than on the host, because endpoint selection
// calls this once per candidate per relay and the uncached path is real work:
// splitting the address, stripping scheme/path/port, then a public-suffix trie
// walk. Keying on the address collapses all of that to one lookup.
func (e EndpointAddr) Operator() string {
	if e == "" {
		return ""
	}
	if v, ok := operatorCache.Load(e); ok {
		return v.(string)
	}

	op := computeOperator(e.Domain())

	// Bound the table. Clearing wholesale rather than evicting one entry keeps
	// this lock-free; a rebuild costs one lookup per live endpoint and only
	// happens if the cardinality is already pathological.
	if operatorCacheLen.Add(1) > operatorCacheMax {
		operatorCache.Clear()
		operatorCacheLen.Store(1)
	}
	operatorCache.Store(e, op)
	return op
}

// operatorCacheMax bounds the memoization table. Addresses come from on-chain
// sessions, so the real cardinality is the endpoint count across live services
// — hundreds to low thousands. The cap is a backstop, not an expected working
// set.
const operatorCacheMax = 16384

var (
	operatorCache    sync.Map // EndpointAddr -> operator string
	operatorCacheLen atomic.Int64
)

// computeOperator maps a host to its registrable domain.
func computeOperator(host string) string {
	if host == "" {
		return ""
	}
	op, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil || op == "" {
		// No registrable domain (IP literal, single-label host, trailing dot).
		// The host is its own operator.
		return host
	}
	return op
}

// Operators returns the distinct operators present in the list, in first-seen
// order.
func (l EndpointAddrList) Operators() []string {
	if len(l) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(l))
	out := make([]string, 0, len(l))
	for _, a := range l {
		op := a.Operator()
		if _, ok := seen[op]; ok {
			continue
		}
		seen[op] = struct{}{}
		out = append(out, op)
	}
	return out
}

// ExcludeOperators returns a new list without any endpoint whose operator is in
// the given set. It never returns an empty list when the input was non-empty:
// if every candidate belongs to an excluded operator, the input is returned
// unchanged. Avoiding an operator is a preference — reach different
// infrastructure if you can — not a reason to have nowhere to send the request.
func (l EndpointAddrList) ExcludeOperators(operators map[string]bool) EndpointAddrList {
	if len(operators) == 0 || len(l) == 0 {
		return l
	}
	out := make(EndpointAddrList, 0, len(l))
	for _, a := range l {
		if !operators[a.Operator()] {
			out = append(out, a)
		}
	}
	if len(out) == 0 {
		return l
	}
	return out
}

// Contains returns true if the list contains the given addr.
func (l EndpointAddrList) Contains(addr EndpointAddr) bool {
	for _, a := range l {
		if a == addr {
			return true
		}
	}
	return false
}

// Exclude returns a new list without the given addrs.
func (l EndpointAddrList) Exclude(addrs map[EndpointAddr]bool) EndpointAddrList {
	if len(addrs) == 0 {
		return l
	}
	out := make(EndpointAddrList, 0, len(l))
	for _, a := range l {
		if !addrs[a] {
			out = append(out, a)
		}
	}
	return out
}
