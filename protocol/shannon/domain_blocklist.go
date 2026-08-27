package shannon

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// envBlockedDomains widens the blocklist at pod-restart speed, without a config
// rollout. Entries are comma-separated: "domain" bans every RPC type,
// "domain:type1|type2" bans only those. Example:
//
//	SAGE_BLOCKED_DOMAINS=op-alpha.example:websocket,evil.example
//
// The env entries are UNIONED with the blocked_domains config list. The env var
// can widen a ban and can never narrow one — an operator reaching for it is
// reacting to something, and the shape of that reaction is "also block this",
// never "actually, allow that again".
const envBlockedDomains = "SAGE_BLOCKED_DOMAINS"

// maxBlocklistCacheEntries bounds the decision cache. Endpoint URLs come from
// on-chain sessions, so the live set is hundreds to low thousands; the ceiling
// is a backstop against a pathological session, not an expected working set.
const maxBlocklistCacheEntries = 1 << 16

// domainBlocklist is the compiled form of config.BlockedDomain entries.
//
// A nil *domainBlocklist blocks nothing and every method is nil-safe, so the
// filter costs one nil check when the feature is unused — which is the common
// case, and this sits on the endpoint-selection path of every relay.
type domainBlocklist struct {
	// blocked maps a lowercased domain — a registrable domain
	// ("op-alpha.example") or an exact hostname ("s019.op-alpha.example") — to
	// the set of banned RPC types. A nil set bans every type.
	blocked map[string]map[domain.RPCType]struct{}

	// cache memoizes URL -> matched blocklist key ("" = no match). The blocklist
	// is immutable after startup, so an entry can never need invalidating.
	cache      sync.Map // string -> string
	cacheCount atomic.Int64
}

// ValidateBlockedDomains reports whether the configured entries compile.
//
// The blocklist itself is built inside the Shannon protocol, because that is
// where endpoints are handed out — which would mean a typo in blocked_domains
// went unnoticed under any other backend, and was only caught the first time
// the gateway ran for real. Wiring calls this whatever the protocol is, so a
// malformed ban fails the same way everywhere.
func ValidateBlockedDomains(entries []config.BlockedDomain) error {
	_, err := newDomainBlocklist(entries)
	return err
}

// SetBlockedDomains rebuilds the operator domain ban from entries and swaps it
// in atomically, unioning in the SAGE_BLOCKED_DOMAINS env entries exactly as
// New does (newDomainBlocklist does that unioning either way it is called).
//
// entries is validated before anything is swapped: an invalid entry (an
// empty domain, an unknown rpc_type) returns an error and leaves the
// previously installed list serving reads unchanged — a malformed reload
// input must not disable a working ban. Safe to call concurrently with
// AvailableEndpoints and SendRelay; a reader sees either the old list or the
// fully built new one, never a partially built one, because they only ever
// observe it through the atomic pointer.
func (p *Protocol) SetBlockedDomains(entries []config.BlockedDomain) error {
	next, err := newDomainBlocklist(entries)
	if err != nil {
		return err
	}
	p.blockedDomains.Store(next)
	return nil
}

// newDomainBlocklist compiles config entries into a matcher, unioning in
// anything named by envBlockedDomains. Returns (nil, nil) when nothing is
// blocked.
//
// An empty domain or an unknown RPC type is an error, which refuses the boot.
// The alternative is a blocklist that is narrower than what the operator wrote
// and says nothing about it — and a ban that silently covers less than it reads
// as covering is worse than no ban, because it is trusted.
func newDomainBlocklist(entries []config.BlockedDomain) (*domainBlocklist, error) {
	entries = append(append([]config.BlockedDomain(nil), entries...),
		parseBlockedDomainsEnv(os.Getenv(envBlockedDomains))...)
	if len(entries) == 0 {
		return nil, nil
	}

	blocked := make(map[string]map[domain.RPCType]struct{}, len(entries))
	for _, e := range entries {
		host := strings.ToLower(strings.TrimSpace(e.Domain))
		if host == "" {
			return nil, fmt.Errorf("blocked_domains: entry with an empty domain")
		}

		existing, seen := blocked[host]

		// No rpc_types means every type, and absorbs any narrower entry for the
		// same domain.
		if len(e.RPCTypes) == 0 {
			blocked[host] = nil
			continue
		}
		// Already banned for everything: a narrower entry cannot un-ban.
		if seen && existing == nil {
			continue
		}

		set := existing
		if set == nil {
			set = make(map[domain.RPCType]struct{}, len(e.RPCTypes))
		}
		for _, t := range e.RPCTypes {
			rpcType, err := parseRPCType(t)
			if err != nil {
				return nil, fmt.Errorf("blocked_domains: domain %q: %w", host, err)
			}
			set[rpcType] = struct{}{}
		}
		blocked[host] = set
	}

	return &domainBlocklist{blocked: blocked}, nil
}

// parseRPCType maps a config string to an RPC type. Unknown is not accepted: it
// is what SAGE calls "we could not tell", not something an operator can ban.
func parseRPCType(s string) (domain.RPCType, error) {
	candidate := domain.RPCType(strings.ToLower(strings.TrimSpace(s)))
	for _, t := range domain.AllRPCTypes() {
		if candidate == t {
			return t, nil
		}
	}
	return "", fmt.Errorf("unknown rpc_type %q (want one of %v)", s, domain.AllRPCTypes())
}

// parseBlockedDomainsEnv parses an envBlockedDomains value into config entries.
// A malformed piece is not dropped here — an empty domain surfaces as an error
// from newDomainBlocklist, so a typo fails the boot rather than the ban.
func parseBlockedDomainsEnv(raw string) []config.BlockedDomain {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	var entries []config.BlockedDomain
	for _, piece := range strings.Split(raw, ",") {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}
		host, typesStr, hasTypes := strings.Cut(piece, ":")
		entry := config.BlockedDomain{Domain: strings.TrimSpace(host)}
		if hasTypes {
			for _, t := range strings.Split(typesStr, "|") {
				if t = strings.TrimSpace(t); t != "" {
					entry.RPCTypes = append(entry.RPCTypes, t)
				}
			}
		}
		entries = append(entries, entry)
	}
	return entries
}

// IsBlocked reports whether an endpoint at rawURL is banned from serving
// rpcType.
//
// Matching is on the URL rather than on an EndpointAddr or a supplier address,
// which is what makes the ban survive session rollover: supplier registrations
// rotate and one operator holds many, but the URL is the machine.
func (b *domainBlocklist) IsBlocked(rawURL string, rpcType domain.RPCType) bool {
	if b == nil || rawURL == "" {
		return false
	}

	key := b.matchKey(rawURL)
	if key == "" {
		return false
	}

	set := b.blocked[key]
	if set == nil {
		return true // every RPC type
	}
	_, banned := set[rpcType]
	return banned
}

// matchKey resolves rawURL to the blocklist key it matches ("" = none),
// memoized because this runs per endpoint per selection.
func (b *domainBlocklist) matchKey(rawURL string) string {
	if v, ok := b.cache.Load(rawURL); ok {
		return v.(string)
	}

	key := b.computeMatchKey(rawURL)

	if b.cacheCount.Load() < maxBlocklistCacheEntries {
		if _, loaded := b.cache.LoadOrStore(rawURL, key); !loaded {
			b.cacheCount.Add(1)
		}
	}
	return key
}

// computeMatchKey checks the exact hostname first, then the registrable domain,
// so a host-specific entry wins over an operator-wide one.
//
// The URL is reused as an EndpointAddr purely to borrow its host and eTLD+1
// parsing — Domain() and Operator() strip scheme, path and port, and an address
// with no supplier prefix is exactly a bare URL to them.
func (b *domainBlocklist) computeMatchKey(rawURL string) string {
	addr := domain.EndpointAddr("-" + rawURL)

	if host := strings.ToLower(addr.Domain()); host != "" {
		if _, ok := b.blocked[host]; ok {
			return host
		}
		if operator := strings.ToLower(addr.Operator()); operator != host {
			if _, ok := b.blocked[operator]; ok {
				return operator
			}
		}
	}
	return ""
}

// entries returns the compiled (domain, rpc_type) pairs for startup logging,
// sorted for stable output. An all-types ban reports rpc_type "all".
func (b *domainBlocklist) entries() [][2]string {
	if b == nil {
		return nil
	}
	var out [][2]string
	for host, set := range b.blocked {
		if set == nil {
			out = append(out, [2]string{host, "all"})
			continue
		}
		for rpcType := range set {
			out = append(out, [2]string{host, string(rpcType)})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}
