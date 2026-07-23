package middleware

import (
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/pokt-network/sage/relay"
)

const forwardedForHeader = "X-Forwarded-For"

// ClientIP resolves the address a request is attributed to and stores it on
// ctx.ClientIP, once, near the front of the chain so every later concern —
// rate limiting, supplier affinity, abuse controls — reads one agreed answer
// instead of each re-deriving it from the raw request and disagreeing.
//
// trustedProxies are the CIDRs of proxies that sit in front of the gateway.
// X-Forwarded-For is believed ONLY when the request's immediate peer is inside
// one of them; from any other peer the header is client-supplied and ignored.
// With none configured the attributed address is always the direct peer — the
// address that cannot be forged, and the right default for the zero value.
//
// Behind a proxy this is the difference between attributing traffic to each real
// client and attributing all of it to the proxy, and between a client being able
// to forge its identity with a header and not. Both matter the moment anything
// keys a decision on the client — which is the point of resolving it here rather
// than leaving each module to guess.
func ClientIP(trustedProxies []netip.Prefix) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if ctx.HTTPRequest != nil {
				ctx.ClientIP = resolveClientIP(
					ctx.HTTPRequest.RemoteAddr,
					ctx.HTTPRequest.Header.Values(forwardedForHeader),
					trustedProxies,
				)
			}
			return next.HandleRelay(ctx)
		})
	}
}

// resolveClientIP attributes a request to a client address.
//
// peer is the transport-level remote address (host:port, or a bare host).
// forwarded is every X-Forwarded-For header on the request; each is a comma list
// "client, proxy1, proxy2, …" that proxies append to left-to-right as the
// request passes through, so the rightmost entries are the hops nearest us.
//
// If the peer is not a trusted proxy, the forwarded headers are attacker-
// controlled and ignored — the peer is the client. If it is trusted, we walk the
// forwarded chain from the right, from us outward, and return the first address
// that is not itself a trusted proxy: whoever handed the request to our outermost
// trusted hop. Trusting the leftmost entry instead would trust whatever the real
// client chose to write there, which is the classic X-Forwarded-For spoof.
//
// Returns the zero Addr (invalid) only when the peer itself cannot be parsed.
func resolveClientIP(peer string, forwarded []string, trusted []netip.Prefix) netip.Addr {
	peerAddr := parseIP(peer)
	if !peerAddr.IsValid() {
		return netip.Addr{}
	}
	if !ipInAny(peerAddr, trusted) {
		return peerAddr
	}

	for i := len(forwarded) - 1; i >= 0; i-- {
		parts := strings.Split(forwarded[i], ",")
		for j := len(parts) - 1; j >= 0; j-- {
			addr := parseIP(strings.TrimSpace(parts[j]))
			if addr.IsValid() && !ipInAny(addr, trusted) {
				return addr
			}
		}
	}

	// Peer is trusted but the chain named no untrusted client (absent, all
	// trusted, or all unparseable). The nearest real address we have is the peer.
	return peerAddr
}

// parseIP accepts a bare IP or a host:port and returns the address, IPv4-mapped
// IPv6 normalised to IPv4 so keys and CIDR comparisons are stable.
func parseIP(s string) netip.Addr {
	if s == "" {
		return netip.Addr{}
	}
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.Unmap()
	}
	if host, _, err := net.SplitHostPort(s); err == nil {
		if addr, err := netip.ParseAddr(host); err == nil {
			return addr.Unmap()
		}
	}
	return netip.Addr{}
}

func ipInAny(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// ParseTrustedProxies parses CIDR strings into prefixes, failing on the first
// bad one so a typo in trusted_proxies is a startup error rather than a proxy
// that is silently never trusted — which would look like working spoof
// protection while quietly attributing every proxied client to the proxy.
func ParseTrustedProxies(cidrs []string) ([]netip.Prefix, error) {
	if len(cidrs) == 0 {
		return nil, nil
	}
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		p, err := netip.ParsePrefix(strings.TrimSpace(c))
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", c, err)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}
