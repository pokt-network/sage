package reputation

import (
	"github.com/pokt-network/sage/domain"
)

// Key granularity: what a reputation score is actually attached to.
//
// An EndpointAddr is a (supplier, backend URL) pair, and the two halves fail
// for different reasons. A supplier is a staked registration — a ticket, not a
// machine — and one physical backend routinely sits behind several of them.
// Scoring per pair therefore makes every supplier on a shared backend prove
// independently that the backend is broken: the pool learns the same fact N
// times, at the cost of N times the failed relays, and a backend fronted by ten
// registrations takes ten times as long to be routed away from as one fronted
// by a single registration.
//
// Coarser is not automatically better. Per-domain and per-supplier both blend
// backends that fail independently — one bad host drags down its healthy
// siblings under the same domain, and one bad backend drags down every other
// backend the same supplier registered. Per-URL is the granularity at which the
// thing being scored is a single machine, so it is the default.
const (
	// KeyPerURL scores the backend URL. Suppliers fronting the exact same URL
	// share one score; distinct URLs stay separate. Default.
	KeyPerURL = "per-url"
	// KeyPerEndpoint scores each (supplier, URL) pair independently.
	KeyPerEndpoint = "per-endpoint"
	// KeyPerDomain scores every endpoint on a hostname together.
	KeyPerDomain = "per-domain"
	// KeyPerSupplier scores every URL a supplier registered together.
	KeyPerSupplier = "per-supplier"
)

// KeyFn maps an endpoint address and the protocol it was reached over to the
// identity its score is stored under.
//
// The RPC type is always part of the key, at every granularity. A Shannon
// supplier stakes one service for several RPC types at once, and the relay
// miner behind a single staked URL routes each type to a different backend —
// so a broken WebSocket backend says nothing about that supplier's REST
// backend. Blending them means one dead transport ejects the endpoint from
// traffic it was serving correctly, and the coarser the granularity the wider
// that blast radius: at per-URL, one key would otherwise cover every transport
// of every supplier fronting the URL.
type KeyFn func(domain.EndpointAddr, domain.RPCType) string

// keyFnFor returns the KeyFn for a granularity name. An empty or unrecognized
// name yields the default (per-URL) — a misspelling in config must not silently
// change how scores are grouped, so callers should validate the name separately
// (see ValidKeyGranularity) rather than relying on this fallback.
func keyFnFor(granularity string) KeyFn {
	var base func(domain.EndpointAddr) string
	switch granularity {
	case KeyPerEndpoint:
		base = func(ep domain.EndpointAddr) string { return string(ep) }
	case KeyPerDomain:
		base = func(ep domain.EndpointAddr) string { return fallbackToAddr(ep, ep.Domain()) }
	case KeyPerSupplier:
		base = func(ep domain.EndpointAddr) string { return fallbackToAddr(ep, ep.Supplier()) }
	default: // KeyPerURL
		base = func(ep domain.EndpointAddr) string {
			url, err := ep.URL()
			if err != nil {
				return string(ep)
			}
			return fallbackToAddr(ep, url)
		}
	}
	return func(ep domain.EndpointAddr, rpcType domain.RPCType) string {
		return base(ep) + "|" + string(rpcType)
	}
}

// fallbackToAddr degrades a malformed address to per-endpoint granularity for
// that one address rather than collapsing every malformed address onto the
// empty key, which would score them as if they were the same host.
func fallbackToAddr(ep domain.EndpointAddr, extracted string) string {
	if extracted == "" {
		return string(ep)
	}
	return extracted
}

// ValidKeyGranularity reports whether name is a granularity SAGE implements.
func ValidKeyGranularity(name string) bool {
	switch name {
	case "", KeyPerURL, KeyPerEndpoint, KeyPerDomain, KeyPerSupplier:
		return true
	default:
		return false
	}
}
