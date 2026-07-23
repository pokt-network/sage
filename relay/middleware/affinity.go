package middleware

import (
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
)

// affinityEntry stores an endpoint address and its expiry time.
type affinityEntry struct {
	endpoint domain.EndpointAddr
	expiry   time.Time
}

// SupplierAffinity returns a middleware that pins a client to a preferred
// supplier endpoint for the lifetime of the TTL, so that stateful operations
// (e.g. transactions submitted and then queried) are more likely to hit the
// same backend.
//
// Before relay: if the client has a valid, non-expired affinity entry and that
// endpoint is still in ctx.Endpoints, it is moved to the front of the list so
// that upstream endpoint-selection logic prefers it.
//
// After relay: if the request contained a "write" method (send/submit/broadcast),
// the chosen endpoint is recorded as the client's affinity.
//
// If the "supplier_affinity" feature flag is disabled, the middleware passes
// through to the inner handler without wrapping.
func SupplierAffinity(flags featureflag.FlagStore, ttl time.Duration) relay.Middleware {
	var store sync.Map // clientKey (string) → affinityEntry

	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if !flags.IsEnabled(ctx.Ctx, featureflag.FlagSupplierAffinity, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			clientKey := remoteIP(ctx)
			if clientKey != "" {
				// Pre-relay: prioritize affinity endpoint if available.
				if raw, ok := store.Load(clientKey); ok {
					entry := raw.(affinityEntry)
					if time.Now().Before(entry.expiry) {
						ctx.Endpoints = prioritize(ctx.Endpoints, entry.endpoint)
					} else {
						store.Delete(clientKey)
					}
				}
			}

			err := next.HandleRelay(ctx)

			// Post-relay: record affinity on successful write operations.
			if err == nil && clientKey != "" && ctx.Endpoint != "" && isWriteMethod(ctx) {
				store.Store(clientKey, affinityEntry{
					endpoint: ctx.Endpoint,
					expiry:   time.Now().Add(ttl),
				})
			}

			return err
		})
	}
}

// remoteIP returns the client IP used to key supplier affinity.
//
// It prefers ctx.ClientIP, the address the ClientIP middleware resolved once
// (honouring trusted proxies). Behind a proxy the raw RemoteAddr is the proxy,
// so keying affinity on it would make every client share one sticky supplier —
// exactly what affinity is meant to avoid. Falls back to RemoteAddr when the
// client_ip middleware is not in the chain (ctx.ClientIP invalid).
func remoteIP(ctx *relay.Context) string {
	if ctx.ClientIP.IsValid() {
		return ctx.ClientIP.String()
	}
	if ctx.HTTPRequest == nil {
		return ""
	}
	addr := ctx.HTTPRequest.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// prioritize returns a copy of list with preferred moved to the front.
// If preferred is not in the list, the original order is preserved.
func prioritize(list domain.EndpointAddrList, preferred domain.EndpointAddr) domain.EndpointAddrList {
	idx := -1
	for i, ep := range list {
		if ep == preferred {
			idx = i
			break
		}
	}
	if idx <= 0 {
		// Not found, or already at front — no change needed.
		return list
	}
	out := make(domain.EndpointAddrList, 0, len(list))
	out = append(out, preferred)
	out = append(out, list[:idx]...)
	out = append(out, list[idx+1:]...)
	return out
}

// isWriteMethod returns true if the first payload's method looks like a
// state-changing call (send, submit, broadcast).
func isWriteMethod(ctx *relay.Context) bool {
	if len(ctx.Payloads) == 0 {
		return false
	}
	method := strings.ToLower(ctx.Payloads[0].Method())
	return strings.Contains(method, "send") ||
		strings.Contains(method, "submit") ||
		strings.Contains(method, "broadcast")
}
