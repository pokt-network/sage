package middleware

import (
	"net/url"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// MethodBlockRecorder is told about method-block events. metrics.Recorder
// satisfies it; nil disables recording.
type MethodBlockRecorder interface {
	RecordMethodBlockEvent(serviceID domain.ServiceID, method, event string)
}

// Method-block event names, a closed set used as a metric label.
const (
	MethodBlockEventMark     = "mark"
	MethodBlockEventEscalate = "escalate"
	MethodBlockEventBypass   = "bypass"
	// MethodBlockEventFamily: a -32601 on a catalogued method marked the
	// host for the whole family the plugin named (qos.MethodFamilyLister).
	MethodBlockEventFamily = "family"
)

// MethodBlocks returns a middleware that keeps a method away from a host that
// could not answer it recently, while the host keeps receiving everything
// else.
//
//  1. Pre-relay: removes from ctx.Endpoints every host the store blocks for
//     this request's method. Bypass fires when every host is blocked, or when
//     the filter removed something and no surviving host is vouched for by
//     reputation (a recorded score at or above the probation threshold) — a
//     block must never divert a method onto hosts reputation hasn't actually
//     measured, including a host that is merely unscored (e.g. right after
//     boot, before the first health check). It also fires when the filter
//     keeps fewer than half of the pool's vouched endpoints (keepsVouchedCapacity).
//     On bypass the relay is marked degraded and the unfiltered list is used.
//  2. Post-relay: if the attempt's verdict is MethodBlocking (a timeout after
//     connect, or the endpoint saying it does not serve the method), marks
//     the attempt's host for that method. The mark counts toward a host-wide
//     escalation only when the verdict blames the supplier; a client-caused
//     one (-32601 for a method the node never claimed) blocks that method
//     alone.
//
// It sits inside Retry and Hedge so every arm and every attempt both honours
// and feeds the store — the losing hedge arm's timeout is the case that
// matters, and nothing outside Hedge ever sees a loser.
//
// The method is the plugin's normalised name (qos.MethodNormalizer): a
// bounded catalogue, never the client's string. A service whose plugin has
// no normaliser, or a payload with no method, passes through untouched.
func MethodBlocks(
	store *methodblock.Store,
	registry *qos.Registry,
	endpointProvider protocol.EndpointProvider,
	flags featureflag.FlagStore,
	repSvc reputation.Service,
	events MethodBlockRecorder,
) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if store == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagMethodBlocks, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}
			method := normalizedMethod(registry, ctx)
			// MethodOther is every uncatalogued method at once: a mark on it
			// is a mark on all of them, and one client sending a bogus
			// method name to each host would divert every legitimate
			// uncatalogued method for every client for a TTL. The bucket
			// bounds keys; it carries no memory.
			if method == "" || method == qos.MethodOther {
				return next.HandleRelay(ctx)
			}
			serviceID := string(ctx.ServiceID)

			// Populate ctx.Endpoints if nothing upstream did. circuit_break
			// fetches too, but only with its own flag on and only when it is
			// in the chain; a filter that applied only to a list someone else
			// happened to fetch would silently stop applying the moment an
			// admin flipped circuit_breaker off. SelectEndpoint skips its own
			// fetch when the list is already populated.
			if len(ctx.Endpoints) == 0 && endpointProvider != nil {
				eps, err := endpointProvider.AvailableEndpoints(ctx.Ctx, ctx.ServiceID, ctx.RPCType)
				if err == nil {
					ctx.Endpoints = eps
				}
				// On error, leave the list empty — SelectEndpoint retries the
				// fetch and surfaces the error.
			}

			open := func(ep domain.EndpointAddr) bool {
				return !store.Blocked(serviceID, blockHost(endpointProvider, ep, ctx.RPCType), method)
			}
			pool := ctx.Endpoints
			if len(ctx.Endpoints) > 0 {
				filtered := filterEndpoints(ctx.Endpoints, open)
				bypass := len(filtered) == 0 ||
					(len(filtered) < len(ctx.Endpoints) &&
						(!anyVouched(repSvc, ctx, filtered) || !keepsVouchedCapacity(repSvc, ctx, filtered, ctx.Endpoints)))
				if bypass {
					ctx.Degraded = true
					if events != nil {
						events.RecordMethodBlockEvent(ctx.ServiceID, method, MethodBlockEventBypass)
					}
				} else {
					ctx.Endpoints = filtered
				}
			}

			err := next.HandleRelay(ctx)

			if ctx.Endpoint != "" && ctx.HeuristicResult != nil && ctx.HeuristicResult.MethodBlocking {
				// Only supplier-attributed evidence may escalate to a
				// host-wide block. -32601 is MethodBlocking and AttrClient:
				// a healthy node without debug_*/trace_* answers it to as
				// many methods as a client asks for, and counting those
				// would remove the node from everything.
				escalates := ctx.HeuristicResult.Attribution == heuristic.AttrSupplier
				event := MethodBlockEventMark
				host := blockHost(endpointProvider, ctx.Endpoint, ctx.RPCType)
				if store.Mark(serviceID, host, method, escalates) {
					event = MethodBlockEventEscalate
				}
				if events != nil {
					events.RecordMethodBlockEvent(ctx.ServiceID, method, event)
				}
				// A host that does not serve this method may not serve its
				// family either; the plugin says which methods those are. The
				// family marks are client-attributed like the one they came
				// from, so they never add up to a host-wide block.
				if ctx.HeuristicResult.Reason == heuristic.ReasonMethodNotFound {
					if family := methodFamily(registry, ctx, method); len(family) > 0 {
						for _, m := range family {
							if m != method {
								store.Mark(serviceID, host, m, false)
							}
						}
						if events != nil {
							events.RecordMethodBlockEvent(ctx.ServiceID, method, MethodBlockEventFamily)
						}
					}
					// The heuristic promotes a -32601 on a catalogued method to
					// a retry so another host can serve it. When the mark just
					// set leaves no host in the pool open for the method, a
					// retry can only reach one that already said no: on beta,
					// 32 registrations on one host made one eth_blockNumber
					// three paid relays for the same answer. Deliver this one.
					if ctx.HeuristicResult.Reason == heuristic.ReasonMethodNotFound && ctx.HeuristicResult.ShouldRetry &&
						len(filterEndpoints(pool, open)) == 0 {
						ctx.HeuristicResult.ShouldRetry = false
						ctx.Err = nil
						err = nil
					}
				}
			}
			return err
		})
	}
}

// anyVouched reports whether at least one endpoint in eps is vouched for by
// reputation (a recorded score at or above the probation threshold) for this
// request's RPC type. A nil repSvc means "all vouched" — a deployment that
// hasn't wired reputation must not have this guard silently degrade every
// filtered relay.
func anyVouched(repSvc reputation.Service, ctx *relay.Context, eps domain.EndpointAddrList) bool {
	if repSvc == nil {
		return true
	}
	for _, ep := range eps {
		if repSvc.Vouched(ctx.Ctx, ctx.ServiceID, ep, ctx.RPCType) {
			return true
		}
	}
	return false
}

// keepsVouchedCapacity reports whether narrowing full to narrowed keeps
// enough of full's vouched endpoints to carry the method: all of them when
// there are one or two, otherwise at least half.
//
// One vouched survivor is not enough. Method marks come from timeouts, and
// timeouts come from load: on mainnet sei (2026-09-14) marks on three hosts
// piled eth_call and eth_getLogs onto the one vouched host left per pod, it
// timed out and fell below probation, and with nothing vouched the whole
// service went to the pool-collapse fallback (12 → 294 per 9 min). A block
// that sheds more than half the healthy capacity feeds itself; bypassing it
// costs a slow answer from the blocked host instead.
func keepsVouchedCapacity(repSvc reputation.Service, ctx *relay.Context, narrowed, full domain.EndpointAddrList) bool {
	if repSvc == nil {
		return true
	}
	have := countVouched(repSvc, ctx, full)
	return countVouched(repSvc, ctx, narrowed) >= min(have, max(2, (have+1)/2))
}

// countVouched counts the endpoints in eps reputation vouches for.
func countVouched(repSvc reputation.Service, ctx *relay.Context, eps domain.EndpointAddrList) int {
	n := 0
	for _, ep := range eps {
		if repSvc.Vouched(ctx.Ctx, ctx.ServiceID, ep, ctx.RPCType) {
			n++
		}
	}
	return n
}

// normalizedMethod asks the service's plugin to name the request's method.
// "" means "nothing to key on" for any reason: no plugin, no normaliser, no
// payload, or a payload without a method notion.
func normalizedMethod(registry *qos.Registry, ctx *relay.Context) string {
	if registry == nil || len(ctx.Payloads) == 0 {
		return ""
	}
	plugin := ctx.Plugin
	if plugin == nil {
		plugin = registry.Get(ctx.ServiceID)
	}
	normalizer, ok := plugin.(qos.MethodNormalizer)
	if !ok {
		return ""
	}
	return normalizer.NormalizeMethod(ctx.Payloads[0])
}

// methodFamily asks the service's plugin which catalogued methods a host
// that refused method will refuse too; nil when the plugin cannot say.
func methodFamily(registry *qos.Registry, ctx *relay.Context, method string) []string {
	plugin := ctx.Plugin
	if plugin == nil && registry != nil {
		plugin = registry.Get(ctx.ServiceID)
	}
	lister, ok := plugin.(qos.MethodFamilyLister)
	if !ok {
		return nil
	}
	return lister.MethodFamily(method)
}

// blockHost is the host a mark is kept against: the host the face is
// actually dialed from when the provider can say (protocol.URLResolver), else
// the address's own. An operator staking one host per type would otherwise
// have a REST refusal marked against its JSON-RPC host.
func blockHost(provider protocol.EndpointProvider, ep domain.EndpointAddr, rpcType domain.RPCType) string {
	if r, ok := provider.(protocol.URLResolver); ok {
		if rawURL, ok := r.EndpointURLFor(ep, rpcType); ok {
			if u, err := url.Parse(rawURL); err == nil && u.Hostname() != "" {
				return u.Hostname()
			}
		}
	}
	return ep.Domain()
}
