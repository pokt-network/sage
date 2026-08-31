package middleware

import (
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
//     boot, before the first health check). On bypass the relay is marked
//     degraded and the unfiltered list is used.
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

			if len(ctx.Endpoints) > 0 {
				filtered := filterEndpoints(ctx.Endpoints, func(ep domain.EndpointAddr) bool {
					return !store.Blocked(serviceID, ep.Domain(), method)
				})
				bypass := len(filtered) == 0 ||
					(len(filtered) < len(ctx.Endpoints) && !anyVouched(repSvc, ctx, filtered))
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
				if store.Mark(serviceID, ctx.Endpoint.Domain(), method, escalates) {
					event = MethodBlockEventEscalate
				}
				if events != nil {
					events.RecordMethodBlockEvent(ctx.ServiceID, method, event)
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
