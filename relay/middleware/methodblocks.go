package middleware

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/methodblock"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
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
//     this request's method. If that leaves nothing, the relay is marked
//     degraded and the unfiltered list is used — a block must never be able
//     to empty a pool.
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
	flags featureflag.FlagStore,
	events MethodBlockRecorder,
) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if store == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagMethodBlocks, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}
			method := normalizedMethod(registry, ctx)
			if method == "" {
				return next.HandleRelay(ctx)
			}
			serviceID := string(ctx.ServiceID)

			if len(ctx.Endpoints) > 0 {
				filtered := filterEndpoints(ctx.Endpoints, func(ep domain.EndpointAddr) bool {
					return !store.Blocked(serviceID, ep.Domain(), method)
				})
				if len(filtered) == 0 {
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
