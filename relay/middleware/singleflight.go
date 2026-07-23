package middleware

import (
	"crypto/sha256"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// Singleflight returns a middleware that coalesces concurrent identical
// requests into a single upstream relay. When two or more requests for the
// same service + method + params arrive simultaneously, only one relay is
// executed; the others share its response and have ctx.Coalesced set to true.
//
// Coalescing is only applied when:
//   - The "singleflight" feature flag is enabled for the service.
//   - The request carries exactly one payload.
//   - The QoS plugin for the service (ctx.Plugin, resolved by Parse)
//     implements CoalescenceClassifier and reports the method as coalescable.
func Singleflight(flags featureflag.FlagStore) relay.Middleware {
	// groups maps domain.ServiceID → *singleflight.Group (created on demand).
	var groups sync.Map

	groupFor := func(serviceID domain.ServiceID) *singleflight.Group {
		v, _ := groups.LoadOrStore(serviceID, &singleflight.Group{})
		return v.(*singleflight.Group)
	}

	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if !flags.IsEnabled(ctx.Ctx, featureflag.FlagSingleflight, ctx.ServiceID) {
				return next.HandleRelay(ctx)
			}

			// Only coalesce single-payload requests; batch payloads are handled
			// by the Batch middleware independently.
			if len(ctx.Payloads) != 1 {
				return next.HandleRelay(ctx)
			}

			// Check whether the method is coalescable via the plugin.
			// ctx.Plugin was already resolved by the Parse middleware — no
			// need to hit the registry (and its lock) again.
			classifier, ok := ctx.Plugin.(qos.CoalescenceClassifier)
			if !ok || !classifier.IsCoalescable(ctx.Payloads[0].Method()) {
				return next.HandleRelay(ctx)
			}

			key := coalescingKey(ctx.ServiceID, ctx.Payloads[0])
			group := groupFor(ctx.ServiceID)

			// ranRelay is set to true inside the Do closure to distinguish the
			// goroutine that actually executed the relay from followers that only
			// received the shared result. singleflight.Do returns shared=true for
			// ALL callers (including the leader) when there is contention, so we
			// cannot rely on the shared flag alone to detect followers.
			ranRelay := false

			v, err, _ := group.Do(key, func() (any, error) {
				ranRelay = true
				if err := next.HandleRelay(ctx); err != nil {
					return nil, err
				}
				return ctx.Response, nil
			})

			if err != nil {
				return err
			}

			if !ranRelay {
				// This goroutine was a follower: copy the shared response and mark
				// the context as coalesced.
				if resp, ok := v.(*domain.Response); ok {
					ctx.Response = resp
				}
				ctx.Coalesced = true
			}
			// When ranRelay==true, next.HandleRelay already populated ctx.Response.

			return nil
		})
	}
}

// coalescingKey builds a stable string key for singleflight deduplication:
// "<serviceID>:<method>:" + raw sha256 of the payload. The key is only a map
// key inside singleflight (never displayed), so hex encoding would just
// double its size and add an allocation per coalescable request.
func coalescingKey(serviceID domain.ServiceID, p domain.Payload) string {
	sum := sha256.Sum256(p.Bytes())
	return string(serviceID) + ":" + p.Method() + ":" + string(sum[:])
}
