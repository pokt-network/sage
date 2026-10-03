package middleware

import (
	"crypto/sha256"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// SingleflightRecorder is notified of each request served from another's
// in-flight relay. metrics.Recorder satisfies it. Nil disables recording.
type SingleflightRecorder interface {
	RecordSingleflightCoalesced(serviceID domain.ServiceID)
}

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
//
// rec, when non-nil, feeds sage_singleflight_coalesced_total. The counter was
// registered and documented from the start and incremented by nothing until
// 2026-09-04: the middleware took no recorder.
func Singleflight(flags featureflag.FlagStore, rec SingleflightRecorder) relay.Middleware {
	// groups maps domain.ServiceID → *singleflight.Group (created on demand).
	var groups sync.Map

	groupFor := func(serviceID domain.ServiceID) *singleflight.Group {
		v, _ := groups.LoadOrStore(serviceID, &singleflight.Group{})
		return v.(*singleflight.Group)
	}

	return func(next relay.Handler) relay.Handler {
		var coalesce relay.HandlerFunc
		coalesce = func(ctx *relay.Context) error {
			if ctx.QuorumArm || !flags.IsEnabled(ctx.Ctx, featureflag.FlagSingleflight, ctx.ServiceID) {
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

			v, _, _ := group.Do(key, func() (any, error) {
				ranRelay = true
				err := next.HandleRelay(ctx)
				return flight{
					response:   ctx.Response,
					verdict:    ctx.HeuristicResult,
					err:        err,
					leaderGone: ctx.Ctx.Err() != nil,
				}, nil
			})
			f := v.(flight)

			// When ranRelay==true, next.HandleRelay already populated ctx.
			if ranRelay {
				return f.err
			}

			// The leader's client hung up or ran out of time: that ended the
			// leader's relay, not this one. Sharing the outcome handed a
			// follower whose client was still waiting context.Canceled. It
			// runs again through this middleware, so the followers left
			// behind coalesce under a new leader rather than each sending
			// its own relay.
			if f.leaderGone && ctx.Ctx.Err() == nil {
				return coalesce(ctx)
			}

			// A follower gets everything the leader's client got: the
			// response, the verdict that says whose it is and the error. A
			// retry verdict with the node's answer in hand is delivered as
			// that answer (router.go); a follower handed the error alone got
			// a gateway -32603 instead.
			ctx.Response, ctx.HeuristicResult = f.response, f.verdict
			ctx.Coalesced = true
			if rec != nil {
				rec.RecordSingleflightCoalesced(ctx.ServiceID)
			}
			return f.err
		}
		return coalesce
	}
}

// flight is what a coalesced relay hands its followers: all of the leader's
// outcome, and whether it ended because the leader's own client left.
type flight struct {
	response   *domain.Response
	verdict    *heuristic.AnalysisResult
	err        error
	leaderGone bool
}

// coalescingKey builds a stable string key for singleflight deduplication:
// the raw sha256 of the service and the payload's request identity
// (writeRequestKey), the same identity the response cache keys on. The key
// is only a map key inside singleflight (never displayed), so hex encoding
// would just double its size.
func coalescingKey(serviceID domain.ServiceID, p domain.Payload) string {
	h := sha256.New()
	_, _ = h.Write([]byte(serviceID))
	writeRequestKey(h, p)
	var sum [sha256.Size]byte
	return string(h.Sum(sum[:0]))
}
