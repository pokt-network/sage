package middleware

import (
	"context"
	"time"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// Score records one reputation signal per relay attempt, for the endpoint
// that served it, from the verdict that attempt produced. It sits inside
// retry, hedge and batch and directly outside heuristic, so a retried or
// hedged attempt that lost is still scored, and a batch item is scored once
// per endpoint through the sink batch installs (docs/scoring.md §4.1–4.3).
//
// Grading is attemptSignal: buildSignal — the same table Observe used when it
// scored per client request — plus the two attributions that must not become a
// penalty. A client-attributed error (a hang-up, a method the chain does not
// have) is nobody's signal and is dropped here, so it is not an attempt for
// the rate term either. A blockchain-attributed answer (block not found, a
// pruned height) is the endpoint answering correctly about a chain state it
// does not have, and scores a success.
//
// Gated by scoring_v2. Off, this is a pass-through and Observe scores as
// before; on, Observe records nothing.
func Score(flags featureflag.FlagStore, repSvc reputation.Service) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			if repSvc == nil || !scoringV2Enabled(flags, ctx) {
				return next.HandleRelay(ctx)
			}
			start := time.Now()
			err := next.HandleRelay(ctx)
			// No endpoint means no attempt was made — selection itself failed,
			// or the chain answered from cache. Nobody served anything.
			if ctx.Endpoint == "" {
				return err
			}
			if nobodysSignal(ctx.HeuristicResult, err) {
				return err
			}
			sig := buildSignal(ctx, err, time.Since(start))
			if sig.Type == "" {
				return err
			}
			if ctx.ScoreSink != nil {
				ctx.ScoreSink.Add(ctx.Endpoint, ctx.RPCType, sig)
				return err
			}
			// Best-effort, as Observe was: never block a relay on scoring.
			_ = repSvc.RecordSignal(context.Background(), ctx.ServiceID, ctx.Endpoint, ctx.RPCType, sig)
			return err
		})
	}
}

// scoringV2Enabled reports whether per-attempt scoring is on for this request.
// Observe, score and batch each read the flag per request rather than caching
// the answer on the context, so an admin flipping it mid-request can have that
// one request counted on both paths or neither — an admin action, not a
// traffic pattern.
//
// A nil FlagStore reads as OFF, matching Observe and batch: the one thing that
// must not happen is score and Observe both deciding they are the recorder.
func scoringV2Enabled(flags featureflag.FlagStore, ctx *relay.Context) bool {
	return flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagScoringV2, ctx.ServiceID)
}

// nobodysSignal reports whether this attempt's verdict belongs to nobody: the
// client hung up, or asked for something this chain cannot answer (-32601, a
// revert, bad params, an HTTP 4xx). The supplier did its job in both cases, so
// the attempt is neither a success for it nor a failure against it — and,
// because nothing is recorded, it is not an attempt for the chronic-rate term
// either (docs/scoring.md §7.1).
//
// buildSignal already drops the half of this that carries an error. The half
// it cannot drop is the one where the supplier answered correctly WITH a
// client error: that reaches here with a nil error and a 200, and would grade
// as a plain success. On beta that was 66% of batch traffic, all of it
// -32601, which is also what makes batch's promise hold — "a batch of only
// client-attributed items scores nothing" is true because nothing was ever
// added to the sink.
func nobodysSignal(res *heuristic.AnalysisResult, relayErr error) bool {
	if res == nil || res.Attribution != heuristic.AttrClient {
		return false
	}
	return relayErr != nil || !res.IsSuccess()
}
