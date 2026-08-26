package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/observe"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// Observe returns a middleware that, after the inner handler completes:
//  1. Records a reputation signal for the endpoint, for both success and
//     error — except when the relay errored and ctx.HeuristicResult
//     attributes the failure to the client (e.g. a client hang-up): that
//     case is nobody's signal, and buildSignal returns a zero Signal that
//     the call site skips recording entirely.
//  2. If the "observation_pipeline" feature flag is enabled, submits an
//     Observation to the async queue for deep processing off the hot path.
//
// Signal severity is derived from ctx.HeuristicResult. If no heuristic result
// is present, the signal defaults to success/minor-error based on HTTP status code.
func Observe(flags featureflag.FlagStore, queue *observe.Queue, repSvc reputation.Service) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			start := time.Now()

			err := next.HandleRelay(ctx)

			latency := time.Since(start)

			// Always record a reputation signal when an endpoint was selected.
			if ctx.Endpoint != "" {
				sig := buildSignal(ctx, err, latency)
				if sig.Type != "" {
					// Best-effort: ignore RecordSignal errors so we never block a relay.
					_ = repSvc.RecordSignal(context.Background(), ctx.ServiceID, ctx.Endpoint, ctx.RPCType, sig)
				}
			}

			// Optionally submit to the observation pipeline.
			if queue != nil && flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagObservationPipeline, ctx.ServiceID) {
				obs := buildObservation(ctx, latency)
				queue.Submit(obs)
			}

			return err
		})
	}
}

// buildSignal converts the relay outcome into a reputation.Signal.
// It consults the heuristic result (if present) to determine severity;
// otherwise it falls back to HTTP status code heuristics.
func buildSignal(ctx *relay.Context, relayErr error, latency time.Duration) reputation.Signal {
	// A failure the client caused is nobody's signal. successResult also
	// carries AttrClient, but with no error, so key on both.
	if relayErr != nil && ctx.HeuristicResult != nil && ctx.HeuristicResult.Attribution == heuristic.AttrClient {
		return reputation.Signal{}
	}

	// Check for a heuristic result stored by the Heuristic middleware.
	if ctx.HeuristicResult != nil && ctx.HeuristicResult.ShouldPenalize {
		return penaltySignal(*ctx.HeuristicResult, latency)
	}

	// No heuristic result — fall back to simple success/error logic.
	if relayErr != nil || !isSuccessStatus(ctx) {
		reason := "relay_error"
		if relayErr != nil {
			reason = relayErr.Error()
		}
		return reputation.NewMinorErrorSignal(reason, latency)
	}

	return reputation.NewSuccessSignal("relay_ok", latency)
}

// penaltySignal maps a heuristic penalty severity to the appropriate signal
// constructor.
func penaltySignal(result heuristic.AnalysisResult, latency time.Duration) reputation.Signal {
	reason := result.Reason
	if reason == "" {
		reason = result.Details
	}
	switch result.PenaltySeverity {
	case heuristic.SeverityFatal:
		return reputation.NewFatalErrorSignal(reason, latency)
	case heuristic.SeverityCritical:
		return reputation.NewCriticalErrorSignal(reason, latency)
	case heuristic.SeverityMajor:
		return reputation.NewMajorErrorSignal(reason, latency)
	default:
		return reputation.NewMinorErrorSignal(reason, latency)
	}
}

// isSuccessStatus returns true when the relay response has a 2xx HTTP status.
func isSuccessStatus(ctx *relay.Context) bool {
	if ctx.Response == nil {
		return false
	}
	return ctx.Response.HTTPStatusCode >= http.StatusOK &&
		ctx.Response.HTTPStatusCode < http.StatusMultipleChoices
}

// buildObservation constructs an observe.Observation from the relay context.
func buildObservation(ctx *relay.Context, latency time.Duration) observe.Observation {
	obs := observe.Observation{
		ServiceID:    ctx.ServiceID,
		EndpointAddr: ctx.Endpoint,
		Timestamp:    time.Now(),
		Source:       observe.SourceRelay,
		Latency:      latency,
	}

	if ctx.Response != nil {
		obs.HTTPStatus = ctx.Response.HTTPStatusCode
		obs.ResponseBody = ctx.Response.Body
	}

	if len(ctx.Payloads) > 0 {
		obs.RequestBody = ctx.Payloads[0].Bytes()
	}

	return obs
}
