package middleware

import (
	"fmt"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"time"
)

// Heuristic returns a middleware that analyses the relay outcome after the
// inner chain has run. It runs heuristic.Analyze on the response body and
// stores the AnalysisResult on the context. When the result indicates the
// request should be retried, a retryable RelayError is returned so that outer
// retry/hedge middleware can react.
//
// When the inner chain returned an ERROR there is no body, but the failure is
// still evidence, so it is graded on the way out by
// heuristic.AnalyzeTransportError: a dead host, a host that cannot do this
// method, or a client that hung up. That part runs regardless of the
// "heuristic" feature flag — grading a transport error is attribution, not
// response analysis, and the circuit breaker, the method blocks and
// reputation all key on it. The flag gates body analysis only.
func Heuristic(flags featureflag.FlagStore, registry *qos.Registry, o HeuristicOptions) relay.Middleware {
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			budget, full := o.budget(ctx)
			// Run the inner chain first.
			if err := next.HandleRelay(ctx); err != nil {
				// No body to analyse, but the failure itself is evidence: a
				// dead host, a host that cannot do this method, or a client
				// that hung up. Without a verdict here none of that reached
				// the breaker, the method blocks, or reputation correctly.
				// The flag gate below is deliberately not applied: grading a
				// transport error is attribution, not response analysis.
				result := heuristic.AnalyzeTransportError(err, ctx.Ctx.Err())
				gradeTimeoutBudget(&result, budget, full)
				// The plugin may know the route better than the analyzer
				// (qos.VerdictRefiner): a verdict refined to "deliver" must
				// also stop Retry, which keys on the error's own flag.
				if refineVerdict(registry, ctx, &result) && !result.ShouldRetry && domain.IsRetryable(err) {
					if re, ok := err.(*domain.RelayError); ok {
						err = domain.NewRelayError(re.Kind, re.Message, re.Cause, false)
						ctx.Err = err
					}
				}
				breakUpstream(flags, ctx, &result)
				ctx.HeuristicResult = &result
				return err
			}

			var headLag uint64
			var headStale bool
			if ctx.Response != nil {
				headLag, headStale = o.observeHeadLag(registry, ctx)
			}

			// Skip analysis if the flag is disabled.
			if flags != nil && !flags.IsEnabled(ctx.Ctx, featureflag.FlagHeuristic, ctx.ServiceID) {
				return nil
			}

			// Only analyse when we have a response.
			if ctx.Response == nil {
				return nil
			}

			// gRPC reports its outcome in grpc-status rather than in the body,
			// so it gets the analyzer that can read one. Without this a chain
			// error like NOT_FOUND would be retried across suppliers and
			// penalize each of them for answering correctly.
			var result heuristic.AnalysisResult
			if ctx.RPCType == domain.RPCTypeGRPC {
				code, message, ok := ctx.Response.GRPCStatus()
				result = heuristic.AnalyzeGRPC(ctx.Response.Body, code, message, ok)
			} else {
				result = heuristic.Analyze(
					ctx.Response.Body,
					ctx.Response.HTTPStatusCode,
					ctx.RPCType,
				)
			}

			// rest_bodies_as_answers is the live undo for passing an empty or
			// plain-text REST answer: off, it is graded by the structural
			// rules as before. Ahead of the plugin's refinement, which may
			// pass a body of its own on top.
			if ctx.RPCType == domain.RPCTypeREST && result.IsSuccess() &&
				flags != nil && !flags.IsEnabled(ctx.Ctx, featureflag.FlagRESTBodiesAsAnswers, ctx.ServiceID) {
				if strict, ok := heuristic.StrictRESTBody(ctx.Response.Body, ctx.Response.HTTPStatusCode); ok {
					result = strict
				}
			}

			// A -32601 on a method the plugin catalogues is retried on another
			// operator. The analyzer leaves it unretried because it cannot tell
			// a real method from a bogus name, and a bogus name must not bounce
			// across the pool; here the catalogue tells them apart. On the
			// 2026-09-13 canary two thirds of kava's json_rpc stakes fronted a
			// CometBFT node and answered eth_blockNumber with -32601; the
			// method block that verdict sets steers the NEXT request, this
			// retry serves the one in hand. PATH passes the -32601 to the
			// client. Attribution stays client so nothing is scored.
			if result.Reason == heuristic.ReasonMethodNotFound && !result.ShouldRetry && namedMethod(registry, ctx) {
				result.ShouldRetry = true
			}

			// The plugin's word on the route: a 5xx the node answers by
			// design to a query it cannot serve is the chain's answer, not
			// the host's failure (qos.VerdictRefiner).
			refineVerdict(registry, ctx, &result)

			// penalize_408 is the live undo for scoring a supplier's 408
			// (heuristic/analyzer.go): off, the 408 is still retried, not scored.
			if result.Reason == "http_408" && flags != nil && !flags.IsEnabled(ctx.Ctx, featureflag.FlagPenalize408, ctx.ServiceID) {
				result.ShouldPenalize = false
			}

			// An answer the analyzer passed, naming a head too far behind
			// (featureflag.FlagStaleResponse): a cache's old view, retried.
			if headStale && result.IsSuccess() && flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagStaleResponse, ctx.ServiceID) {
				result = heuristic.StaleResponse(headLag)
			}

			// An answer the analyzer passed whose result no node produces for
			// its method (qos.ResultValidator): counted always, graded
			// invalid_result and retried behind featureflag.FlagInvalidResult.
			if result.IsSuccess() {
				if detail, invalid := o.invalidResult(registry, ctx); invalid &&
					flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagInvalidResult, ctx.ServiceID) {
					result = heuristic.InvalidResult(detail)
				}
			}

			breakUpstream(flags, ctx, &result)
			if result.Reason == "http_408" && result.Attribution == heuristic.AttrSupplier &&
				flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagMethodBlock408, ctx.ServiceID) {
				result.MethodBlocking = true
			}
			// A refusal worded as a prune (heuristic.RefusedRecent) keeps the
			// method away from the host the same way, behind its own flag.
			if result.Reason == heuristic.ReasonRefusedRecent &&
				flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagMethodBlockRefusal, ctx.ServiceID) {
				result.MethodBlocking = true
			}
			ctx.HeuristicResult = &result

			if result.ShouldRetry && result.Attribution == heuristic.AttrBlockchain {
				observeAttempt(registry, ctx)
			}

			if result.ShouldRetry {
				relayErr := domain.NewRelayError(
					domain.ErrEndpoint,
					"heuristic analysis suggests retry: "+result.Reason,
					domain.ErrRetryVerdict,
					true,
				)
				ctx.Err = relayErr
				return relayErr
			}

			return nil
		})
	}
}

// HeuristicOptions tunes the Heuristic middleware. Every field has a working
// zero value.
type HeuristicOptions struct {
	// AttemptTimeout gives each service's per-attempt relay timeout, the
	// budget an attempt is owed. Nil grades every timeout as the host's own.
	AttemptTimeout func(domain.ServiceID) time.Duration
	// HeadLag receives, for every answer that names the chain head, how far
	// it lagged the head the service's plugin expected (qos.HeadLagReader).
	HeadLag func(serviceID domain.ServiceID, party, method string, lag uint64, stale bool)
	// InvalidResult receives every single-payload answer whose result the
	// service's plugin says no node produces (qos.ResultValidator).
	InvalidResult func(serviceID domain.ServiceID, party, method string)
}

// invalidResult asks the service's plugin whether a single-payload answer's
// result is one no node produces, and reports it to o.InvalidResult.
func (o HeuristicOptions) invalidResult(registry *qos.Registry, ctx *relay.Context) (string, bool) {
	if ctx.Response == nil || len(ctx.Payloads) != 1 {
		return "", false
	}
	v, ok := pluginOf(registry, ctx).(qos.ResultValidator)
	if !ok {
		return "", false
	}
	detail, invalid := v.InvalidResult(ctx.Payloads[0], ctx.Response.Body)
	if invalid && o.InvalidResult != nil {
		o.InvalidResult(ctx.ServiceID, ctx.Endpoint.Party(), ctx.Payloads[0].Method())
	}
	return detail, invalid
}

// observeHeadLag reads how far a single-payload answer that names the chain
// head lags the perceived head, hands it to the head-lag recorder when one is
// set, and returns it for the stale_response verdict. The recording is
// measurement only, before and apart from the heuristic flag.
func (o HeuristicOptions) observeHeadLag(registry *qos.Registry, ctx *relay.Context) (lag uint64, stale bool) {
	if len(ctx.Payloads) != 1 || ctx.Response.HTTPStatusCode != 200 {
		return 0, false
	}
	reader, ok := pluginOf(registry, ctx).(qos.HeadLagReader)
	if !ok {
		return 0, false
	}
	lag, stale, ok = reader.HeadLag(ctx.Payloads[0], ctx.Response.Body, time.Now())
	if !ok {
		return 0, false
	}
	if o.HeadLag != nil {
		o.HeadLag(ctx.ServiceID, ctx.Endpoint.Party(), ctx.Payloads[0].Method(), lag, stale)
	}
	return lag, stale
}

// budget is the time this attempt has (the smaller of the per-attempt timeout
// and what is left of the request) and the time it is owed. Zero owed means
// unknown.
func (o HeuristicOptions) budget(ctx *relay.Context) (have, owed time.Duration) {
	if o.AttemptTimeout == nil {
		return 0, 0
	}
	owed = o.AttemptTimeout(ctx.ServiceID)
	have = owed
	if dl, ok := ctx.Ctx.Deadline(); ok {
		if left := time.Until(dl); left < have || have <= 0 {
			have = left
		}
	}
	return have, owed
}

// shortBudgetShare: an attempt left with less than 1/shortBudgetShare of the
// per-attempt timeout did not get a fair chance to answer.
const shortBudgetShare = 2

// gradeTimeoutBudget stamps a timeout verdict with the budget the attempt had,
// and takes the penalty off one that had too little of it.
//
// A timeout on an attempt that started with a fraction of its budget — a retry
// after another host used most of the request, a probation try held to a
// quarter — measures the time others spent, not the host. Graded major, it
// kept a working operator floored: once its keys were low it was sent mostly
// such leftovers, they timed out, and the score stayed low. On mainnet sei
// (2026-09-28) that held one operator's keys at 0-3 while it served 97% of the
// same service for PATH; a manual reset put them at 99 for good. Still
// retried, not scored and not method-blocked. A host that is truly dead is
// still demoted by its health checks, which always run with the full budget.
//
// A dial the deadline ended (heuristic.ReasonConnectTimeout) is the same case
// one step earlier, and it carried more: critical and a breaker vote, so one
// late retry with 50ms left could take a working host out of the pool.
func gradeTimeoutBudget(r *heuristic.AnalysisResult, have, owed time.Duration) {
	if (r.Reason != "transport_timeout" && r.Reason != heuristic.ReasonConnectTimeout) || owed <= 0 {
		return
	}
	r.Details += fmt.Sprintf(" (budget %s of %s)", have.Round(time.Millisecond), owed)
	if have < owed/shortBudgetShare {
		r.Reason = "short_budget_timeout"
		r.ShouldPenalize = false
		r.ShouldCircuitBreak = false
		r.PenaltySeverity = ""
		r.MethodBlocking = false
	}
}

// observeAttempt hands the plugin an attempt whose answer the chain state
// explains, so an endpoint saying it does not keep the state asked for is
// remembered (qos.DataExtractor).
//
// The observation pipeline sees only the final attempt: it sits outside Retry.
// A pruned node answering first is retried away, the archival node that serves
// the retry is what gets observed, and the pruned node is never marked. On
// mainnet base (2026-09-26) one such node took the first attempt of numbered
// eth_getBalance calls two years back, around 1,000 times in five minutes, on
// every pod, answering "historical state is not available" each time.
func observeAttempt(registry *qos.Registry, ctx *relay.Context) {
	if len(ctx.Payloads) == 0 {
		return
	}
	if x, ok := pluginOf(registry, ctx).(qos.DataExtractor); ok {
		_, _ = x.ExtractData(ctx.Endpoint, ctx.Payloads[0].Bytes(), ctx.Response.Body)
	}
}

// pluginOf is the request's plugin: the one Parse set, else the service's
// registered one. Nil when neither is known.
func pluginOf(registry *qos.Registry, ctx *relay.Context) qos.Plugin {
	if ctx.Plugin == nil && registry != nil {
		return registry.Get(ctx.ServiceID)
	}
	return ctx.Plugin
}

// refineVerdict lets the service's plugin re-attribute the verdict from the
// request's shape (qos.VerdictRefiner); reports whether it did.
func refineVerdict(registry *qos.Registry, ctx *relay.Context, result *heuristic.AnalysisResult) bool {
	if len(ctx.Payloads) == 0 {
		return false
	}
	refiner, ok := pluginOf(registry, ctx).(qos.VerdictRefiner)
	if !ok {
		return false
	}
	refined, ok := refiner.RefineVerdict(ctx.Endpoint, ctx.Payloads[0], *result)
	if !ok {
		return false
	}
	*result = refined
	return true
}

// namedMethod reports whether the request's method is one the service's
// plugin catalogues: not empty, not the MethodOther bucket.
func namedMethod(registry *qos.Registry, ctx *relay.Context) bool {
	m := normalizedMethod(registry, ctx)
	return m != "" && m != qos.MethodOther
}

// breakUpstream hands a relay miner's own failure answers — its 408 and its
// 5xx — to the circuit breaker's rate gate while circuit_break_upstream is on
// for the service (see the flag). A backend's answer never qualifies.
func breakUpstream(flags featureflag.FlagStore, ctx *relay.Context, result *heuristic.AnalysisResult) {
	if (result.Reason == "http_408" || result.Reason == "upstream_5xx") && result.Attribution == heuristic.AttrSupplier &&
		flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagCircuitBreakUpstream, ctx.ServiceID) {
		result.ShouldCircuitBreak = true
	}
}
