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
func Heuristic(flags featureflag.FlagStore, registry *qos.Registry, opts ...HeuristicOption) relay.Middleware {
	var o heuristicOptions
	for _, opt := range opts {
		opt(&o)
	}
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

			breakUpstream(flags, ctx, &result)
			if result.Reason == "http_408" && result.Attribution == heuristic.AttrSupplier &&
				flags != nil && flags.IsEnabled(ctx.Ctx, featureflag.FlagMethodBlock408, ctx.ServiceID) {
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

// HeuristicOption tunes the Heuristic middleware.
type HeuristicOption func(*heuristicOptions)

type heuristicOptions struct {
	attemptTimeout func(domain.ServiceID) time.Duration
}

// WithAttemptTimeout gives the middleware each service's per-attempt relay
// timeout, the budget an attempt is owed. Without it every timeout is graded
// as the host's own.
func WithAttemptTimeout(fn func(domain.ServiceID) time.Duration) HeuristicOption {
	return func(o *heuristicOptions) { o.attemptTimeout = fn }
}

// budget is the time this attempt has (the smaller of the per-attempt timeout
// and what is left of the request) and the time it is owed. Zero owed means
// unknown.
func (o heuristicOptions) budget(ctx *relay.Context) (have, owed time.Duration) {
	if o.attemptTimeout == nil {
		return 0, 0
	}
	owed = o.attemptTimeout(ctx.ServiceID)
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
func gradeTimeoutBudget(r *heuristic.AnalysisResult, have, owed time.Duration) {
	if r.Reason != "transport_timeout" || owed <= 0 {
		return
	}
	r.Details += fmt.Sprintf(" (budget %s of %s)", have.Round(time.Millisecond), owed)
	if have < owed/shortBudgetShare {
		r.Reason = "short_budget_timeout"
		r.ShouldPenalize = false
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
	plugin := ctx.Plugin
	if plugin == nil && registry != nil {
		plugin = registry.Get(ctx.ServiceID)
	}
	if x, ok := plugin.(qos.DataExtractor); ok {
		_, _ = x.ExtractData(ctx.Endpoint, ctx.Payloads[0].Bytes(), ctx.Response.Body)
	}
}

// refineVerdict lets the service's plugin re-attribute the verdict from the
// request's shape (qos.VerdictRefiner); reports whether it did.
func refineVerdict(registry *qos.Registry, ctx *relay.Context, result *heuristic.AnalysisResult) bool {
	if len(ctx.Payloads) == 0 {
		return false
	}
	plugin := ctx.Plugin
	if plugin == nil && registry != nil {
		plugin = registry.Get(ctx.ServiceID)
	}
	refiner, ok := plugin.(qos.VerdictRefiner)
	if !ok {
		return false
	}
	refined, ok := refiner.RefineVerdict(ctx.Payloads[0], *result)
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
