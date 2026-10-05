package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// MetricsRecorder is the interface that metrics backends must implement.
// Implementations should be safe for concurrent use.
type MetricsRecorder interface {
	// RecordRelay records the outcome of a single relay attempt.
	// statusCode is the HTTP status code of the backend response (0 if unknown).
	// latency is the total time from the start of the relay to response receipt.
	// err is non-nil if the relay failed.
	RecordRelay(serviceID domain.ServiceID, endpoint domain.EndpointAddr, statusCode int, latency time.Duration, err error)
	// RecordVerdict records the heuristic's reading of a single relay attempt:
	// the reason it settled on and which side it attributed the outcome to.
	// Called once per attempt that produced a verdict, so a retried request
	// records one verdict per attempt.
	RecordVerdict(serviceID domain.ServiceID, rpcType domain.RPCType, reason, attribution string)
	// RecordUnclassified records the detail of a verdict the heuristic could
	// not place (server_error, unknown_error_code): the wordings left to
	// catalogue.
	RecordUnclassified(serviceID domain.ServiceID, reason, detail string)
	// RecordOperatorAttempt counts one attempt per operator: who served it,
	// whose fault the outcome was, and how long it took. kind is the
	// attempt's relay.AttemptKind label (first, retry, hedge, probation);
	// methodClass is methodClassOf the request's method.
	RecordOperatorAttempt(serviceID domain.ServiceID, rpcType domain.RPCType, endpoint domain.EndpointAddr, attribution, kind, methodClass string, latency time.Duration)
}

// methodClassOf sorts a method into what it costs a node to answer: light
// (a head, a chain id, a health check), heavy (log scans, calls, traces,
// program-account scans), or standard. An operator that answers the light ones
// and stalls the heavy ones looks healthy on a success rate that mixes them;
// split by class, on first attempts only, it does not.
func methodClassOf(method string) string {
	switch {
	case lightMethods[method]:
		return "light"
	case heavyMethods[method],
		strings.HasPrefix(method, "debug_"), strings.HasPrefix(method, "trace_"):
		return "heavy"
	}
	return "standard"
}

var lightMethods = map[string]bool{
	"eth_blockNumber": true, "eth_chainId": true, "net_version": true, "eth_gasPrice": true,
	"eth_maxPriorityFeePerGas": true, "eth_syncing": true, "web3_clientVersion": true,
	"getSlot": true, "getBlockHeight": true, "getHealth": true, "getLatestBlockhash": true,
	"status": true, "health": true, "abci_info": true,
}

var heavyMethods = map[string]bool{
	"eth_getLogs": true, "eth_call": true, "eth_estimateGas": true, "eth_getBlockReceipts": true,
	"eth_getProof": true, "getProgramAccounts": true, "getSignaturesForAddress": true,
	"getMultipleAccounts": true, "getBlock": true, "abci_query": true, "tx_search": true,
	"block_results": true,
}

// AttemptHook is told every attempt Metrics records against an operator:
// service, RPC type, endpoint, attribution (none, blockchain, supplier,
// unknown, client), kind (first, retry, hedge, probation) and the method as
// the service's plugin catalogues it ("" when it has no notion of one).
type AttemptHook func(serviceID domain.ServiceID, rpcType domain.RPCType, endpoint domain.EndpointAddr, attribution, kind, method string)

// MetricsOption configures Metrics.
type MetricsOption func(*metricsOptions)

type metricsOptions struct{ attemptHook AttemptHook }

// WithAttemptHook installs fn, run on every attempt after it is recorded. It
// runs on every attempt, so it must not block.
func WithAttemptHook(fn AttemptHook) MetricsOption {
	return func(o *metricsOptions) { o.attemptHook = fn }
}

// Metrics returns a middleware that records one upstream attempt via recorder
// after it completes: status, the endpoint the attempt picked, and the
// attempt's own latency. It belongs inside retry, hedge and batch and outside
// select_endpoint — relay/chain_order.go enforces that — so a retried or
// hedged request is recorded once per attempt. The middleware always calls
// next.HandleRelay; recording is best-effort and never affects the returned
// error.
func Metrics(recorder MetricsRecorder, opts ...MetricsOption) relay.Middleware {
	var o metricsOptions
	for _, opt := range opts {
		opt(&o)
	}
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			start := time.Now()

			err := next.HandleRelay(ctx)

			latency := time.Since(start)

			statusCode := 0
			if ctx.Response != nil {
				statusCode = ctx.Response.HTTPStatusCode
			} else if err != nil {
				// Use 502 Bad Gateway as a sentinel when the relay itself failed.
				statusCode = http.StatusBadGateway
			}

			recorder.RecordRelay(ctx.ServiceID, ctx.Endpoint, statusCode, latency, err)

			// The heuristic runs inside this middleware, so its verdict for
			// this attempt is on the context by now. Retry clears the field
			// before each attempt, so a verdict is never carried over from
			// the previous one. This is the only place the verdict itself is
			// counted: reputation_attempts_total sees only what scoring kept
			// (client-attributed outcomes are dropped before it), and
			// retry_total sees only what retried. What the gateway concluded
			// about every answer — passed through, penalised, retried — was
			// otherwise invisible.
			if v := ctx.HeuristicResult; v != nil {
				recorder.RecordVerdict(ctx.ServiceID, ctx.RPCType, v.Reason, verdictAttribution(v))
				if v.Reason == "server_error" || v.Reason == "unknown_error_code" {
					recorder.RecordUnclassified(ctx.ServiceID, v.Reason, v.Details)
				}
			}
			if ctx.Endpoint != "" {
				kind := ctx.AttemptKind
				if kind == "" {
					kind = relay.AttemptFirst
				}
				method := ""
				if len(ctx.Payloads) > 0 {
					method = ctx.Payloads[0].Method()
				}
				attribution := attemptAttribution(ctx.HeuristicResult, err)
				recorder.RecordOperatorAttempt(ctx.ServiceID, ctx.RPCType, ctx.Endpoint,
					attribution, kind, methodClassOf(method), latency)
				if o.attemptHook != nil {
					o.attemptHook(ctx.ServiceID, ctx.RPCType, ctx.Endpoint, attribution, kind, cataloguedMethod(ctx))
				}
			}

			return err
		})
	}
}

// cataloguedMethod is the request's method as the service's plugin names it,
// the key method blocks steer by; "" when the plugin has no notion of one.
func cataloguedMethod(ctx *relay.Context) string {
	if n, ok := ctx.Plugin.(qos.MethodNormalizer); ok && len(ctx.Payloads) > 0 {
		return n.NormalizeMethod(ctx.Payloads[0])
	}
	return ""
}

// verdictAttribution is the attribution label for a verdict. A success carries
// heuristic.AttrClient internally, meaning "no action needed", which exported
// as "client" reads as a client error beside the real ones; it is exported as
// "none" so the label answers only "whose fault" and success has no answer.
func verdictAttribution(v *heuristic.AnalysisResult) string {
	if v.Reason == heuristic.ReasonSuccess {
		return verdictAttributionNone
	}
	return v.Attribution.String()
}

const verdictAttributionNone = "none"

// attemptAttribution is verdictAttribution for an attempt that may have no
// verdict: the heuristic flag off, or a failure before any answer. A clean
// return is a good answer, and a failure nothing graded is nobody's known
// fault.
func attemptAttribution(v *heuristic.AnalysisResult, err error) string {
	switch {
	case v != nil:
		return verdictAttribution(v)
	case err == nil:
		return verdictAttributionNone
	default:
		return heuristic.AttrUnknown.String()
	}
}
