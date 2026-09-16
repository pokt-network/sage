package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/protocol"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// Quorum request and response headers. The request pair is the whole trigger:
// there is no quorum route, so a client adds a header to a request it already
// sends. The response set says what was actually done, because the cost (N
// paid relays) and the shape (collect mode is an envelope) both depend on it.
const (
	headerQuorumCount = "Target-Quorum-Count"
	headerQuorumMode  = "Target-Quorum-Mode"

	headerQuorumModeUsed = "X-Quorum-Mode"
	headerQuorumArms     = "X-Quorum-Count"
	headerQuorumAchieved = "X-Quorum-Achieved"
	headerQuorumMajority = "X-Quorum-Majority"
	headerQuorumSkipped  = "X-Quorum-Skipped"
)

// Quorum modes, the values of Target-Quorum-Mode.
const (
	QuorumModeConsensus = "consensus"
	QuorumModeCollect   = "collect"
)

const (
	// defaultQuorum is the arm count when the header is absent or unreadable,
	// and the floor: fewer than three answers cannot outvote one.
	defaultQuorum = 3
	// maxQuorum caps the paid relays one request may buy.
	maxQuorum = 9
)

// QuorumRecorder counts quorum requests by outcome and the answers that
// disagreed with a majority. metrics.Recorder satisfies it.
type QuorumRecorder interface {
	RecordQuorum(serviceID domain.ServiceID, outcome string)
	RecordQuorumDissent(serviceID domain.ServiceID, n int)
}

// Quorum returns the middleware that sends one request to several operators at
// once, triggered per request by Target-Quorum-Count / Target-Quorum-Mode and
// gated per service by the quorum flag. The design and its decisions are in
// docs/design/specs/2026-08-31-multi-supplier-quorum-design.md.
//
// Each arm is a clone marked QuorumArm whose candidate list is one operator's
// endpoints, run through the rest of the chain: selection, circuit breaking,
// method blocks, scoring, the heuristic and metrics all apply to it as to any
// attempt, while cache, singleflight, retry and hedge pass through. Operators
// are ranked by their best endpoint's score and the top N taken, N odd, 3 to
// 9, and never more than the operators there are.
//
// Consensus mode returns the first answer a majority of arms agree on, as the
// normal response; the arms still running finish detached and score
// themselves. It applies only to a request the plugin calls immutable
// (qos.ImmutableClassifier) — any other is answered in collect mode, since
// honest nodes a block apart disagree on it. Collect mode, and a consensus
// that ends without a majority, answer with every arm's result in one
// envelope, so a disagreement changes the shape of the response rather than
// hiding behind one arm's answer.
//
// A single arm's answer is a vote only if it came back without an error, and
// only once per operator: an arm whose operator's endpoints were all pruned is
// handed the full pool by select_endpoint, and its answer must not count twice.
func Quorum(flags featureflag.FlagStore, endpoints protocol.EndpointProvider, repSvc reputation.Service, rec QuorumRecorder) relay.Middleware {
	record := func(ctx *relay.Context, outcome string) {
		if rec != nil {
			rec.RecordQuorum(ctx.ServiceID, outcome)
		}
	}
	return func(next relay.Handler) relay.Handler {
		return relay.HandlerFunc(func(ctx *relay.Context) error {
			countHeader := ctx.HTTPRequest.Header.Get(headerQuorumCount)
			modeHeader := ctx.HTTPRequest.Header.Get(headerQuorumMode)
			if countHeader == "" && modeHeader == "" {
				return next.HandleRelay(ctx)
			}

			mode := strings.ToLower(strings.TrimSpace(modeHeader))
			switch mode {
			case "":
				mode = QuorumModeConsensus
			case QuorumModeConsensus, QuorumModeCollect:
			default:
				return rejectRequest(ctx, singleBody(ctx), http.StatusBadRequest, domain.ErrValidation,
					fmt.Sprintf("invalid %s header value %q", headerQuorumMode, modeHeader), nil,
					map[string]any{"allowed_quorum_modes": []string{QuorumModeConsensus, QuorumModeCollect}})
			}

			skip := func(reason string) error {
				setHeader(ctx, headerQuorumSkipped, reason)
				record(ctx, "skipped_"+reason)
				return next.HandleRelay(ctx)
			}
			switch {
			case flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagQuorum, ctx.ServiceID):
				return skip("disabled")
			case len(ctx.Payloads) != 1:
				// A quorum per batch item multiplies the fan-out by the batch,
				// which is the path that took mainnet pods down on 2026-09-16.
				return skip("batch")
			case ctx.RPCType == domain.RPCTypeGRPC:
				// A binary body cannot sit in a JSON envelope, and gRPC's
				// outcome travels in trailers the vote never sees.
				return skip("rpc_type")
			}

			outcome := ""
			if mode == QuorumModeConsensus {
				if c, ok := ctx.Plugin.(qos.ImmutableClassifier); !ok || !c.IsImmutable(ctx.Payloads[0]) {
					mode = QuorumModeCollect
					outcome = "collect_not_immutable"
				}
			}

			pool, err := endpoints.AvailableEndpoints(ctx.Ctx, ctx.ServiceID, ctx.RPCType)
			if err != nil {
				return err
			}
			groups := topOperators(ctx, repSvc, pool, quorumSize(countHeader))
			if len(groups) == 0 {
				return domain.NewRelayError(domain.ErrProtocol, "no endpoint available for service", nil, false)
			}

			results := make(chan armResult, len(groups))
			for i, g := range groups {
				arm := ctx.Clone()
				arm.QuorumArm = true
				arm.Endpoints = g.endpoints
				arm.Endpoint = ""
				arm.SelectedEndpoint = nil
				arm.Response = nil
				arm.Err = nil
				arm.HeuristicResult = nil
				// Detached like a hedge arm: consensus returns before the
				// stragglers finish, and their signed relays must still flush.
				armCtx, cancel := armContext(ctx.Ctx)
				arm.Ctx = armCtx
				go func() {
					defer safego.Recover(arm.Logger, "quorum.arm.goroutine")
					defer cancel()
					err := safego.Call(arm.Logger, "quorum.arm", func() error {
						return next.HandleRelay(arm)
					})
					results <- armResult{index: i, ctx: arm, err: err}
				}()
			}

			need := len(groups)/2 + 1
			votes := map[[sha256.Size]byte][]armResult{}
			voted := map[string]bool{}
			done := make([]*armResult, len(groups))
			answered := 0

		wait:
			for received := 0; received < len(groups); received++ {
				select {
				case <-ctx.Ctx.Done():
					break wait
				case r := <-results:
					done[r.index] = &r
					if r.err != nil || r.ctx.Response == nil {
						continue
					}
					answered++
					if mode != QuorumModeConsensus {
						continue
					}
					op := r.ctx.Endpoint.Operator()
					if voted[op] {
						continue
					}
					voted[op] = true
					digest := answerDigest(r.ctx.Response)
					votes[digest] = append(votes[digest], r)
					if len(votes[digest]) < need {
						continue
					}
					mergeContext(ctx, r.ctx)
					setHeader(ctx, headerQuorumModeUsed, QuorumModeConsensus)
					setHeader(ctx, headerQuorumArms, strconv.Itoa(len(groups)))
					setHeader(ctx, headerQuorumAchieved, strconv.Itoa(len(votes[digest])))
					setHeader(ctx, headerQuorumMajority, "true")
					record(ctx, "majority")
					if dissent := len(voted) - len(votes[digest]); dissent > 0 && rec != nil {
						// Counted, not scored: the dissenter's own attempt was
						// already scored on its merits, and whether being
						// outvoted should cost more is not decided.
						rec.RecordQuorumDissent(ctx.ServiceID, dissent)
					}
					return nil
				}
			}

			if answered == 0 && ctx.Ctx.Err() != nil {
				record(ctx, "timeout")
				return ctxDoneError(ctx.Ctx)
			}

			switch {
			case outcome != "":
			case ctx.Ctx.Err() != nil:
				outcome = "timeout"
			case mode == QuorumModeConsensus:
				outcome = "no_majority"
			default:
				outcome = "collect"
			}
			setHeader(ctx, headerQuorumModeUsed, QuorumModeCollect)
			setHeader(ctx, headerQuorumArms, strconv.Itoa(len(groups)))
			setHeader(ctx, headerQuorumAchieved, strconv.Itoa(answered))
			if mode == QuorumModeConsensus {
				setHeader(ctx, headerQuorumMajority, "false")
			}
			record(ctx, outcome)
			ctx.Response = &domain.Response{Body: collectEnvelope(groups, done), HTTPStatusCode: http.StatusOK}
			ctx.Err = nil
			return nil
		})
	}
}

// armResult is one quorum arm's outcome. index is the arm's position in the
// operator ranking, so the envelope lists answers in a stable order rather
// than in arrival order.
type armResult struct {
	index int
	ctx   *relay.Context
	err   error
}

// operatorGroup is one operator's endpoints, the candidate list of one arm.
type operatorGroup struct {
	operator  string
	endpoints domain.EndpointAddrList
	best      float64
}

// quorumSize reads Target-Quorum-Count: 3 when absent or unreadable, clamped
// to 3..9, and an even count rounded down so a majority is never a tie.
func quorumSize(header string) int {
	n, err := strconv.Atoi(strings.TrimSpace(header))
	if err != nil {
		n = defaultQuorum
	}
	n = min(max(n, defaultQuorum), maxQuorum)
	if n%2 == 0 {
		n--
	}
	return n
}

// topOperators groups the pool by operator and returns the n operators whose
// best endpoint scores highest, ties broken by name so the ranking is stable.
func topOperators(ctx *relay.Context, repSvc reputation.Service, pool domain.EndpointAddrList, n int) []operatorGroup {
	byOperator := map[string]*operatorGroup{}
	var groups []*operatorGroup
	for _, ep := range pool {
		op := ep.Operator()
		g, ok := byOperator[op]
		if !ok {
			g = &operatorGroup{operator: op, best: -1}
			byOperator[op] = g
			groups = append(groups, g)
		}
		g.endpoints = append(g.endpoints, ep)
		if repSvc != nil {
			if score, err := repSvc.GetScore(ctx.Ctx, ctx.ServiceID, ep, ctx.RPCType); err == nil && score > g.best {
				g.best = score
			}
		}
	}
	slices.SortFunc(groups, func(a, b *operatorGroup) int {
		switch {
		case a.best > b.best:
			return -1
		case a.best < b.best:
			return 1
		}
		return strings.Compare(a.operator, b.operator)
	})
	out := make([]operatorGroup, 0, min(n, len(groups)))
	for _, g := range groups[:min(n, len(groups))] {
		out = append(out, *g)
	}
	return out
}

// answerDigest is what a vote compares: the status and the body with the
// differences that carry no meaning taken out.
func answerDigest(resp *domain.Response) [sha256.Size]byte {
	status := resp.HTTPStatusCode
	if status == 0 {
		status = http.StatusOK
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%d\n", status)
	_, _ = h.Write(canonicalBody(resp.Body))
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

// canonicalBody re-encodes a JSON body with its keys sorted and the JSON-RPC
// envelope's id and jsonrpc members dropped, so two nodes that give the same
// answer in a different key order, or echo a different id, agree. Numbers are
// kept as written. A body that is not one JSON value is compared as bytes.
func canonicalBody(body []byte) []byte {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return body
	}
	if m, ok := v.(map[string]any); ok {
		delete(m, "id")
		delete(m, "jsonrpc")
	}
	out, err := json.Marshal(v)
	if err != nil {
		return body
	}
	return out
}

// quorumAnswer is one arm in the collect envelope. The operator is named so a
// caller can tell independent answers apart; the supplier address is not,
// for the reason SAGE sends no supplier headers (docs/path-compat.md).
type quorumAnswer struct {
	Operator   string          `json:"operator"`
	HTTPStatus int             `json:"http_status,omitempty"`
	Body       json.RawMessage `json:"body,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// collectEnvelope renders every arm's result, in ranking order:
// {"quorum": {"count": N, "responses": [...]}}. An arm still running when the
// wait ended is listed with an error rather than left out, so count and
// responses always agree.
func collectEnvelope(groups []operatorGroup, done []*armResult) []byte {
	answers := make([]quorumAnswer, len(groups))
	for i, g := range groups {
		a := quorumAnswer{Operator: g.operator}
		if r := done[i]; r == nil {
			a.Error = "no answer before the deadline"
		} else {
			if resp := r.ctx.Response; resp != nil {
				a.HTTPStatus = resp.HTTPStatusCode
				a.Body = rawJSON(resp.Body)
			}
			if r.err != nil {
				a.Error = domain.ClientMessage(r.err)
			}
		}
		answers[i] = a
	}
	b, err := json.Marshal(map[string]any{"quorum": map[string]any{"count": len(groups), "responses": answers}})
	if err != nil {
		return []byte(`{"quorum":{"error":"failed to render the quorum envelope"}}`)
	}
	return b
}

// rawJSON embeds a body as JSON when it is JSON and as a JSON string otherwise.
func rawJSON(body []byte) json.RawMessage {
	if len(body) == 0 {
		return nil
	}
	if json.Valid(body) {
		return body
	}
	s, _ := json.Marshal(string(body))
	return s
}

// singleBody is the request body a rejection echoes the id from, when there is
// exactly one.
func singleBody(ctx *relay.Context) []byte {
	if len(ctx.Payloads) == 1 {
		return ctx.Payloads[0].Bytes()
	}
	return nil
}

// setHeader stages a response header when the context has a writer.
func setHeader(ctx *relay.Context, key, value string) {
	if ctx.Writer != nil {
		ctx.Writer.SetHeader(key, value)
	}
}
