package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// immutablePlugin answers IsImmutable with a fixed value.
type immutablePlugin struct {
	immutable bool
}

func (p *immutablePlugin) ParseRequest(context.Context, *http.Request, []byte, domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}

func (p *immutablePlugin) SelectEndpoints(eps domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return eps, nil
}

func (p *immutablePlugin) IsImmutable(domain.Payload) bool { return p.immutable }

// flatScores scores every endpoint the same; nothing else of the service is
// used by quorum.
type flatScores struct{ reputation.Service }

func (flatScores) GetScore(context.Context, domain.ServiceID, domain.EndpointAddr, domain.RPCType) (float64, error) {
	return 100, nil
}

// quorumRecorder keeps outcomes and the dissent total.
type quorumRecorder struct {
	mu       sync.Mutex
	outcomes []string
	dissent  int
}

func (r *quorumRecorder) RecordQuorum(_ domain.ServiceID, outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, outcome)
}

func (r *quorumRecorder) RecordQuorumDissent(_ domain.ServiceID, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dissent += n
}

// operatorAnswer is what the fake upstream says for one operator.
type operatorAnswer struct {
	body  string
	delay time.Duration
	err   error
}

// fakeArms stands in for the inner chain: it picks the first candidate, as
// select_endpoint would from a one-operator list, and answers per operator.
type fakeArms struct {
	answers map[string]operatorAnswer
	calls   atomic.Int32
	mu      sync.Mutex
	seen    []*relay.Context
}

func (f *fakeArms) HandleRelay(ctx *relay.Context) error {
	f.calls.Add(1)
	f.mu.Lock()
	f.seen = append(f.seen, ctx)
	f.mu.Unlock()
	if len(ctx.Endpoints) == 0 {
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"plain"}`), HTTPStatusCode: 200}
		return nil
	}
	ctx.Endpoint = ctx.Endpoints[0]
	a := f.answers[ctx.Endpoint.Operator()]
	time.Sleep(a.delay)
	if a.err != nil {
		return a.err
	}
	ctx.Response = &domain.Response{Body: []byte(a.body), HTTPStatusCode: 200}
	return nil
}

var quorumPool = domain.EndpointAddrList{
	"pokt1a-https://rpc.alpha.net",
	"pokt1b-https://rpc.beta.net",
	"pokt1c-https://rpc.gamma.net",
	"pokt1d-https://rpc2.alpha.net",
}

func quorumCtx(t *testing.T, headers map[string]string, plugin *immutablePlugin, payloads int) (*relay.Context, *httptest.ResponseRecorder, *relay.HTTPResponseWriter) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	w := relay.NewHTTPResponseWriter(rec)
	ctx := relay.NewContext(t.Context(), req, slog.New(slog.DiscardHandler), w)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	if plugin != nil {
		ctx.Plugin = plugin
	}
	for range payloads {
		ctx.Payloads = append(ctx.Payloads, domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_getTransactionReceipt","params":["0x1"]}`), domain.RPCTypeJSONRPC, "eth_getTransactionReceipt"))
	}
	return ctx, rec, w
}

// headersOf commits the staged response and returns its headers.
func headersOf(t *testing.T, w *relay.HTTPResponseWriter, rec *httptest.ResponseRecorder) http.Header {
	t.Helper()
	require.NoError(t, w.Write(nil))
	return rec.Header()
}

func TestQuorum_NoHeadersPassesThrough(t *testing.T) {
	arms := &fakeArms{}
	ctx, rec, w := quorumCtx(t, nil, &immutablePlugin{immutable: true}, 1)
	require.NoError(t, Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, nil)(arms).HandleRelay(ctx))
	assert.Equal(t, int32(1), arms.calls.Load())
	assert.False(t, arms.seen[0].QuorumArm)
	assert.Empty(t, headersOf(t, w, rec).Get("X-Quorum-Count"))
}

func TestQuorum_SkipsAndSaysWhy(t *testing.T) {
	for name, tc := range map[string]struct {
		flags    *mockFlags
		payloads int
		want     string
	}{
		"flag off": {newFlags(), 1, "disabled"},
		"batch":    {newFlags(featureflag.FlagQuorum), 2, "batch"},
	} {
		t.Run(name, func(t *testing.T) {
			arms := &fakeArms{}
			rec := &quorumRecorder{}
			ctx, httpRec, w := quorumCtx(t, map[string]string{"Target-Quorum-Count": "3"}, &immutablePlugin{immutable: true}, tc.payloads)
			require.NoError(t, Quorum(tc.flags, stubProvider{quorumPool}, flatScores{}, rec)(arms).HandleRelay(ctx))
			assert.Equal(t, int32(1), arms.calls.Load(), "answered as an ordinary request")
			assert.False(t, arms.seen[0].QuorumArm, "retry and hedge must still apply")
			assert.Equal(t, tc.want, headersOf(t, w, httpRec).Get("X-Quorum-Skipped"))
			assert.Equal(t, []string{"skipped_" + tc.want}, rec.outcomes)
		})
	}
}

func TestQuorum_InvalidModeIsRefused(t *testing.T) {
	arms := &fakeArms{}
	ctx, rec, _ := quorumCtx(t, map[string]string{"Target-Quorum-Mode": "vote"}, &immutablePlugin{immutable: true}, 1)
	err := Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, nil)(arms).HandleRelay(ctx)
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Zero(t, arms.calls.Load())
}

// Two operators agree once the id and key order are ignored; the third is
// outvoted. The agreed answer is the response, and each arm ran on one
// operator's endpoints only.
func TestQuorum_ConsensusReturnsTheMajority(t *testing.T) {
	arms := &fakeArms{answers: map[string]operatorAnswer{
		"gamma.net": {body: `{"jsonrpc":"2.0","id":7,"result":{"status":"0x0"}}`},
		"alpha.net": {body: `{"jsonrpc":"2.0","id":7,"result":{"status":"0x1","gas":"0x5"}}`, delay: 20 * time.Millisecond},
		"beta.net":  {body: `{"result":{"gas":"0x5","status":"0x1"},"id":99,"jsonrpc":"2.0"}`, delay: 20 * time.Millisecond},
	}}
	rec := &quorumRecorder{}
	ctx, httpRec, w := quorumCtx(t, map[string]string{"Target-Quorum-Count": "3"}, &immutablePlugin{immutable: true}, 1)
	require.NoError(t, Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, rec)(arms).HandleRelay(ctx))

	require.NotNil(t, ctx.Response)
	assert.Contains(t, string(ctx.Response.Body), `"status":"0x1"`)
	h := headersOf(t, w, httpRec)
	assert.Equal(t, "consensus", h.Get("X-Quorum-Mode"))
	assert.Equal(t, "3", h.Get("X-Quorum-Count"))
	assert.Equal(t, "2", h.Get("X-Quorum-Achieved"))
	assert.Equal(t, "true", h.Get("X-Quorum-Majority"))
	assert.Equal(t, []string{"majority"}, rec.outcomes)
	assert.Equal(t, 1, rec.dissent)

	arms.mu.Lock()
	defer arms.mu.Unlock()
	require.Len(t, arms.seen, 3)
	for _, arm := range arms.seen {
		assert.True(t, arm.QuorumArm)
		assert.Len(t, arm.Endpoints.Operators(), 1, "an arm's candidates are one operator")
	}
}

// Every arm answers differently, or fails: the envelope carries all three,
// and the response says no majority formed.
func TestQuorum_NoMajorityAnswersWithTheEnvelope(t *testing.T) {
	arms := &fakeArms{answers: map[string]operatorAnswer{
		"alpha.net": {body: `{"jsonrpc":"2.0","id":7,"result":"0xa"}`},
		"beta.net":  {body: `{"jsonrpc":"2.0","id":7,"result":"0xb"}`},
		"gamma.net": {err: errors.New("dial tcp: refused")},
	}}
	rec := &quorumRecorder{}
	ctx, httpRec, w := quorumCtx(t, map[string]string{"Target-Quorum-Mode": "consensus"}, &immutablePlugin{immutable: true}, 1)
	require.NoError(t, Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, rec)(arms).HandleRelay(ctx))

	var env struct {
		Quorum struct {
			Count     int `json:"count"`
			Responses []struct {
				Operator string          `json:"operator"`
				Body     json.RawMessage `json:"body"`
				Error    string          `json:"error"`
			} `json:"responses"`
		} `json:"quorum"`
	}
	require.NoError(t, json.Unmarshal(ctx.Response.Body, &env))
	assert.Equal(t, 3, env.Quorum.Count)
	require.Len(t, env.Quorum.Responses, 3)
	errs := 0
	for _, r := range env.Quorum.Responses {
		assert.NotEmpty(t, r.Operator)
		if r.Error != "" {
			errs++
		}
	}
	assert.Equal(t, 1, errs)
	h := headersOf(t, w, httpRec)
	assert.Equal(t, "collect", h.Get("X-Quorum-Mode"))
	assert.Equal(t, "false", h.Get("X-Quorum-Majority"))
	assert.Equal(t, "2", h.Get("X-Quorum-Achieved"))
	assert.Equal(t, []string{"no_majority"}, rec.outcomes)
}

// A consensus request for something that can change between blocks is not
// voted on: it is answered in collect mode, and the header says so.
func TestQuorum_ConsensusOnAMutableRequestCollects(t *testing.T) {
	arms := &fakeArms{answers: map[string]operatorAnswer{
		"alpha.net": {body: `{"result":"0x1"}`},
		"beta.net":  {body: `{"result":"0x1"}`},
		"gamma.net": {body: `{"result":"0x1"}`},
	}}
	rec := &quorumRecorder{}
	ctx, httpRec, w := quorumCtx(t, map[string]string{"Target-Quorum-Count": "3"}, &immutablePlugin{immutable: false}, 1)
	require.NoError(t, Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, rec)(arms).HandleRelay(ctx))

	assert.Contains(t, string(ctx.Response.Body), `"quorum"`)
	h := headersOf(t, w, httpRec)
	assert.Equal(t, "collect", h.Get("X-Quorum-Mode"))
	assert.Empty(t, h.Get("X-Quorum-Majority"))
	assert.Equal(t, []string{"collect_not_immutable"}, rec.outcomes)
}

// An arm whose operator's endpoints were pruned is handed the whole pool by
// select_endpoint and may land on an operator that already voted. It must not
// vote twice.
func TestQuorum_OneVotePerOperator(t *testing.T) {
	var n atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		// Every arm lands on alpha, as if the other operators' lists were empty.
		ctx.Endpoint = "pokt1a-https://rpc.alpha.net"
		if n.Add(1) > 1 {
			time.Sleep(10 * time.Millisecond)
		}
		ctx.Response = &domain.Response{Body: []byte(`{"result":"same"}`), HTTPStatusCode: 200}
		return nil
	})
	ctx, httpRec, w := quorumCtx(t, map[string]string{"Target-Quorum-Count": "3"}, &immutablePlugin{immutable: true}, 1)
	require.NoError(t, Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, nil)(inner).HandleRelay(ctx))
	assert.Equal(t, "false", headersOf(t, w, httpRec).Get("X-Quorum-Majority"), "three answers from one operator are one vote")
}

func TestQuorumSize(t *testing.T) {
	for header, want := range map[string]int{
		"": 3, "abc": 3, "1": 3, "3": 3, "4": 3, "5": 5, "8": 7, "9": 9, "20": 9, "-5": 3,
	} {
		assert.Equal(t, want, quorumSize(header), "Target-Quorum-Count %q", header)
	}
}

// Never more arms than operators, and the pool's siblings count as one.
func TestTopOperators_ClampsToOperatorsAndGroupsSiblings(t *testing.T) {
	ctx, _, _ := quorumCtx(t, nil, nil, 1)
	groups := topOperators(ctx, flatScores{}, quorumPool, 9)
	require.Len(t, groups, 3)
	for _, g := range groups {
		if g.operator == "alpha.net" {
			assert.Len(t, g.endpoints, 2)
		}
	}
}

func TestCanonicalBody(t *testing.T) {
	a := canonicalBody([]byte(`{"jsonrpc":"2.0","id":1,"result":{"b":1,"a":12345678901234567890}}`))
	b := canonicalBody([]byte(`{"result":{"a":12345678901234567890,"b":1},"id":"x","jsonrpc":"2.0"}`))
	assert.Equal(t, string(a), string(b))
	assert.Equal(t, "not json", string(canonicalBody([]byte("not json"))))
}

// The deadline ends the wait; arms that answered are in the envelope and the
// rest are named as not having answered.
func TestQuorum_DeadlineAnswersWithWhatArrived(t *testing.T) {
	arms := &fakeArms{answers: map[string]operatorAnswer{
		"alpha.net": {body: `{"result":"0xa"}`},
		"beta.net":  {body: `{"result":"0xb"}`, delay: time.Second},
		"gamma.net": {body: `{"result":"0xc"}`, delay: time.Second},
	}}
	rec := &quorumRecorder{}
	ctx, _, _ := quorumCtx(t, map[string]string{"Target-Quorum-Mode": "collect"}, &immutablePlugin{immutable: true}, 1)
	deadline, cancel := context.WithTimeout(ctx.Ctx, 50*time.Millisecond)
	defer cancel()
	ctx.Ctx = deadline
	require.NoError(t, Quorum(newFlags(featureflag.FlagQuorum), stubProvider{quorumPool}, flatScores{}, rec)(arms).HandleRelay(ctx))
	assert.Contains(t, string(ctx.Response.Body), "no answer before the deadline")
	assert.Equal(t, []string{"timeout"}, rec.outcomes)
}
