package middleware

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

// scriptedBackend is the fake below select_endpoint: a queue of endpoints to
// hand out in order, and a per-endpoint response script.
type scriptedBackend struct {
	mu        sync.Mutex
	endpoints []domain.EndpointAddr
	responses map[domain.EndpointAddr]func(ctx *relay.Context) error
}

// selectMW pops the next scripted endpoint into ctx.Endpoint, the way
// SelectEndpoint would, or fails when the script is exhausted.
func (b *scriptedBackend) selectMW(next relay.Handler) relay.Handler {
	return relay.HandlerFunc(func(ctx *relay.Context) error {
		b.mu.Lock()
		if len(b.endpoints) == 0 {
			b.mu.Unlock()
			return nonRetryableErr("script exhausted")
		}
		ctx.Endpoint = b.endpoints[0]
		b.endpoints = b.endpoints[1:]
		b.mu.Unlock()
		if ctx.SelectedEndpoint != nil {
			ep := ctx.Endpoint
			ctx.SelectedEndpoint.Store(&ep)
		}
		return next.HandleRelay(ctx)
	})
}

func (b *scriptedBackend) sendMW(next relay.Handler) relay.Handler {
	return relay.HandlerFunc(func(ctx *relay.Context) error {
		b.mu.Lock()
		respond, ok := b.responses[ctx.Endpoint]
		b.mu.Unlock()
		if !ok {
			return next.HandleRelay(ctx)
		}
		return respond(ctx)
	})
}

func okJSON(ctx *relay.Context) error {
	ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`), HTTPStatusCode: 200}
	return nil
}

func emptyBody(ctx *relay.Context) error {
	ctx.Response = &domain.Response{Body: []byte(``), HTTPStatusCode: 200}
	return nil
}

func fabricated(ctx *relay.Context) error {
	ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x10","error":{"code":-32000,"message":"x"}}`), HTTPStatusCode: 200}
	return nil
}

// blockNotFound is the chain answering honestly about state it does not have:
// heuristic.classifyServerError grades a -32000 carrying this wording
// AttrBlockchain, ShouldRetry, and explicitly NOT ShouldPenalize.
func blockNotFound(ctx *relay.Context) error {
	ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"block not found"}}`), HTTPStatusCode: 200}
	return nil
}

// noResponse is an attempt that produces no verdict at all: Heuristic returns
// early when there is no body to analyse and no error to grade. It exists so a
// test can tell "this attempt's own verdict" from "the previous attempt's,
// still on the context".
func noResponse(ctx *relay.Context) error {
	ctx.Response = nil
	return nil
}

func methodNotFound(ctx *relay.Context) error {
	ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method trace_call does not exist/is not available"}}`), HTTPStatusCode: 200}
	return nil
}

// connectionRefused is the shape the real path produces, not a hand-built
// string: relayer.go wraps whatever the HTTP client returned as the RelayError's
// cause, and heuristic.AnalyzeTransportError grades a refused dial by
// unwrapping to the *net.OpError (see protocol/shannon/transport_errors_test.go,
// which drives the real sendHTTP against a dead port for exactly this reason).
// A RelayError carrying only the words "connection refused" in its message
// grades as an unclassified minor error, which is a fake nobody would ship.
func connectionRefused(*relay.Context) error {
	return domain.NewRelayError(domain.ErrEndpoint, "sendHTTP",
		&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, true)
}

// buildChain wires batch → observe → retry → hedge → [scripted select] →
// score → heuristic → [scripted send], with a real Heuristic middleware.
func buildChain(rep reputation.Service, b *scriptedBackend, hedgeDelay time.Duration, maxRetries int) relay.Handler {
	flags := newFlags(featureflag.FlagScoringV2, featureflag.FlagRetry, featureflag.FlagHedge, featureflag.FlagHeuristic)
	retryCfg := func(domain.ServiceID) config.RetryConfig {
		return config.RetryConfig{Enabled: true, MaxRetries: maxRetries, HedgeDelay: hedgeDelay}
	}
	var h relay.Handler = relay.HandlerFunc(okJSON)
	h = b.sendMW(h)
	h = Heuristic(flags)(h)
	h = Score(flags, rep)(h)
	h = b.selectMW(h)
	h = Hedge(flags, retryCfg)(h)
	h = Retry(flags, retryCfg)(h)
	h = Observe(flags, nil, rep, nil)(h)
	h = Batch(0, 0, flags, rep)(h)
	return h
}

func chainCtx(payloads int) *relay.Context {
	ctx := baseContext()
	ctx.ServiceID = "svc"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Payloads = make([]domain.Payload, payloads)
	for i := range ctx.Payloads {
		ctx.Payloads[i] = domain.NewPayload(
			[]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`),
			domain.RPCTypeJSONRPC, "eth_blockNumber")
	}
	return ctx
}

const (
	epA = domain.EndpointAddr("pokt1a-https://a.example")
	epB = domain.EndpointAddr("pokt1b-https://b.example")
)

func signalsByEndpoint(rep *recordingRepService) map[domain.EndpointAddr][]reputation.SignalType {
	out := map[domain.EndpointAddr][]reputation.SignalType{}
	for _, s := range rep.all() {
		out[s.Endpoint] = append(out[s.Endpoint], s.Signal.Type)
	}
	return out
}

func TestChain_RetryLoserIsScored(t *testing.T) {
	rep := &recordingRepService{}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epB},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: emptyBody, epB: okJSON}}
	require.NoError(t, buildChain(rep, b, 0, 2).HandleRelay(chainCtx(1)))
	got := signalsByEndpoint(rep)
	assert.Equal(t, []reputation.SignalType{reputation.SignalCriticalError}, got[epA], "A's empty body is A's critical")
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epB])
}

func TestChain_StaleVerdictCannotReachNextAttempt(t *testing.T) {
	rep := &recordingRepService{}
	// B produces NO verdict of its own — no response, no error, so Heuristic
	// returns without storing one. That is the only shape that can catch the
	// stale-verdict bug: every error path writes a fresh transport verdict,
	// which would mask a missing reset in retry.go. With the reset
	// (retry.go: ctx.HeuristicResult = nil) B is graded by the status
	// fallback; without it B inherits A's empty_response and is scored
	// critical for something A did.
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epB},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: emptyBody, epB: noResponse}}
	require.NoError(t, buildChain(rep, b, 0, 2).HandleRelay(chainCtx(1)))
	got := signalsByEndpoint(rep)
	assert.Equal(t, []reputation.SignalType{reputation.SignalCriticalError}, got[epA])
	require.Len(t, got[epB], 1)
	assert.Equal(t, reputation.SignalMinorError, got[epB][0], "B has no verdict of its own; the status fallback grades it")

	var bReason string
	for _, s := range rep.all() {
		if s.Endpoint == epB {
			bReason = s.Signal.Reason
		}
	}
	assert.NotContains(t, bReason, "empty_response", "that verdict is A's")
	assert.Equal(t, "relay_error", bReason)
}

// A blockchain-attributed answer is the endpoint ANSWERING (docs/scoring.md
// §2.1): it told the truth about a block it does not hold. It still retries
// elsewhere, and the endpoint that answered must not pay for it — which it
// would, as a minor error carrying "heuristic analysis suggests retry: …",
// if the relay error the retry needs were graded literally.
func TestChain_BlockchainErrorScoresSuccessAndRetries(t *testing.T) {
	rep := &recordingRepService{}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epB},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: blockNotFound, epB: okJSON}}
	require.NoError(t, buildChain(rep, b, 0, 2).HandleRelay(chainCtx(1)))
	got := signalsByEndpoint(rep)
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epA],
		"A answered honestly about state the chain does not have")
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epB],
		"and the request still retried onto B")

	var aReason string
	for _, s := range rep.all() {
		if s.Endpoint == epA {
			aReason = s.Signal.Reason
		}
	}
	assert.Equal(t, "blockchain_error", aReason, "the analyzer's own verdict")
	assert.NotContains(t, aReason, "heuristic analysis suggests retry",
		"the retry wrapper's message is not a reputation reason")
}

func TestChain_HedgeLoserIsScored(t *testing.T) {
	rep := &recordingRepService{}
	slowThenOK := func(ctx *relay.Context) error {
		time.Sleep(150 * time.Millisecond)
		return okJSON(ctx)
	}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epB},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: slowThenOK, epB: okJSON}}
	require.NoError(t, buildChain(rep, b, 20*time.Millisecond, 0).HandleRelay(chainCtx(1)))
	// Wait for the losing arm to finish and score.
	require.Eventually(t, func() bool { return len(rep.all()) == 2 }, 5*time.Second, 5*time.Millisecond)
	got := signalsByEndpoint(rep)
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epA])
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epB])
}

func TestChain_BothHedgeArmsFailBothScored(t *testing.T) {
	rep := &recordingRepService{}
	slowRefused := func(ctx *relay.Context) error {
		time.Sleep(40 * time.Millisecond)
		return connectionRefused(ctx)
	}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epB},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: slowRefused, epB: connectionRefused}}
	require.Error(t, buildChain(rep, b, 5*time.Millisecond, 0).HandleRelay(chainCtx(1)))
	require.Eventually(t, func() bool { return len(rep.all()) == 2 }, 5*time.Second, 5*time.Millisecond)
	got := signalsByEndpoint(rep)
	assert.Len(t, got[epA], 1)
	assert.Len(t, got[epB], 1)
	assert.Equal(t, reputation.SignalCriticalError, got[epA][0])
	assert.Equal(t, reputation.SignalCriticalError, got[epB][0])
}

func TestChain_BatchOneFabricatedItemIsOneFatal(t *testing.T) {
	rep := &recordingRepService{}
	var n int32
	mostlyOK := func(ctx *relay.Context) error {
		if atomic.AddInt32(&n, 1) == 7 {
			return fabricated(ctx)
		}
		return okJSON(ctx)
	}
	eps := make([]domain.EndpointAddr, 20)
	for i := range eps {
		eps[i] = epA
	}
	b := &scriptedBackend{endpoints: eps, responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: mostlyOK}}
	require.NoError(t, buildChain(rep, b, 0, 0).HandleRelay(chainCtx(20)))
	got := rep.all()
	require.Len(t, got, 1, "20 payloads, one endpoint, one signal")
	assert.Equal(t, reputation.SignalFatalError, got[0].Signal.Type)
	assert.Equal(t, "fabricated_response", got[0].Signal.Reason)
}

func TestChain_BatchOnlyClientErrorsScoresNothing(t *testing.T) {
	rep := &recordingRepService{}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epA, epA},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: methodNotFound}}
	require.NoError(t, buildChain(rep, b, 0, 0).HandleRelay(chainCtx(3)))
	assert.Empty(t, rep.all(), "-32601 is the client's; three of them are still nobody's signal")
}

func TestChain_BatchClientErrorsPlusSuccessIsOneSuccess(t *testing.T) {
	rep := &recordingRepService{}
	var n int32
	mixed := func(ctx *relay.Context) error {
		if atomic.AddInt32(&n, 1)%2 == 0 {
			return methodNotFound(ctx)
		}
		return okJSON(ctx)
	}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epA, epA, epA},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: mixed}}
	require.NoError(t, buildChain(rep, b, 0, 0).HandleRelay(chainCtx(4)))
	got := rep.all()
	require.Len(t, got, 1)
	assert.Equal(t, reputation.SignalSuccess, got[0].Signal.Type)
}

func TestChain_CancelledAttemptScoresNothing(t *testing.T) {
	rep := &recordingRepService{}
	cancelling := func(*relay.Context) error {
		return context.Canceled
	}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: cancelling}}
	ctx := chainCtx(1)
	c, cancel := context.WithCancel(ctx.Ctx)
	cancel()
	ctx.Ctx = c
	_ = buildChain(rep, b, 0, 0).HandleRelay(ctx)
	assert.Empty(t, rep.all())
}

func TestChain_FlagOffObserveScoresOncePerRequest(t *testing.T) {
	rep := &recordingRepService{}
	b := &scriptedBackend{endpoints: []domain.EndpointAddr{epA, epB},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{epA: emptyBody, epB: okJSON}}
	flags := newFlags(featureflag.FlagRetry, featureflag.FlagHeuristic) // no scoring_v2
	retryCfg := func(domain.ServiceID) config.RetryConfig { return config.RetryConfig{Enabled: true, MaxRetries: 2} }
	h := b.sendMW(relay.HandlerFunc(okJSON))
	h = Heuristic(flags)(h)
	h = Score(flags, rep)(h)
	h = b.selectMW(h)
	h = Retry(flags, retryCfg)(h)
	h = Observe(flags, nil, rep, nil)(h)
	require.NoError(t, h.HandleRelay(chainCtx(1)))
	got := rep.all()
	require.Len(t, got, 1, "the pre-v2 path: one signal per client request (the revert-check for the feature)")
	assert.Equal(t, epB, got[0].Endpoint)
}

// epC exists only for the stray-hedge slot below: a client-attributed answer
// scores nothing, so an arm that pops it cannot change the signal count.
const epC = domain.EndpointAddr("pokt1c-https://c.example")

// A hedge arm inside a batch sub-relay outlives the batch: hedge runs its
// losing arm to completion on a context detached with context.WithoutCancel,
// and the batch flushes its ScoreSink as soon as every payload has an answer.
// The sink closes onto the flush function for exactly this, so the late arm is
// forwarded rather than collapsed into a map nobody reads.
//
// A is slow and B is fast, so one payload hedges off A onto B and returns while
// A is still in flight. Both are successes, so the only way the count lands on
// two is: B's collapsed signal at flush time, and A's forwarded afterwards.
func TestChain_HedgeLoserInsideBatchOutlivesFlush(t *testing.T) {
	rep := &recordingRepService{}
	slowThenOK := func(ctx *relay.Context) error {
		time.Sleep(150 * time.Millisecond)
		return okJSON(ctx)
	}
	// One A, then Bs: the two payloads' primary arms take A and B, and the
	// hedge that fires off A takes a B. The last slot is epC, which only a
	// spuriously-scheduled second hedge reaches and which scores nothing.
	b := &scriptedBackend{
		endpoints: []domain.EndpointAddr{epA, epB, epB, epC},
		responses: map[domain.EndpointAddr]func(*relay.Context) error{
			epA: slowThenOK, epB: okJSON, epC: methodNotFound,
		},
	}
	require.NoError(t, buildChain(rep, b, 5*time.Millisecond, 0).HandleRelay(chainCtx(2)))

	// The batch answered on B and flushed; A's arm is still sleeping.
	require.Len(t, rep.all(), 1, "the batch flushed before the slow arm returned")

	require.Eventually(t, func() bool { return len(rep.all()) == 2 }, 2*time.Second, 5*time.Millisecond,
		"the post-flush arm was never forwarded: got %v", signalsByEndpoint(rep))
	got := signalsByEndpoint(rep)
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epA],
		"one signal for the arm that outlived the flush")
	assert.Equal(t, []reputation.SignalType{reputation.SignalSuccess}, got[epB],
		"two payloads on B are still one signal for B")
	assert.Len(t, rep.all(), 2, "two endpoints served the batch, two signals")
}
