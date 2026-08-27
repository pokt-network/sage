package middleware

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/reputation"
)

func scoreCtx(ep string) *relay.Context {
	ctx := baseContext()
	ctx.ServiceID = "svc"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Endpoint = domain.EndpointAddr(ep)
	return ctx
}

func TestScore_RecordsSuccessForThisAttempt(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), HTTPStatusCode: 200}
		return nil
	})
	require.NoError(t, Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a")))
	got := rep.all()
	require.Len(t, got, 1)
	assert.Equal(t, domain.EndpointAddr("pokt1a-https://a"), got[0].Endpoint)
	assert.Equal(t, reputation.SignalSuccess, got[0].Signal.Type)
}

func TestScore_UsesHeuristicSeverity(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.HeuristicResult = &heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityFatal, Reason: "fabricated_response"}
		return nil
	})
	require.NoError(t, Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a")))
	got := rep.all()
	require.Len(t, got, 1)
	assert.Equal(t, reputation.SignalFatalError, got[0].Signal.Type)
	assert.Equal(t, "fabricated_response", got[0].Signal.Reason)
}

func TestScore_ClientAttributedErrorIsNoSignal(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.HeuristicResult = &heuristic.AnalysisResult{Attribution: heuristic.AttrClient, Reason: "client_cancelled"}
		return retryableErr("context canceled")
	})
	_ = Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a"))
	assert.Empty(t, rep.all())
}

// TestScore_ClientAttributedAnswerIsNoSignal covers the half of "nobody's
// signal" that carries no error at all: the supplier answered correctly and
// promptly that the chain does not have the method. Scoring that as a success
// is what docs/scoring.md §7.1 rules out — on beta it was 66% of batch
// traffic — and batch.go's comment already promises score drops it before Add.
func TestScore_ClientAttributedAnswerIsNoSignal(t *testing.T) {
	rep := &recordingRepService{}
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method trace_call does not exist/is not available"}}`)
	inner := Heuristic(newFlags(featureflag.FlagHeuristic))(relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: body, HTTPStatusCode: 200}
		return nil
	}))
	require.NoError(t, Score(newFlags(featureflag.FlagScoringV2, featureflag.FlagHeuristic), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a")))
	assert.Empty(t, rep.all(), "-32601 is the client's; the supplier answered it correctly")
}

// TestScore_AnalyzerSuccessVerdictIsStillScored pins the other side of that
// rule. heuristic's success verdict also carries AttrClient ("no action
// needed"), so score tells the two apart by the verdict's reason. If that
// string ever changes in heuristic, this test fails loudly rather than every
// success silently going unrecorded.
func TestScore_AnalyzerSuccessVerdictIsStillScored(t *testing.T) {
	rep := &recordingRepService{}
	inner := Heuristic(newFlags(featureflag.FlagHeuristic))(relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`), HTTPStatusCode: 200}
		return nil
	}))
	require.NoError(t, Score(newFlags(featureflag.FlagScoringV2, featureflag.FlagHeuristic), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a")))
	got := rep.all()
	require.Len(t, got, 1)
	assert.Equal(t, reputation.SignalSuccess, got[0].Signal.Type)

	// The two properties score depends on, asserted against the real analyzer
	// rather than a hand-built result: the success verdict is AttrClient (so
	// attribution alone cannot discriminate) and its reason is the exported
	// constant score compares with.
	verdict := heuristic.Analyze([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`), 200, domain.RPCTypeJSONRPC)
	assert.Equal(t, heuristic.AttrClient, verdict.Attribution,
		"heuristic's success verdict is AttrClient; score has to tell it from a real client error")
	assert.Equal(t, heuristic.ReasonSuccess, verdict.Reason,
		"score keys the difference on this reason")
}

// An endpoint that answered "block not found" ANSWERED. The verdict sets
// ShouldRetry, so Heuristic hands score a relay error, and grading that error
// literally would walk a whole pool of pruned nodes down for telling the
// truth. Recorded as a success carrying the analyzer's reason.
func TestScore_BlockchainAnswerScoresSuccess(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			Attribution: heuristic.AttrBlockchain, ShouldRetry: true, Reason: "blockchain_error",
		}
		return retryableErr("heuristic analysis suggests retry: blockchain_error")
	})
	_ = Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a"))
	got := rep.all()
	require.Len(t, got, 1)
	assert.Equal(t, reputation.SignalSuccess, got[0].Signal.Type)
	assert.Equal(t, "blockchain_error", got[0].Signal.Reason, "the analyzer's reason, not the retry wrapper's message")
}

// The boundary of the rule above: ShouldPenalize is still honoured, so a
// blockchain-attributed verdict the analyzer decided the endpoint is also at
// fault for is graded as the penalty it carries.
func TestScore_BlockchainVerdictThatPenalizesStillPenalizes(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.HeuristicResult = &heuristic.AnalysisResult{
			Attribution: heuristic.AttrBlockchain, ShouldPenalize: true,
			PenaltySeverity: heuristic.SeverityMajor, Reason: "chain_halted",
		}
		return retryableErr("chain halted")
	})
	_ = Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a"))
	got := rep.all()
	require.Len(t, got, 1)
	assert.Equal(t, reputation.SignalMajorError, got[0].Signal.Type)
}

func TestScore_AddsToSinkWhenPresent(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.HeuristicResult = &heuristic.AnalysisResult{ShouldPenalize: true, PenaltySeverity: heuristic.SeverityCritical, Reason: "empty_response"}
		return retryableErr("empty")
	})
	ctx := scoreCtx("pokt1a-https://a")
	ctx.ScoreSink = relay.NewScoreSink()
	_ = Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(ctx)
	assert.Empty(t, rep.all(), "with a sink the middleware must not record directly")
	n := 0
	ctx.ScoreSink.Flush(func(_ domain.EndpointAddr, _ domain.RPCType, sig reputation.Signal) {
		n++
		assert.Equal(t, reputation.SignalCriticalError, sig.Type)
	})
	assert.Equal(t, 1, n)
}

func TestScore_NoEndpointNoSignal(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(*relay.Context) error { return retryableErr("no endpoint") })
	_ = Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx(""))
	assert.Empty(t, rep.all())
}

func TestScore_FlagOffIsPassThrough(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{}`)}
		return nil
	})
	require.NoError(t, Score(newFlags(), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a")))
	assert.Empty(t, rep.all())
}

// A nil FlagStore reads as flag-off here, as it does in Observe and batch.
// The pair must agree: if score treated nil as ON while Observe treats it as
// OFF, a gateway wired with no flag store would score every request twice.
func TestScore_NilFlagStoreIsPassThrough(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{}`)}
		return nil
	})
	require.NoError(t, Score(nil, rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a")))
	assert.Empty(t, rep.all())
}

func TestScore_LatencyIsThisAttempts(t *testing.T) {
	rep := &recordingRepService{}
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		time.Sleep(20 * time.Millisecond)
		ctx.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{}`)}
		return nil
	})
	_ = Score(newFlags(featureflag.FlagScoringV2), rep)(inner).HandleRelay(scoreCtx("pokt1a-https://a"))
	require.Len(t, rep.all(), 1)
	assert.GreaterOrEqual(t, rep.all()[0].Signal.Latency, 20*time.Millisecond)
}
