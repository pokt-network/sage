package middleware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

// headPlugin names a head in every eth_blockNumber answer, 40 blocks behind
// and stale.
type headPlugin struct{ normPlugin }

func (headPlugin) HeadLag(payload domain.Payload, _ []byte, _ time.Time) (uint64, bool, bool) {
	if payload.Method() != "eth_blockNumber" {
		return 0, false, false
	}
	return 40, true, true
}

// The head-lag recorder sees each 200 answer the plugin reads a head from,
// with the serving party and the method, whatever the heuristic flag says,
// and the verdict is untouched.
func TestHeuristic_RecordsHeadLag(t *testing.T) {
	type rec struct {
		party, method string
		lag           uint64
		stale         bool
	}
	var got []rec
	mw := Heuristic(newFlags(), nil, WithHeadLag(func(_ domain.ServiceID, party, method string, lag uint64, stale bool) {
		got = append(got, rec{party, method, lag, stale})
	}))
	run := func(method string, status int) {
		ctx := baseContext()
		ctx.Plugin = headPlugin{}
		ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"method":"`+method+`"}`), domain.RPCTypeJSONRPC, method)}
		h := relay.HandlerFunc(func(c *relay.Context) error {
			c.Endpoint = "s1-https://r1.cache.example.xyz"
			c.Response = &domain.Response{HTTPStatusCode: status, Body: []byte(`{"result":"0x1"}`)}
			return nil
		})
		if err := mw(h).HandleRelay(ctx); err != nil {
			t.Fatal(err)
		}
		if ctx.HeuristicResult != nil {
			t.Errorf("%s: heuristic flag off, verdict must stay unset", method)
		}
	}
	run("eth_blockNumber", 200)
	run("eth_call", 200)
	run("eth_blockNumber", 502)
	if len(got) != 1 || got[0] != (rec{"example.xyz", "eth_blockNumber", 40, true}) {
		t.Fatalf("recorded %v, want one eth_blockNumber record for example.xyz, lag 40, stale", got)
	}
}

// lagPlugin names a head in every eth_blockNumber answer: the lag is read
// from the answer's "lag" field so each attempt can say its own.
type lagPlugin struct{ normPlugin }

func (lagPlugin) HeadLag(payload domain.Payload, body []byte, _ time.Time) (uint64, bool, bool) {
	if payload.Method() != "eth_blockNumber" {
		return 0, false, false
	}
	lag := gjson.GetBytes(body, "lag").Uint()
	return lag, lag > 5, true
}

func headCtx() *relay.Context {
	ctx := baseContext()
	ctx.Plugin = lagPlugin{}
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber")}
	return ctx
}

func answerWithLag(lag int) []byte {
	return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":"0x10","lag":%d}`, lag))
}

// Behind stale_response a stale head answer becomes a retried major
// supplier verdict; a fresh one, or any answer with the flag off, is
// untouched and only measured.
func TestHeuristic_StaleResponse(t *testing.T) {
	run := func(flags *mockFlags, lag int) (*relay.Context, int, error) {
		recorded := 0
		ctx := headCtx()
		mw := Heuristic(flags, nil, WithHeadLag(func(domain.ServiceID, string, string, uint64, bool) { recorded++ }))
		err := mw(relay.HandlerFunc(func(c *relay.Context) error {
			c.Endpoint = "s1-https://r1.cache.example.xyz"
			c.Response = &domain.Response{HTTPStatusCode: 200, Body: answerWithLag(lag)}
			return nil
		})).HandleRelay(ctx)
		return ctx, recorded, err
	}
	ctx, rec, err := run(newFlags("heuristic", "stale_response"), 40)
	if ctx.HeuristicResult == nil || ctx.HeuristicResult.Reason != heuristic.ReasonStaleResponse ||
		ctx.HeuristicResult.PenaltySeverity != heuristic.SeverityMajor || ctx.HeuristicResult.HeadLag != 40 ||
		!domain.IsRetryable(err) || rec != 1 {
		t.Fatalf("stale: verdict %+v err %v recorded %d, want a retried major stale_response", ctx.HeuristicResult, err, rec)
	}
	ctx, _, err = run(newFlags("heuristic", "stale_response"), 1)
	if err != nil || !ctx.HeuristicResult.IsSuccess() {
		t.Fatalf("fresh: verdict %+v err %v, want success", ctx.HeuristicResult, err)
	}
	ctx, rec, err = run(newFlags("heuristic"), 40)
	if err != nil || !ctx.HeuristicResult.IsSuccess() || rec != 1 {
		t.Fatalf("flag off: verdict %+v err %v recorded %d, want success and still measured", ctx.HeuristicResult, err, rec)
	}
}

// When every attempt answers stale the freshest answer is delivered, from
// whichever attempt gave it, and a stale answer outlives a later failure.
func TestRetry_DeliversTheFreshestStaleAnswer(t *testing.T) {
	flags := newFlags("retry", "heuristic", "stale_response")
	run := func(lags ...int) (*relay.Context, error) {
		ctx := headCtx()
		attempt := 0
		send := relay.HandlerFunc(func(c *relay.Context) error {
			lag := lags[attempt]
			attempt++
			c.Endpoint = c.Endpoints[0]
			if lag < 0 {
				return domain.NewRelayError(domain.ErrTransport, "timeout", context.DeadlineExceeded, true)
			}
			c.Response = &domain.Response{HTTPStatusCode: 200, Body: answerWithLag(lag)}
			return nil
		})
		h := Retry(flags, retryCfg(len(lags)-1, 0))(Heuristic(flags, nil)(send))
		err := h.HandleRelay(ctx)
		return ctx, err
	}
	for _, tc := range []struct {
		name string
		lags []int
		want uint64
	}{
		{"fresher first", []int{10, 40}, 10},
		{"fresher last", []int{40, 10}, 10},
		{"stale then a timeout", []int{40, -1}, 40},
	} {
		ctx, _ := run(tc.lags...)
		if ctx.Response == nil || gjson.GetBytes(ctx.Response.Body, "lag").Uint() != tc.want {
			t.Errorf("%s: delivered %v, want the answer %d behind", tc.name, ctx.Response, tc.want)
		}
	}
}

// refusingPlugin refines every verdict into a refusal worded as a prune.
type refusingPlugin struct{ normPlugin }

func (refusingPlugin) RefineVerdict(_ domain.Payload, _ heuristic.AnalysisResult) (heuristic.AnalysisResult, bool) {
	return heuristic.RefusedRecent("claims block gone"), true
}

// A refused_recent verdict is a method mark only behind method_block_refusal;
// the verdict itself (supplier, major, retried) does not wait for the flag.
func TestHeuristic_RefusalMarksBehindItsFlag(t *testing.T) {
	run := func(flags *mockFlags) *heuristic.AnalysisResult {
		ctx := baseContext()
		ctx.Plugin = refusingPlugin{}
		ctx.RPCType = domain.RPCTypeJSONRPC
		ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{}]}`), domain.RPCTypeJSONRPC, "eth_getLogs")}
		_ = Heuristic(flags, nil)(relay.HandlerFunc(func(c *relay.Context) error {
			c.Endpoint = "s1-https://r1.cache.example.xyz"
			c.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"requested height has been pruned"}}`)}
			return nil
		})).HandleRelay(ctx)
		return ctx.HeuristicResult
	}
	if r := run(newFlags("heuristic", "method_block_refusal")); r == nil || r.Reason != heuristic.ReasonRefusedRecent || !r.MethodBlocking {
		t.Fatalf("flag on: %+v, want a method-blocking refusal", r)
	}
	if r := run(newFlags("heuristic")); r == nil || r.Reason != heuristic.ReasonRefusedRecent || r.MethodBlocking || !r.ShouldPenalize {
		t.Fatalf("flag off: %+v, want a scored refusal and no mark", r)
	}
}
