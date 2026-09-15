package middleware

import (
	"errors"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

type limitRec struct{ outcomes []string }

func (r *limitRec) RecordRetry(domain.ServiceID, string) {}
func (r *limitRec) RecordRetryResolution(_ domain.ServiceID, reason, outcome string) {
	r.outcomes = append(r.outcomes, reason+":"+outcome)
}

// After a rate limit the retry goes to another operator with a vouched
// endpoint, never a sibling behind the same limiter; with no such operator
// it does not happen and the node's own answer is delivered.
func TestRetry_RateLimitRetriesOnlyOnAnotherOperator(t *testing.T) {
	a1 := domain.EndpointAddr("pokt1a-https://a1.opa.example")
	a2 := domain.EndpointAddr("pokt1b-https://a2.opa.example")
	b := domain.EndpointAddr("pokt1c-https://b.opb.example")
	limitedAnswer := &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rate limit exceeded"}}`)}

	run := func(pool domain.EndpointAddrList, rep *stubRepService) ([]domain.EndpointAddr, *relay.Context, *limitRec, error) {
		var attempts []domain.EndpointAddr
		rec := &limitRec{}
		h := relay.HandlerFunc(func(c *relay.Context) error {
			ep := c.Endpoints[0]
			c.Endpoint = ep
			attempts = append(attempts, ep)
			if ep.Operator() == "opa.example" {
				c.Response = limitedAnswer
				c.HeuristicResult = &heuristic.AnalysisResult{ShouldRetry: true, Attribution: heuristic.AttrSupplier, Reason: "rate_limited"}
				c.Err = domain.NewRelayError(domain.ErrEndpoint, "heuristic analysis suggests retry: rate_limited", domain.ErrRetryVerdict, true)
				return c.Err
			}
			c.Response = &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`)}
			return nil
		})
		ctx := baseContext()
		ctx.Endpoints = pool
		err := RetryWithRecorder(newFlags("retry"), retryCfg(2, 0), rec, RetryVouchedBy(rep))(h).HandleRelay(ctx)
		return attempts, ctx, rec, err
	}

	// Another operator is vouched: the retry goes there, not to the sibling.
	attempts, _, _, err := run(domain.EndpointAddrList{a1, a2, b}, &stubRepService{scores: map[domain.EndpointAddr]float64{b: 60}})
	if err != nil || len(attempts) != 2 || attempts[1] != b {
		t.Fatalf("attempts %v err %v; want a1 then b", attempts, err)
	}

	// Only the rate-limited operator, or another that is not vouched: stop,
	// and deliver the node's own answer.
	for name, tc := range map[string]struct {
		pool domain.EndpointAddrList
		rep  *stubRepService
	}{
		"only siblings":        {domain.EndpointAddrList{a1, a2}, &stubRepService{scores: map[domain.EndpointAddr]float64{a2: 90}}},
		"other is not vouched": {domain.EndpointAddrList{a1, a2, b}, &stubRepService{scores: map[domain.EndpointAddr]float64{}}},
	} {
		attempts, ctx, rec, err := run(tc.pool, tc.rep)
		if len(attempts) != 1 {
			t.Errorf("%s: attempts %v, want only the first", name, attempts)
		}
		if !errors.Is(err, domain.ErrRetryVerdict) || ctx.Response != limitedAnswer {
			t.Errorf("%s: err %v response %v, want the node's rate-limit answer delivered", name, err, ctx.Response)
		}
		if len(rec.outcomes) != 1 || rec.outcomes[0] != "rate_limited:limited" {
			t.Errorf("%s: resolutions %v, want rate_limited:limited", name, rec.outcomes)
		}
	}
}
