package middleware

import (
	"errors"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// A node's answer that came with a retry verdict ("block not found") is what
// the client gets when the retry then produces no answer at all (a relay
// miner's 408): the router delivers a response in hand under a retry verdict,
// and writes a 500 when there is none. mainnet celo, 2026-09-14.
func TestRetry_KeepsTheLastUpstreamAnswerWhenLaterAttemptsHaveNone(t *testing.T) {
	eps := testEndpoints(2)
	answer := &domain.Response{HTTPStatusCode: 200, Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"block not found"}}`)}
	attempt := 0
	h := relay.HandlerFunc(func(ctx *relay.Context) error {
		attempt++
		if attempt == 1 {
			ctx.Endpoint = eps[0]
			ctx.Response = answer
			ctx.Err = domain.NewRelayError(domain.ErrEndpoint, "heuristic analysis suggests retry: blockchain_error", domain.ErrRetryVerdict, true)
			return ctx.Err
		}
		ctx.Endpoint = eps[1]
		return retryableErr("relay miner answered HTTP 408")
	})

	ctx := baseContext()
	ctx.Endpoints = eps
	err := Retry(newFlags("retry"), retryCfg(1, 0))(h).HandleRelay(ctx)

	if attempt != 2 {
		t.Fatalf("attempts = %d, want the retry to have run", attempt)
	}
	if !errors.Is(err, domain.ErrRetryVerdict) {
		t.Fatalf("err = %v, want the kept answer's retry verdict so the router delivers it", err)
	}
	if ctx.Response != answer || ctx.Endpoint != eps[0] {
		t.Fatalf("response = %v from %s, want the first attempt's answer from %s", ctx.Response, ctx.Endpoint, eps[0])
	}
}

// A later attempt's own answer wins over the kept one.
func TestRetry_ALaterAnswerReplacesTheKeptOne(t *testing.T) {
	eps := testEndpoints(2)
	first := &domain.Response{HTTPStatusCode: 200, Body: []byte(`first`)}
	second := &domain.Response{HTTPStatusCode: 200, Body: []byte(`second`)}
	attempt := 0
	h := relay.HandlerFunc(func(ctx *relay.Context) error {
		attempt++
		ctx.Endpoint = eps[attempt-1]
		if attempt == 1 {
			ctx.Response = first
		} else {
			ctx.Response = second
		}
		ctx.Err = domain.NewRelayError(domain.ErrEndpoint, "retry verdict", domain.ErrRetryVerdict, true)
		return ctx.Err
	})

	ctx := baseContext()
	ctx.Endpoints = eps
	_ = Retry(newFlags("retry"), retryCfg(1, 0))(h).HandleRelay(ctx)
	if ctx.Response != second {
		t.Fatalf("response = %s, want the last attempt's own answer", ctx.Response.Body)
	}
}
