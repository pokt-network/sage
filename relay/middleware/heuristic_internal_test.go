package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

// A -32601 on a catalogued method is retried on another operator: the
// analyzer cannot tell a real method from a bogus name and leaves it
// unretried, the middleware can, through the plugin's catalogue. A name the
// catalogue does not know stays the client's, no retry, so a bogus method
// cannot bounce across the pool. Nothing is scored either way.
func TestHeuristic_MethodNotFound_RetriesOnlyCataloguedMethods(t *testing.T) {
	notFound := func(ctx *relay.Context) error {
		ctx.Response = &domain.Response{
			HTTPStatusCode: 200,
			Body:           []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}`),
		}
		return nil
	}
	eps := testEndpoints(2)

	t.Run("catalogued method retries", func(t *testing.T) {
		ctx := methodCtx("eth_blockNumber", eps)
		err := Heuristic(newFlags("heuristic"), registryWith(t), HeuristicOptions{})(relay.HandlerFunc(notFound)).HandleRelay(ctx)
		if err == nil || !errors.Is(err, domain.ErrRetryVerdict) || !domain.IsRetryable(err) {
			t.Fatalf("err = %v, want a retryable retry verdict", err)
		}
		r := ctx.HeuristicResult
		if r == nil || !r.ShouldRetry || r.Reason != heuristic.ReasonMethodNotFound {
			t.Fatalf("verdict = %+v, want ShouldRetry on method_not_found", r)
		}
		if r.ShouldPenalize || r.Attribution != heuristic.AttrClient || !r.MethodBlocking {
			t.Fatalf("verdict = %+v, want no penalty, client attribution, method blocking kept", r)
		}
	})

	t.Run("uncatalogued method does not retry", func(t *testing.T) {
		ctx := methodCtx("", eps) // normPlugin reports "" as no method
		if err := Heuristic(newFlags("heuristic"), registryWith(t), HeuristicOptions{})(relay.HandlerFunc(notFound)).HandleRelay(ctx); err != nil {
			t.Fatalf("err = %v, want none: a name the catalogue does not know is the client's", err)
		}
		if ctx.HeuristicResult == nil || ctx.HeuristicResult.ShouldRetry {
			t.Fatalf("verdict = %+v, want no retry", ctx.HeuristicResult)
		}
	})

	t.Run("no registry does not retry", func(t *testing.T) {
		ctx := methodCtx("eth_blockNumber", eps)
		if err := Heuristic(newFlags("heuristic"), nil, HeuristicOptions{})(relay.HandlerFunc(notFound)).HandleRelay(ctx); err != nil {
			t.Fatalf("err = %v, want none without a catalogue to consult", err)
		}
	})
}

// Behind light_method_errors a node's own -32603 on a light call is the
// supplier's: a retried major penalty. On any other call, or with the flag
// off, it passes through unscored as before. When every attempt gets it the
// error is delivered, not SAGE's 5xx.
func TestHeuristic_LightMethodErrorIsTheSuppliers(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}`)
	ctxFor := func(method string) *relay.Context {
		ctx := baseContext()
		ctx.RPCType = domain.RPCTypeJSONRPC
		ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"`+method+`"}`), domain.RPCTypeJSONRPC, method)}
		return ctx
	}
	send := relay.HandlerFunc(func(c *relay.Context) error {
		c.Endpoint = c.Endpoints[0]
		c.Response = &domain.Response{HTTPStatusCode: 200, Body: body}
		return nil
	})
	verdict := func(flags *mockFlags, method string) (*heuristic.AnalysisResult, error) {
		ctx := ctxFor(method)
		err := Heuristic(flags, nil, HeuristicOptions{})(send).HandleRelay(ctx)
		return ctx.HeuristicResult, err
	}

	on := newFlags("heuristic", "light_method_errors")
	if r, err := verdict(on, "getSlot"); r == nil || r.Reason != heuristic.ReasonLightMethodError ||
		r.Attribution != heuristic.AttrSupplier || r.PenaltySeverity != heuristic.SeverityMajor || !domain.IsRetryable(err) {
		t.Fatalf("light call: verdict %+v err %v, want a retried major light_method_error", r, err)
	}
	if r, err := verdict(on, "getAccountInfo"); err != nil || r == nil || r.Reason != "internal_error" || r.ShouldPenalize {
		t.Fatalf("standard call: verdict %+v err %v, want the unscored pass-through", r, err)
	}
	if r, err := verdict(newFlags("heuristic"), "getSlot"); err != nil || r == nil || r.Reason != "internal_error" || r.ShouldPenalize {
		t.Fatalf("flag off: verdict %+v err %v, want the unscored pass-through", r, err)
	}

	// The error, then a timeout: the error is still the only answer.
	attempt := 0
	thenTimeout := relay.HandlerFunc(func(c *relay.Context) error {
		if attempt++; attempt > 1 {
			c.Endpoint = c.Endpoints[0]
			return domain.NewRelayError(domain.ErrTransport, "timeout", context.DeadlineExceeded, true)
		}
		return send(c)
	})
	flags := newFlags("retry", "heuristic", "light_method_errors")
	ctx := ctxFor("getSlot")
	_ = Retry(flags, retryCfg(1, 0), nil, RetryOptions{})(Heuristic(flags, nil, HeuristicOptions{})(thenTimeout)).HandleRelay(ctx)
	if ctx.Response == nil || string(ctx.Response.Body) != string(body) {
		t.Fatalf("error then timeout: delivered %v, want the node's error", ctx.Response)
	}
}

// A CometBFT node's error answer the HA relay miner handed on as its own 500
// is the node's answer: delivered with its body, graded by it, and on a light
// call still the supplier's.
func TestHeuristic_CometBFT500FromTheMinerIsTheNodesAnswer(t *testing.T) {
	envelope := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"tx (ABCD) not found"}}`)
	run := func(method string) (*relay.Context, error) {
		ctx := baseContext()
		ctx.RPCType = domain.RPCTypeCometBFT
		ctx.Payloads = []domain.Payload{domain.NewPayload([]byte(`{}`), domain.RPCTypeCometBFT, method)}
		err := Heuristic(newFlags("heuristic", "light_method_errors"), nil, HeuristicOptions{})(relay.HandlerFunc(func(c *relay.Context) error {
			c.Endpoint = c.Endpoints[0]
			return domain.NewRelayError(domain.ErrEndpoint, "upstream endpoint unavailable", &domain.UpstreamStatusError{Status: 500, Body: envelope}, true)
		})).HandleRelay(ctx)
		return ctx, err
	}
	ctx, err := run("block")
	if err != nil || ctx.Response == nil || string(ctx.Response.Body) != string(envelope) ||
		ctx.HeuristicResult.Attribution != heuristic.AttrBlockchain || ctx.HeuristicResult.ShouldPenalize {
		t.Fatalf("block: err %v response %v verdict %+v, want the node's answer delivered, unscored", err, ctx.Response, ctx.HeuristicResult)
	}
	if ctx, err := run("status"); !domain.IsRetryable(err) || ctx.HeuristicResult.Reason != heuristic.ReasonLightMethodError {
		t.Fatalf("status: err %v verdict %+v, want a retried light_method_error", err, ctx.HeuristicResult)
	}
}
