package middleware

import (
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
		err := Heuristic(newFlags("heuristic"), registryWith(t))(relay.HandlerFunc(notFound)).HandleRelay(ctx)
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
		if err := Heuristic(newFlags("heuristic"), registryWith(t))(relay.HandlerFunc(notFound)).HandleRelay(ctx); err != nil {
			t.Fatalf("err = %v, want none: a name the catalogue does not know is the client's", err)
		}
		if ctx.HeuristicResult == nil || ctx.HeuristicResult.ShouldRetry {
			t.Fatalf("verdict = %+v, want no retry", ctx.HeuristicResult)
		}
	})

	t.Run("no registry does not retry", func(t *testing.T) {
		ctx := methodCtx("eth_blockNumber", eps)
		if err := Heuristic(newFlags("heuristic"), nil)(relay.HandlerFunc(notFound)).HandleRelay(ctx); err != nil {
			t.Fatalf("err = %v, want none without a catalogue to consult", err)
		}
	})
}
