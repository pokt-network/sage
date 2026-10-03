package middleware

import (
	"encoding/json"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
)

// Review of 5fd0d96..c3a3a7a: grading changes that can hide a broken
// supplier. Each test asserts the invariant; on c3a3a7a they fail.

// SupplierPage (relay/context.go) counts only a 408 or a non-JSON body as the
// supplier's page, so a supplier's 5xx with a JSON body that is not a JSON-RPC
// envelope (a load balancer's {"message":"Bad Gateway"}) is delivered as the
// node's answer. The router does the same for a single request, but with the
// 502 status on the wire. A batch item has no status of its own: since
// 3695fc8 the item is that object, with no id and no error, inside a 200
// array; before, it was a -32603 carrying the request's id. A JSON-RPC client
// matching batch answers by id cannot place it.
func TestReview_BatchItemFromSupplierJSONPageKeepsItsID(t *testing.T) {
	page := `{"message":"Bad Gateway"}`
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		id := string(ctx.Payloads[0].JSONRPCID())
		if id == "2" {
			ctx.Response = &domain.Response{Body: []byte(page), HTTPStatusCode: 502}
			ctx.HeuristicResult = &heuristic.AnalysisResult{
				ShouldRetry: true, ShouldPenalize: true, PenaltySeverity: heuristic.SeverityMajor,
				Attribution: heuristic.AttrSupplier, Reason: "http_5xx",
			}
			return domain.NewRelayError(domain.ErrEndpoint, "heuristic analysis suggests retry: http_5xx", domain.ErrRetryVerdict, true)
		}
		ctx.Response = &domain.Response{Body: []byte(`{"jsonrpc":"2.0","id":` + id + `,"result":"0x1"}`), HTTPStatusCode: 200}
		return nil
	})
	ctx := makeMultiPayloadCtx([]domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}`), domain.RPCTypeJSONRPC, "eth_chainId"),
	})
	if err := Batch(fixedLimits(4, 0), nil, nil, nil)(inner).HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(ctx.Response.Body, &arr); err != nil || len(arr) != 2 {
		t.Fatalf("batch body %s: %v", ctx.Response.Body, err)
	}
	if id := gjson.GetBytes(arr[1], "id"); id.Raw != "2" {
		t.Fatalf("item 2 = %s (status %d): a supplier's 502 page delivered as the item, with no id; want an item answering id 2",
			arr[1], ctx.Response.HTTPStatusCode)
	}
}
