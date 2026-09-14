package middleware_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func TestHeuristic_SuccessResponse_NoError(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Payloads = []domain.Payload{
		domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}
	ctx.Response = &domain.Response{
		Body:           []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`),
		HTTPStatusCode: 200,
	}

	handler := mw(relay.Noop)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error for valid response: %v", err)
	}

	if ctx.HeuristicResult == nil {
		t.Fatal("expected HeuristicResult to be set in context")
	}
	if ctx.HeuristicResult.ShouldRetry {
		t.Errorf("expected ShouldRetry=false for valid response, got true: %s", ctx.HeuristicResult.Reason)
	}
}

func TestHeuristic_500Response_TriggersRetry(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Response = &domain.Response{
		Body:           []byte(`Internal Server Error`),
		HTTPStatusCode: 500,
	}

	handler := mw(relay.Noop)
	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}

	re, ok := err.(*domain.RelayError)
	if !ok {
		t.Fatalf("expected *domain.RelayError, got %T", err)
	}
	if !re.Retryable {
		t.Error("expected retryable error for 500 response")
	}
	if ctx.Err == nil {
		t.Error("expected ctx.Err to be set")
	}
}

func TestHeuristic_EmptyBody_TriggersRetry(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Response = &domain.Response{
		Body:           []byte{},
		HTTPStatusCode: 200,
	}

	handler := mw(relay.Noop)
	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error for empty response body")
	}

	re, ok := err.(*domain.RelayError)
	if !ok {
		t.Fatalf("expected *domain.RelayError, got %T", err)
	}
	if !re.Retryable {
		t.Error("expected retryable error for empty response")
	}
}

func TestHeuristic_FlagDisabled_NoAnalysis(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": false})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Response = &domain.Response{
		Body:           []byte{}, // empty — would normally trigger retry
		HTTPStatusCode: 200,
	}

	handler := mw(relay.Noop)
	err := handler.HandleRelay(ctx)
	if err != nil {
		t.Fatalf("expected no error when heuristic flag is disabled, got: %v", err)
	}

	if ctx.HeuristicResult != nil {
		t.Error("expected HeuristicResult NOT to be set when flag is disabled")
	}
}

func TestHeuristic_NilResponse_NoAnalysis(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Response = nil // no response yet

	handler := mw(relay.Noop)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error when response is nil: %v", err)
	}

	if ctx.HeuristicResult != nil {
		t.Error("expected HeuristicResult NOT to be set when response is nil")
	}
}

func TestHeuristic_InnerHandlerError_Propagated(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC

	innerErr := domain.NewRelayError(domain.ErrTransport, "dial failed", nil, true)
	handler := mw(relay.HandlerFunc(func(_ *relay.Context) error {
		return innerErr
	}))

	err := handler.HandleRelay(ctx)
	if err != innerErr {
		t.Errorf("expected inner error to be propagated, got: %v", err)
	}
}

func TestHeuristic_4xxResponse_NoRetry(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags, nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Response = &domain.Response{
		Body:           []byte(`{"error":"not found"}`),
		HTTPStatusCode: 404,
	}

	handler := mw(relay.Noop)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("expected no error for 4xx (client error, no retry): %v", err)
	}

	if ctx.HeuristicResult == nil {
		t.Fatal("expected HeuristicResult to be set")
	}
	if ctx.HeuristicResult.ShouldRetry {
		t.Error("expected ShouldRetry=false for 4xx response")
	}
}

// A transport failure used to leave the chain with no verdict at all. Now
// the attempt is graded: the inner error is still returned (retry needs it),
// and ctx.HeuristicResult carries what the failure meant.
//
// This package (middleware_test) has no unexported helpers of its own, so
// this test builds its context and flags with newCtx/newPOSTRequest/
// newMockFlags — this file's existing convention — rather than the
// package-internal baseContext/newFlags the brief's snippet used.
func TestHeuristic_TransportErrorIsGraded(t *testing.T) {
	inner := relay.HandlerFunc(func(_ *relay.Context) error {
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.DeadlineExceeded, true)
	})
	flags := newMockFlags(map[string]bool{"heuristic": true})
	h := middleware.Heuristic(flags, nil)(inner)

	ctx := newCtx(newPOSTRequest("/v1", ""))
	err := h.HandleRelay(ctx)
	if err == nil {
		t.Fatal("the transport error must still propagate")
	}
	if ctx.HeuristicResult == nil {
		t.Fatal("transport error left no HeuristicResult")
	}
	if ctx.HeuristicResult.Reason != "transport_timeout" {
		t.Fatalf("reason = %q, want transport_timeout", ctx.HeuristicResult.Reason)
	}
}

// The classifier needs the request context's own error to tell a client
// hang-up from a supplier hang; the middleware must pass it.
func TestHeuristic_ClientCancelIsAttributedToClient(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	inner := relay.HandlerFunc(func(_ *relay.Context) error {
		cancel()
		return domain.NewRelayError(domain.ErrTransport, "HTTP relay failed", context.Canceled, true)
	})
	flags := newMockFlags(map[string]bool{"heuristic": true})
	h := middleware.Heuristic(flags, nil)(inner)

	ctx := newCtx(newPOSTRequest("/v1", ""))
	ctx.Ctx = goCtx
	_ = h.HandleRelay(ctx)
	if ctx.HeuristicResult == nil || ctx.HeuristicResult.Attribution != heuristic.AttrClient {
		t.Fatalf("result = %+v, want AttrClient", ctx.HeuristicResult)
	}
}

// refinerPlugin says a REST 5xx on /cosmwasm/…/smart/… is the chain's answer.
type refinerPlugin struct{ *mockPlugin }

func (refinerPlugin) RefineVerdict(payload domain.Payload, result heuristic.AnalysisResult) (heuristic.AnalysisResult, bool) {
	if (result.Reason == "http_5xx" || result.Reason == "upstream_5xx") && strings.Contains(payload.Path(), "/smart/") {
		return heuristic.AnalysisResult{Attribution: heuristic.AttrBlockchain, Reason: "query_5xx"}, true
	}
	return result, false
}

func smartQueryCtx(t *testing.T) (*relay.Context, *qos.Registry) {
	t.Helper()
	reg := qos.NewRegistry()
	if err := reg.Register("osmosis", refinerPlugin{&mockPlugin{}}); err != nil {
		t.Fatal(err)
	}
	ctx := newCtx(newGETRequest("/cosmwasm/wasm/v1/contract/osmo1abc/smart/eyJ9"))
	ctx.ServiceID = "osmosis"
	ctx.RPCType = domain.RPCTypeREST
	ctx.Payloads = []domain.Payload{domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/cosmwasm/wasm/v1/contract/osmo1abc/smart/eyJ9", "GET")}
	return ctx, reg
}

// A node's 5xx on a route the plugin refines is delivered: no retry verdict,
// the refined attribution on the context.
func TestHeuristic_PluginRefinesA5xxResponse(t *testing.T) {
	ctx, reg := smartQueryCtx(t)
	ctx.Response = &domain.Response{Body: []byte(`{"code":2,"message":"query wasm contract failed"}`), HTTPStatusCode: 500}
	flags := newMockFlags(map[string]bool{"heuristic": true})

	if err := middleware.Heuristic(flags, reg)(relay.Noop).HandleRelay(ctx); err != nil {
		t.Fatalf("a refined 5xx must be delivered, got %v", err)
	}
	if ctx.HeuristicResult == nil || ctx.HeuristicResult.Reason != "query_5xx" || ctx.HeuristicResult.Attribution != heuristic.AttrBlockchain {
		t.Fatalf("result = %+v", ctx.HeuristicResult)
	}

	// The same 5xx on a route the plugin does not refine is still retried.
	ctx.Payloads = []domain.Payload{domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP("/cosmos/bank/v1beta1/balances/osmo1abc", "GET")}
	ctx.Response = &domain.Response{Body: []byte(`oops`), HTTPStatusCode: 500}
	if err := middleware.Heuristic(flags, reg)(relay.Noop).HandleRelay(ctx); !errors.Is(err, domain.ErrRetryVerdict) {
		t.Fatalf("an unrefined 5xx must still carry the retry verdict, got %v", err)
	}
}

// The miner's 5xx relaying the same answer arrives as a transport error
// that the relayer marked retryable; the refined verdict turns it into a
// final error so Retry stops, and the attribution follows.
func TestHeuristic_PluginRefinesAnUpstream5xxError(t *testing.T) {
	ctx, reg := smartQueryCtx(t)
	inner := relay.HandlerFunc(func(_ *relay.Context) error {
		return domain.NewRelayError(domain.ErrEndpoint, "upstream endpoint unavailable", &domain.UpstreamStatusError{Status: 500}, true)
	})
	flags := newMockFlags(map[string]bool{"heuristic": true})

	err := middleware.Heuristic(flags, reg)(inner).HandleRelay(ctx)
	if err == nil {
		t.Fatal("the transport error must still propagate")
	}
	if domain.IsRetryable(err) {
		t.Fatalf("a refined verdict that says deliver must not leave the error retryable: %v", err)
	}
	if ctx.Err == nil || domain.IsRetryable(ctx.Err) {
		t.Fatalf("ctx.Err must carry the final error, got %v", ctx.Err)
	}
	if ctx.HeuristicResult == nil || ctx.HeuristicResult.Reason != "query_5xx" || ctx.HeuristicResult.ShouldPenalize {
		t.Fatalf("result = %+v", ctx.HeuristicResult)
	}
}

// The retry verdict is an error only to make Retry go again; whoever is left
// holding it with a response in hand must be able to tell it from a failure
// that has nothing to deliver.
func TestHeuristic_RetryVerdict_IsIdentifiable(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	handler := middleware.Heuristic(flags, nil)(relay.Noop)

	ctx := newCtx(newPOSTRequest("/v1", ""))
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Response = &domain.Response{Body: []byte(`Internal Server Error`), HTTPStatusCode: 500}

	err := handler.HandleRelay(ctx)
	if !errors.Is(err, domain.ErrRetryVerdict) {
		t.Fatalf("retry verdict must wrap domain.ErrRetryVerdict, got %v", err)
	}
}
