package middleware_test

import (
	"context"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func TestHeuristic_SuccessResponse_NoError(t *testing.T) {
	flags := newMockFlags(map[string]bool{"heuristic": true})
	mw := middleware.Heuristic(flags)

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
	mw := middleware.Heuristic(flags)

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
	mw := middleware.Heuristic(flags)

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
	mw := middleware.Heuristic(flags)

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
	mw := middleware.Heuristic(flags)

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
	mw := middleware.Heuristic(flags)

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
	mw := middleware.Heuristic(flags)

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
	h := middleware.Heuristic(flags)(inner)

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
	h := middleware.Heuristic(flags)(inner)

	ctx := newCtx(newPOSTRequest("/v1", ""))
	ctx.Ctx = goCtx
	_ = h.HandleRelay(ctx)
	if ctx.HeuristicResult == nil || ctx.HeuristicResult.Attribution != heuristic.AttrClient {
		t.Fatalf("result = %+v, want AttrClient", ctx.HeuristicResult)
	}
}
