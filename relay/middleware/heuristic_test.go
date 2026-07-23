package middleware_test

import (
	"testing"

	"github.com/pokt-network/sage/domain"
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
