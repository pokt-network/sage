package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func TestSendRelay_Success(t *testing.T) {
	resp := &domain.Response{
		Body:           []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`),
		HTTPStatusCode: 200,
	}
	relayer := &mockRelayer{response: resp}
	mw := middleware.SendRelay(relayer)

	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoint = "supplier1-https://rpc1.example.com"
	ctx.Payloads = []domain.Payload{
		domain.NewPayload([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`), domain.RPCTypeJSONRPC, "eth_blockNumber"),
	}

	// SendRelay is terminal — pass Noop as next (it won't be called).
	handler := mw(relay.Noop)

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx.Response == nil {
		t.Fatal("expected response to be set")
	}
	if string(ctx.Response.Body) != string(resp.Body) {
		t.Errorf("unexpected response body: %s", ctx.Response.Body)
	}
	if ctx.Err != nil {
		t.Errorf("expected no error on success, got: %v", ctx.Err)
	}
}

func TestSendRelay_ErrorWrapped(t *testing.T) {
	relayer := &mockRelayer{err: errors.New("connection refused")}
	mw := middleware.SendRelay(relayer)

	req := newPOSTRequest("/v1", `{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`)
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoint = "supplier1-https://rpc1.example.com"
	ctx.Payloads = []domain.Payload{
		domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, ""),
	}

	handler := mw(relay.Noop)

	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error from relayer")
	}

	re, ok := err.(*domain.RelayError)
	if !ok {
		t.Fatalf("expected *domain.RelayError, got %T", err)
	}
	if re.Kind != domain.ErrEndpoint {
		t.Errorf("expected ErrEndpoint, got %v", re.Kind)
	}
	if !re.Retryable {
		t.Error("expected error to be retryable")
	}
	if ctx.Response != nil {
		t.Error("expected response to be nil on error")
	}
	if ctx.Err == nil {
		t.Error("expected ctx.Err to be set")
	}
}

func TestSendRelay_MultiplePayloads_StoresFirst(t *testing.T) {
	callCount := 0
	responses := []*domain.Response{
		{Body: []byte(`{"id":1}`), HTTPStatusCode: 200},
		{Body: []byte(`{"id":2}`), HTTPStatusCode: 200},
	}

	multi := &multiRelayer{responses: responses, count: &callCount}
	mw := middleware.SendRelay(multi)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoint = "supplier1-https://rpc1.example.com"
	ctx.Payloads = []domain.Payload{
		domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, ""),
		domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, ""),
	}

	handler := mw(relay.Noop)
	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if callCount != 2 {
		t.Errorf("expected 2 relay calls, got %d", callCount)
	}
	// First response should be stored.
	if string(ctx.Response.Body) != `{"id":1}` {
		t.Errorf("expected first response body, got %s", ctx.Response.Body)
	}
}

// multiRelayer returns successive responses from a list.
type multiRelayer struct {
	responses []*domain.Response
	count     *int
}

func (m *multiRelayer) SendRelay(_ context.Context, _ domain.ServiceID, _ domain.EndpointAddr, _ domain.Payload) (*domain.Response, error) {
	idx := *m.count
	*m.count++
	if idx < len(m.responses) {
		return m.responses[idx], nil
	}
	return nil, errors.New("no more responses")
}
