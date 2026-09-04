package middleware_test

import (
	"testing"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func TestValidate_SupportedType_Passes(t *testing.T) {
	services := []config.ServiceConfig{
		{ID: "eth", RPCTypes: []string{"json_rpc", "websocket"}},
	}
	mw := middleware.Validate(services)

	req := newPOSTRequest("/v1", "")
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC

	var called bool
	handler := mw(relay.HandlerFunc(func(_ *relay.Context) error {
		called = true
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected next handler to be called for supported type")
	}
}

func TestValidate_UnsupportedType_Blocked(t *testing.T) {
	services := []config.ServiceConfig{
		{ID: "eth", RPCTypes: []string{"json_rpc"}},
	}
	mw := middleware.Validate(services)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeREST // not in allowed list

	var called bool
	handler := mw(relay.HandlerFunc(func(_ *relay.Context) error {
		called = true
		return nil
	}))

	err := handler.HandleRelay(ctx)
	if err == nil {
		t.Fatal("expected error for unsupported RPC type")
	}
	if called {
		t.Fatal("next handler should not be called for unsupported type")
	}
	re, ok := err.(*domain.RelayError)
	if !ok {
		t.Fatalf("expected *domain.RelayError, got %T", err)
	}
	if re.Kind != domain.ErrValidation {
		t.Errorf("expected ErrValidation, got %v", re.Kind)
	}
}

func TestValidate_EmptyServiceList_AllPass(t *testing.T) {
	mw := middleware.Validate(nil)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeCometBFT

	var called bool
	handler := mw(relay.HandlerFunc(func(_ *relay.Context) error {
		called = true
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected next handler to be called when service list is empty")
	}
}
