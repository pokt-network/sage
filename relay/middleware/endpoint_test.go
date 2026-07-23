package middleware_test

import (
	"errors"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
)

func TestSelectEndpoint_NormalSelection(t *testing.T) {
	endpoints := domain.EndpointAddrList{
		"supplier1-https://rpc1.example.com",
		"supplier2-https://rpc2.example.com",
	}

	repSvc := &mockRepService{bestEndpoint: "supplier1-https://rpc1.example.com"}
	registry := qos.NewRegistry()
	flags := newMockFlags(nil)

	mw := middleware.SelectEndpoint(repSvc, nil, registry, flags)

	req := newPOSTRequest("/v1", "")
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.RPCType = domain.RPCTypeJSONRPC
	ctx.Endpoints = endpoints

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if c.Endpoint != "supplier1-https://rpc1.example.com" {
			t.Errorf("expected best endpoint, got %s", c.Endpoint)
		}
		if c.Degraded {
			t.Error("expected not degraded")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSelectEndpoint_WithPlugin_Filters(t *testing.T) {
	endpoints := domain.EndpointAddrList{
		"supplier1-https://rpc1.example.com",
		"supplier2-https://rpc2.example.com",
		"supplier3-https://rpc3.example.com",
	}
	// Plugin will return only the last endpoint.
	filtered := domain.EndpointAddrList{"supplier3-https://rpc3.example.com"}

	plugin := &mockPlugin{selectResult: filtered}
	registry := qos.NewRegistry()
	_ = registry.Register("eth", plugin)

	repSvc := &mockRepService{}
	flags := newMockFlags(nil)

	mw := middleware.SelectEndpoint(repSvc, nil, registry, flags)

	req := newPOSTRequest("/v1", "")
	req.Header.Set("Target-Service-Id", "eth")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoints = endpoints
	ctx.Plugin = plugin

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if c.Endpoint != "supplier3-https://rpc3.example.com" {
			t.Errorf("expected filtered endpoint, got %s", c.Endpoint)
		}
		if c.Degraded {
			t.Error("expected not degraded when plugin returns results")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSelectEndpoint_DegradedFallback_PluginError(t *testing.T) {
	endpoints := domain.EndpointAddrList{
		"supplier1-https://rpc1.example.com",
	}

	plugin := &mockPlugin{selectErr: errors.New("no synced endpoints")}
	registry := qos.NewRegistry()
	_ = registry.Register("eth", plugin)

	repSvc := &mockRepService{}
	flags := newMockFlags(nil)

	mw := middleware.SelectEndpoint(repSvc, nil, registry, flags)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoints = endpoints
	ctx.Plugin = plugin

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if !c.Degraded {
			t.Error("expected Degraded=true on plugin error")
		}
		if c.Endpoint == "" {
			t.Error("expected a fallback endpoint to be selected")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// SelectEndpoint records degradation on the context and nothing else. It runs
	// inside the batch and hedge fan-outs, so it cannot know whether the attempt
	// it just degraded is the one the client will be answered with — the router
	// emits the header once, from the merged result. See relay.HeaderDegraded.
	if !ctx.Degraded {
		t.Error("expected ctx.Degraded to be set")
	}
	w := ctx.Writer.(*mockWriter)
	if _, ok := w.headers[relay.HeaderDegraded]; ok {
		t.Error("SelectEndpoint must not write the response header itself")
	}
}

func TestSelectEndpoint_DegradedFallback_PluginReturnsEmpty(t *testing.T) {
	endpoints := domain.EndpointAddrList{
		"supplier1-https://rpc1.example.com",
	}

	plugin := &mockPlugin{selectResult: domain.EndpointAddrList{}} // empty result
	registry := qos.NewRegistry()
	_ = registry.Register("eth", plugin)

	repSvc := &mockRepService{}
	flags := newMockFlags(nil)

	mw := middleware.SelectEndpoint(repSvc, nil, registry, flags)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoints = endpoints
	ctx.Plugin = plugin

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if !c.Degraded {
			t.Error("expected Degraded=true when plugin returns empty list")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSelectEndpoint_EmptyEndpoints_Degraded(t *testing.T) {
	repSvc := &mockRepService{}
	registry := qos.NewRegistry()
	flags := newMockFlags(nil)

	mw := middleware.SelectEndpoint(repSvc, nil, registry, flags)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoints = domain.EndpointAddrList{} // empty from start

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		if !c.Degraded {
			t.Error("expected Degraded=true when no endpoints available")
		}
		return nil
	}))

	if err := handler.HandleRelay(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
