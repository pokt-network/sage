package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

// An empty pool is an honest, non-retryable error — never an empty address
// sent on to the protocol, which reported it as a session rollover and
// retried it.
func TestSelectEndpoint_EmptyEndpointsIsAnError(t *testing.T) {
	repSvc := &mockRepService{}
	registry := qos.NewRegistry()
	flags := newMockFlags(nil)

	mw := middleware.SelectEndpoint(repSvc, nil, registry, flags)

	req := newPOSTRequest("/v1", "")
	ctx := newCtx(req)
	ctx.ServiceID = "eth"
	ctx.Endpoints = domain.EndpointAddrList{} // empty from start

	handler := mw(relay.HandlerFunc(func(c *relay.Context) error {
		t.Errorf("sent on with endpoint %q from an empty pool", c.Endpoint)
		return nil
	}))

	err := handler.HandleRelay(ctx)
	var relayErr *domain.RelayError
	if err == nil || domain.IsRetryable(err) || !errors.As(err, &relayErr) || relayErr.Kind != domain.ErrProtocol {
		t.Fatalf("err = %v, want a non-retryable protocol error", err)
	}
}

// probationRepService is mockRepService whose probation band is a set.
type probationRepService struct {
	mockRepService
	onProbation map[domain.EndpointAddr]bool
}

func (p *probationRepService) OnProbation(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType) bool {
	return p.onProbation[ep]
}

// A probation first try is held to a quarter of the attempt's remaining
// deadline, so a host demoted for timing out cannot spend the client's
// budget timing out again; a healthy pick keeps the whole deadline, and the
// caller gets its own context back either way.
func TestSelectEndpoint_ProbationPickGetsShortDeadline(t *testing.T) {
	const bad, good = domain.EndpointAddr("s1-https://slow.example"), domain.EndpointAddr("s2-https://ok.example")
	for _, tc := range []struct {
		pick     domain.EndpointAddr
		maxShare float64
		minShare float64
	}{
		{bad, 0.26, 0.2},
		{good, 1.01, 0.95},
	} {
		repSvc := &probationRepService{mockRepService{bestEndpoint: tc.pick}, map[domain.EndpointAddr]bool{bad: true}}
		mw := middleware.SelectEndpoint(repSvc, nil, qos.NewRegistry(), newMockFlags(nil))

		req := newPOSTRequest("/v1", "")
		c := newCtx(req)
		c.ServiceID, c.RPCType, c.Endpoints = "eth", domain.RPCTypeJSONRPC, domain.EndpointAddrList{bad, good}
		parent, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		c.Ctx = parent

		var budget time.Duration
		err := mw(relay.HandlerFunc(func(c *relay.Context) error {
			dl, _ := c.Ctx.Deadline()
			budget = time.Until(dl)
			return nil
		})).HandleRelay(c)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		share := float64(budget) / float64(8*time.Second)
		if share > tc.maxShare || share < tc.minShare {
			t.Errorf("pick %s: attempt got %.2f of the deadline, want %.2f–%.2f", tc.pick, share, tc.minShare, tc.maxShare)
		}
		if c.Ctx != parent {
			t.Errorf("pick %s: the caller's context was not restored", tc.pick)
		}
	}
}
