package middleware

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
)

// --- test doubles ---

// staticFlags is a FlagStore that always returns a fixed value.
type staticFlags struct{ enabled bool }

func (f *staticFlags) IsEnabled(_ context.Context, _ string, _ domain.ServiceID) bool {
	return f.enabled
}
func (f *staticFlags) Set(_ context.Context, _ string, _ bool) error { return nil }
func (f *staticFlags) SetForService(_ context.Context, _ string, _ domain.ServiceID, _ bool) error {
	return nil
}
func (f *staticFlags) GetAll(_ context.Context) (map[string]featureflag.FlagState, error) {
	return nil, nil
}
func (f *staticFlags) Delete(_ context.Context, _ string, _ domain.ServiceID) error { return nil }
func (f *staticFlags) DeleteGlobal(_ context.Context, _ string) error               { return nil }

// coalescablePlugin is a minimal qos.Plugin that classifies every method as
// coalescable.
type coalescablePlugin struct{}

func (p *coalescablePlugin) ParseRequest(_ context.Context, _ *http.Request, _ []byte, _ domain.RPCType) ([]domain.Payload, error) {
	return nil, nil
}
func (p *coalescablePlugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return endpoints, nil
}
func (p *coalescablePlugin) IsCoalescable(_ string) bool { return true }

// nonCoalescablePlugin never reports methods as coalescable.
type nonCoalescablePlugin struct{ coalescablePlugin }

func (p *nonCoalescablePlugin) IsCoalescable(_ string) bool { return false }

// noClassifierPlugin does not implement CoalescenceClassifier.
type noClassifierPlugin struct{ coalescablePlugin }

func newRelayContext(svc domain.ServiceID, plugin qos.Plugin, payload domain.Payload) *relay.Context {
	req, _ := http.NewRequest(http.MethodPost, "/v1", nil)
	ctx := relay.NewContext(context.Background(), req, nil, nil)
	ctx.ServiceID = svc
	ctx.Plugin = plugin
	ctx.Payloads = []domain.Payload{payload}
	return ctx
}

// --- tests ---

func TestSingleflight_TwoConcurrentRequests_OneRelay(t *testing.T) {
	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`{"method":"eth_blockNumber","params":[]}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &coalescablePlugin{}

	var relayCount atomic.Int32
	ready := make(chan struct{})
	proceed := make(chan struct{})

	innerResp := &domain.Response{Body: []byte(`{"result":"0x1"}`), HTTPStatusCode: 200}

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		close(ready)
		<-proceed
		ctx.Response = innerResp
		return nil
	})

	mw := Singleflight(&staticFlags{enabled: true})
	handler := mw(inner)

	ctx1 := newRelayContext(svc, plugin, payload)
	ctx2 := newRelayContext(svc, plugin, payload)

	var wg sync.WaitGroup
	wg.Add(2)

	var err1, err2 error

	go func() {
		defer wg.Done()
		err1 = handler.HandleRelay(ctx1)
	}()

	// Wait until the first goroutine is inside the inner handler, then fire
	// the second request so it joins the inflight call.
	<-ready

	go func() {
		defer wg.Done()
		err2 = handler.HandleRelay(ctx2)
	}()

	// Give the second goroutine time to register with singleflight.
	time.Sleep(20 * time.Millisecond)
	close(proceed)

	wg.Wait()

	if err1 != nil {
		t.Fatalf("first request error: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second request error: %v", err2)
	}

	if relayCount.Load() != 1 {
		t.Fatalf("expected exactly 1 upstream relay, got %d", relayCount.Load())
	}

	// Exactly one context should be marked Coalesced (the follower).
	// The leader actually ran the relay so it is not coalesced.
	coalesced := 0
	if ctx1.Coalesced {
		coalesced++
	}
	if ctx2.Coalesced {
		coalesced++
	}
	if coalesced != 1 {
		t.Fatalf("expected exactly 1 coalesced context, got %d", coalesced)
	}

	// Both should have the same response body.
	if string(ctx1.Response.Body) != string(innerResp.Body) {
		t.Fatalf("ctx1 body mismatch: %s", ctx1.Response.Body)
	}
	if string(ctx2.Response.Body) != string(innerResp.Body) {
		t.Fatalf("ctx2 body mismatch: %s", ctx2.Response.Body)
	}
}

func TestSingleflight_NonCoalescable_NoCoalescing(t *testing.T) {
	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`{"method":"eth_sendTransaction","params":[]}`), domain.RPCTypeJSONRPC, "eth_sendTransaction")
	plugin := &nonCoalescablePlugin{}

	var relayCount atomic.Int32
	ready := make(chan struct{})
	proceed := make(chan struct{})

	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		n := relayCount.Add(1)
		if n == 1 {
			close(ready)
			<-proceed
		}
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Singleflight(&staticFlags{enabled: true})
	handler := mw(inner)

	ctx1 := newRelayContext(svc, plugin, payload)
	ctx2 := newRelayContext(svc, plugin, payload)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = handler.HandleRelay(ctx1) }()
	<-ready
	go func() { defer wg.Done(); _ = handler.HandleRelay(ctx2) }()
	time.Sleep(20 * time.Millisecond)
	close(proceed)
	wg.Wait()

	if relayCount.Load() != 2 {
		t.Fatalf("expected 2 relays for non-coalescable method, got %d", relayCount.Load())
	}
	if ctx1.Coalesced || ctx2.Coalesced {
		t.Fatal("neither context should be marked Coalesced for non-coalescable method")
	}
}

func TestSingleflight_NoClassifier_PassThrough(t *testing.T) {
	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`{"method":"eth_blockNumber","params":[]}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &noClassifierPlugin{}

	var relayCount atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Singleflight(&staticFlags{enabled: true})
	handler := mw(inner)

	ctx1 := newRelayContext(svc, plugin, payload)
	ctx2 := newRelayContext(svc, plugin, payload)

	// Run sequentially — no coalescing expected.
	_ = handler.HandleRelay(ctx1)
	_ = handler.HandleRelay(ctx2)

	if relayCount.Load() != 2 {
		t.Fatalf("expected 2 separate relays when plugin has no CoalescenceClassifier, got %d", relayCount.Load())
	}
}

func TestSingleflight_DifferentMethods_SeparateRelays(t *testing.T) {
	const svc = domain.ServiceID("eth")
	p1 := domain.NewPayload([]byte(`params1`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	p2 := domain.NewPayload([]byte(`params1`), domain.RPCTypeJSONRPC, "eth_getBalance")
	plugin := &coalescablePlugin{}

	var relayCount atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Singleflight(&staticFlags{enabled: true})
	handler := mw(inner)

	_ = handler.HandleRelay(newRelayContext(svc, plugin, p1))
	_ = handler.HandleRelay(newRelayContext(svc, plugin, p2))

	if relayCount.Load() != 2 {
		t.Fatalf("expected 2 relays for different methods, got %d", relayCount.Load())
	}
}

func TestSingleflight_FlagDisabled_PassThrough(t *testing.T) {
	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	plugin := &coalescablePlugin{}

	var relayCount atomic.Int32
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relayCount.Add(1)
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	mw := Singleflight(&staticFlags{enabled: false})
	handler := mw(inner)

	_ = handler.HandleRelay(newRelayContext(svc, plugin, payload))
	_ = handler.HandleRelay(newRelayContext(svc, plugin, payload))

	if relayCount.Load() != 2 {
		t.Fatalf("expected 2 relays when flag disabled, got %d", relayCount.Load())
	}
}
