package middleware

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/responsecache"
)

// cachePolicyPlugin is a minimal Plugin + CachePolicy that returns a fixed TTL.
type cachePolicyPlugin struct {
	coalescablePlugin
	ttl time.Duration
}

func (p *cachePolicyPlugin) CacheTTL(_ string, _ []byte, _ []byte) time.Duration { return p.ttl }

// noCachePolicyPlugin does not implement CachePolicy.
type noCachePolicyPlugin struct{ coalescablePlugin }

func makeRelayCtx(svc domain.ServiceID, payload domain.Payload, plugin qos.Plugin) *relay.Context {
	req, _ := http.NewRequest(http.MethodPost, "/v1", nil)
	ctx := relay.NewContext(context.Background(), req, nil, nil)
	ctx.ServiceID = svc
	ctx.Payloads = []domain.Payload{payload}
	ctx.Plugin = plugin
	return ctx
}

func TestCache_Miss_ThenHit(t *testing.T) {
	c := responsecache.NewCache(100)
	mw := Cache(&staticFlags{enabled: true}, c)

	relays := 0
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relays++
		ctx.Response = &domain.Response{Body: []byte(`{"result":"0x1"}`), HTTPStatusCode: 200}
		return nil
	})

	plugin := &cachePolicyPlugin{ttl: time.Minute}
	handler := mw(inner)

	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`params`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	// First call: cache miss.
	ctx1 := makeRelayCtx(svc, payload, plugin)
	if err := handler.HandleRelay(ctx1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx1.Cached {
		t.Fatal("first call should not be cached")
	}
	if relays != 1 {
		t.Fatalf("expected 1 relay, got %d", relays)
	}

	// Second call: cache hit, relay should not run again.
	ctx2 := makeRelayCtx(svc, payload, plugin)
	if err := handler.HandleRelay(ctx2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ctx2.Cached {
		t.Fatal("second call should be served from cache")
	}
	if relays != 1 {
		t.Fatalf("relay should not run on cache hit, got %d relays", relays)
	}
	if string(ctx2.Response.Body) != `{"result":"0x1"}` {
		t.Fatalf("unexpected cached body: %s", ctx2.Response.Body)
	}
}

func TestCache_ExpiredEntry_Miss(t *testing.T) {
	c := responsecache.NewCache(100)
	mw := Cache(&staticFlags{enabled: true}, c)

	relays := 0
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relays++
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	plugin := &cachePolicyPlugin{ttl: 10 * time.Millisecond}
	handler := mw(inner)

	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`params`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	_ = handler.HandleRelay(makeRelayCtx(svc, payload, plugin))
	time.Sleep(30 * time.Millisecond)

	ctx2 := makeRelayCtx(svc, payload, plugin)
	_ = handler.HandleRelay(ctx2)

	if ctx2.Cached {
		t.Fatal("expired entry should result in cache miss")
	}
	if relays != 2 {
		t.Fatalf("expected 2 relays after expiry, got %d", relays)
	}
}

func TestCache_NoCachePolicy_NotStored(t *testing.T) {
	c := responsecache.NewCache(100)
	mw := Cache(&staticFlags{enabled: true}, c)

	relays := 0
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relays++
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	plugin := &noCachePolicyPlugin{}
	handler := mw(inner)

	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`params`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	_ = handler.HandleRelay(makeRelayCtx(svc, payload, plugin))
	ctx2 := makeRelayCtx(svc, payload, plugin)
	_ = handler.HandleRelay(ctx2)

	if ctx2.Cached {
		t.Fatal("should not be cached when plugin has no CachePolicy")
	}
	if relays != 2 {
		t.Fatalf("expected 2 relays, got %d", relays)
	}
}

func TestCache_FlagDisabled_PassThrough(t *testing.T) {
	c := responsecache.NewCache(100)
	mw := Cache(&staticFlags{enabled: false}, c)

	relays := 0
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relays++
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	plugin := &cachePolicyPlugin{ttl: time.Minute}
	handler := mw(inner)

	const svc = domain.ServiceID("eth")
	payload := domain.NewPayload([]byte(`params`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	_ = handler.HandleRelay(makeRelayCtx(svc, payload, plugin))
	ctx2 := makeRelayCtx(svc, payload, plugin)
	_ = handler.HandleRelay(ctx2)

	if ctx2.Cached {
		t.Fatal("cache should be disabled by flag")
	}
	if relays != 2 {
		t.Fatalf("expected 2 relays when flag disabled, got %d", relays)
	}
}

func TestCache_LRUEviction(t *testing.T) {
	// Cache with capacity 2; third entry should evict the LRU.
	c := responsecache.NewCache(2)
	mw := Cache(&staticFlags{enabled: true}, c)

	const svc = domain.ServiceID("eth")

	makePayloadAndCtx := func(data string) (*relay.Context, domain.Payload) {
		p := domain.NewPayload([]byte(data), domain.RPCTypeJSONRPC, "eth_blockNumber")
		return makeRelayCtx(svc, p, &cachePolicyPlugin{ttl: time.Minute}), p
	}

	relays := 0
	inner := relay.HandlerFunc(func(ctx *relay.Context) error {
		relays++
		ctx.Response = &domain.Response{Body: []byte(`{}`), HTTPStatusCode: 200}
		return nil
	})

	handler := mw(inner)

	ctx1, p1 := makePayloadAndCtx("params1")
	ctx2, _ := makePayloadAndCtx("params2")
	ctx3, _ := makePayloadAndCtx("params3")

	_ = handler.HandleRelay(ctx1) // fills slot 1
	_ = handler.HandleRelay(ctx2) // fills slot 2 — p1 is now LRU

	// Accessing p1 promotes it; p2 becomes LRU.
	freshCtx1 := makeRelayCtx(svc, p1, &cachePolicyPlugin{ttl: time.Minute})
	_ = handler.HandleRelay(freshCtx1)
	if !freshCtx1.Cached {
		t.Fatal("p1 should be cached")
	}

	// Adding p3 should evict p2 (LRU).
	_ = handler.HandleRelay(ctx3)

	// p2 should now be a miss (evicted).
	p2Again := domain.NewPayload([]byte("params2"), domain.RPCTypeJSONRPC, "eth_blockNumber")
	freshCtx2 := makeRelayCtx(svc, p2Again, &cachePolicyPlugin{ttl: time.Minute})
	_ = handler.HandleRelay(freshCtx2)
	if freshCtx2.Cached {
		t.Fatal("p2 should have been evicted from cache")
	}
}
