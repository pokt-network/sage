package responsecache

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func makeResponse(body string) *domain.Response {
	return &domain.Response{
		Body:           []byte(body),
		HTTPStatusCode: 200,
	}
}

func TestCache_GetSet(t *testing.T) {
	c := NewCache(10)

	const key = "k1"
	resp := makeResponse("hello")

	// Miss before set.
	if _, ok := c.Get(key); ok {
		t.Fatal("expected cache miss before set")
	}

	c.Set(key, resp, time.Minute)

	got, ok := c.Get(key)
	if !ok {
		t.Fatal("expected cache hit after set")
	}
	if string(got.Body) != "hello" {
		t.Fatalf("unexpected body: %s", got.Body)
	}
}

func TestCache_TTLExpiry(t *testing.T) {
	c := NewCache(10)

	c.Set("expiring", makeResponse("data"), 10*time.Millisecond)

	// Should be present immediately.
	if _, ok := c.Get("expiring"); !ok {
		t.Fatal("expected hit before expiry")
	}

	time.Sleep(30 * time.Millisecond)

	// Should have expired.
	if _, ok := c.Get("expiring"); ok {
		t.Fatal("expected miss after expiry")
	}
}

func TestCache_LRUEviction(t *testing.T) {
	c := NewCache(3)

	c.Set("a", makeResponse("a"), time.Minute)
	c.Set("b", makeResponse("b"), time.Minute)
	c.Set("c", makeResponse("c"), time.Minute)

	// Access "a" to make it recently used; "b" becomes LRU.
	c.Get("a")

	// Adding "d" should evict "b" (LRU).
	c.Set("d", makeResponse("d"), time.Minute)

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected 'b' to be evicted as LRU entry")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected 'a' to still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("expected 'c' to still be present")
	}
	if _, ok := c.Get("d"); !ok {
		t.Fatal("expected 'd' to be present after set")
	}
}

func TestCache_OverwriteExisting(t *testing.T) {
	c := NewCache(10)

	c.Set("k", makeResponse("v1"), time.Minute)
	c.Set("k", makeResponse("v2"), time.Minute)

	got, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got.Body) != "v2" {
		t.Fatalf("expected v2, got %s", got.Body)
	}
	// Size should still be 1.
	if c.Stats().Size != 1 {
		t.Fatalf("expected size 1, got %d", c.Stats().Size)
	}
}

func TestCache_ZeroOrNegativeTTL_NotStored(t *testing.T) {
	c := NewCache(10)

	c.Set("k", makeResponse("v"), 0)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss: zero TTL should not store")
	}

	c.Set("k", makeResponse("v"), -time.Second)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss: negative TTL should not store")
	}
}

func TestCache_Stats(t *testing.T) {
	c := NewCache(2)

	// Two misses.
	c.Get("x")
	c.Get("y")

	c.Set("x", makeResponse("x"), time.Minute)
	c.Set("y", makeResponse("y"), time.Minute)

	// Two hits.
	c.Get("x")
	c.Get("y")

	// One eviction: add a third entry to a cache with maxSize=2.
	c.Set("z", makeResponse("z"), time.Minute)

	stats := c.Stats()
	if stats.Misses != 2 {
		t.Fatalf("expected 2 misses, got %d", stats.Misses)
	}
	if stats.Hits != 2 {
		t.Fatalf("expected 2 hits, got %d", stats.Hits)
	}
	if stats.Evictions != 1 {
		t.Fatalf("expected 1 eviction, got %d", stats.Evictions)
	}
	if stats.Size != 2 {
		t.Fatalf("expected size 2, got %d", stats.Size)
	}
}

func TestKey_Determinism(t *testing.T) {
	svc := domain.ServiceID("eth")
	p := domain.NewPayload([]byte(`{"method":"eth_blockNumber","params":[]}`), domain.RPCTypeJSONRPC, "eth_blockNumber")

	k1 := Key(svc, []domain.Payload{p})
	k2 := Key(svc, []domain.Payload{p})
	if k1 != k2 {
		t.Fatalf("Key is not deterministic: %s vs %s", k1, k2)
	}
}

func TestKey_DifferentServicesDiffer(t *testing.T) {
	p := domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber")
	k1 := Key("eth", []domain.Payload{p})
	k2 := Key("poly", []domain.Payload{p})
	if k1 == k2 {
		t.Fatal("keys for different services should differ")
	}
}

func TestKey_DifferentPayloadsDiffer(t *testing.T) {
	svc := domain.ServiceID("eth")
	p1 := domain.NewPayload([]byte(`params1`), domain.RPCTypeJSONRPC, "m")
	p2 := domain.NewPayload([]byte(`params2`), domain.RPCTypeJSONRPC, "m")
	if Key(svc, []domain.Payload{p1}) == Key(svc, []domain.Payload{p2}) {
		t.Fatal("keys for different payloads should differ")
	}
}
