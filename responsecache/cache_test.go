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
	if len(c.entries) != 1 {
		t.Fatalf("expected size 1, got %d", len(c.entries))
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

// The cache holds at most maxBytes of bodies: least-recently-used entries go
// to make room, a body larger than the whole cap is never stored, and an
// overwrite or an eviction gives its bytes back.
func TestCache_ByteCap(t *testing.T) {
	c := NewCache(100)
	c.maxBytes = 10

	c.Set("a", makeResponse("aaaa"), time.Minute)
	c.Set("b", makeResponse("bbbb"), time.Minute)
	c.Get("a") // a is now the most recently used
	c.Set("c", makeResponse("cccc"), time.Minute)
	if _, ok := c.Get("b"); ok {
		t.Error("b, least recently used, should have made room for c")
	}
	for _, k := range []string{"a", "c"} {
		if _, ok := c.Get(k); !ok {
			t.Errorf("%s should still be cached", k)
		}
	}
	if c.bytes != 8 {
		t.Errorf("bytes = %d, want 8", c.bytes)
	}

	c.Set("a", makeResponse("aa"), time.Minute)
	if c.bytes != 6 {
		t.Errorf("bytes after overwrite = %d, want 6", c.bytes)
	}

	c.Set("huge", makeResponse("0123456789x"), time.Minute)
	if _, ok := c.Get("huge"); ok || c.bytes != 6 {
		t.Errorf("a body over the whole cap was stored (bytes %d)", c.bytes)
	}
}

// Expired entries at the least-recently-used end are dropped on Set, not left
// holding their bodies until the cache fills.
func TestCache_SetSweepsExpired(t *testing.T) {
	c := NewCache(100)
	c.Set("old1", makeResponse("1111"), time.Millisecond)
	c.Set("old2", makeResponse("2222"), time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	c.Set("new", makeResponse("nn"), time.Minute)
	if len(c.entries) != 1 || c.bytes != 2 {
		t.Errorf("entries %d bytes %d after Set; want only the new entry", len(c.entries), c.bytes)
	}
}
