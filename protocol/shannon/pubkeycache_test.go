package shannon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
)

func TestPubKeyCache_ServesRepeatLookupsFromMemory(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{key.address: key.pub}}
	cache := newPubKeyCache(fetcher, newTestLogger())

	for i := 0; i < 100; i++ {
		got, err := cache.GetPubKeyFromAddress(t.Context(), key.address)
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if !got.Equals(key.pub) {
			t.Fatalf("lookup %d returned the wrong key", i)
		}
	}

	if got := fetcher.callCount(); got != 1 {
		t.Errorf("fetched %d times, want 1", got)
	}
}

// An account with no public key is a real answer, but a temporary one: it
// changes the first time that account signs a transaction. Cache it briefly to
// keep it off the hot path, not forever.
func TestPubKeyCache_NegativeAnswerIsCachedButInvalidatable(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{}}
	cache := newPubKeyCache(fetcher, newTestLogger())

	for i := 0; i < 10; i++ {
		got, err := cache.GetPubKeyFromAddress(t.Context(), key.address)
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if got != nil {
			t.Fatalf("lookup %d: want nil key", i)
		}
	}
	if got := fetcher.callCount(); got != 1 {
		t.Fatalf("fetched %d times for a nil answer, want 1", got)
	}

	// The supplier signs its first transaction; invalidation is what lets us
	// see that before the TTL runs out.
	fetcher.setAnswer(key.address, key.pub)
	cache.invalidate(key.address)

	got, err := cache.GetPubKeyFromAddress(t.Context(), key.address)
	if err != nil {
		t.Fatalf("lookup after invalidate: %v", err)
	}
	if got == nil || !got.Equals(key.pub) {
		t.Error("invalidated address should have been re-fetched")
	}
}

func TestPubKeyCache_FetchErrorsAreNotCached(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{
		answers: map[string]cryptotypes.PubKey{},
		err:     errors.New("full node unreachable"),
	}
	cache := newPubKeyCache(fetcher, newTestLogger())

	for i := 0; i < 3; i++ {
		if _, err := cache.GetPubKeyFromAddress(t.Context(), key.address); err == nil {
			t.Fatalf("lookup %d: want error", i)
		}
	}
	if got := fetcher.callCount(); got != 3 {
		t.Errorf("fetched %d times, want 3 — an outage must not be cached", got)
	}
}

func TestPubKeyCache_AllowRefetchIsRateLimitedPerAddress(t *testing.T) {
	cache := newPubKeyCache(&fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{}}, newTestLogger())

	if !cache.allowRefetch("pokt1a") {
		t.Fatal("first refetch should be allowed")
	}
	if cache.allowRefetch("pokt1a") {
		t.Error("second refetch within the interval should be refused")
	}
	// The limit is per supplier: one bad actor must not block another's retry.
	if !cache.allowRefetch("pokt1b") {
		t.Error("a different supplier should still get its first refetch")
	}

	// Reaching back past the interval is the same as time passing.
	cache.mu.Lock()
	cache.lastRefetch["pokt1a"] = time.Now().Add(-2 * pubKeyRefetchInterval)
	cache.mu.Unlock()

	if !cache.allowRefetch("pokt1a") {
		t.Error("refetch should be allowed again once the interval has passed")
	}
}

func TestPubKeyCache_ConcurrentLookups(t *testing.T) {
	key := newSupplierKey()
	fetcher := &fakePubKeyFetcher{answers: map[string]cryptotypes.PubKey{key.address: key.pub}}
	cache := newPubKeyCache(fetcher, newTestLogger())

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < 20; j++ {
				if _, err := cache.GetPubKeyFromAddress(ctx, key.address); err != nil {
					t.Errorf("goroutine %d: %v", i, err)
					return
				}
				if i%4 == 0 {
					cache.invalidate(key.address)
					cache.allowRefetch(key.address)
				}
			}
		}(i)
	}
	wg.Wait()

	if cache.entries.Load() < 0 {
		t.Errorf("entry count went negative: %d", cache.entries.Load())
	}
}
