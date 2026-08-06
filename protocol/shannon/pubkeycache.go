package shannon

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/pokt-network/shannon-sdk"
)

const (
	// nilPubKeyTTL is how long a "this account has no public key onchain"
	// answer is trusted before the full node is asked again. An account gets a
	// public key the first time it signs a transaction, so unlike a real key
	// this answer can change — but rarely, and asking once per relay is the
	// cost this cache exists to remove.
	nilPubKeyTTL = 15 * time.Minute

	// pubKeyRefetchInterval rate-limits the invalidate-and-refetch path in
	// FullNode.ValidateRelayResponse. Without it a supplier whose signatures
	// never verify would cost two account queries per relay instead of zero.
	pubKeyRefetchInterval = 15 * time.Minute

	// maxPubKeyCacheEntries caps the positive cache. Nothing is evicted
	// otherwise, and nothing needs to be: a public key is immutable once set,
	// so the natural bound is the number of distinct addresses seen (suppliers
	// across served sessions, plus each app and its gateway delegates). The cap
	// is there so a pathological session set cannot grow the map without limit.
	maxPubKeyCacheEntries = 200_000

	// negativeMapSweepThreshold is the size at which the mutex-guarded maps
	// (nil-pubkey answers, refetch timestamps) drop their expired entries.
	// Both are keyed by address and would otherwise retain every supplier that
	// ever misbehaved, for the lifetime of the process.
	negativeMapSweepThreshold = 1024
)

// pubKeyCache is an sdk.PublicKeyFetcher that answers from memory.
//
// Verifying a supplier's signature needs that supplier's public key, and the
// SDK's account client fetches it over gRPC every single call — so an uncached
// fetcher costs one full-node round trip per relay response, and one per
// WebSocket frame. Public keys are immutable once set, which makes them the
// easy case: cache forever, no TTL, no invalidation except the explicit one
// below.
//
// Two things are deliberately not cached forever:
//   - a nil answer (account exists, has never signed a transaction, so has no
//     public key yet) is cached for nilPubKeyTTL only, since it can change;
//   - a fetch error is not cached at all.
type pubKeyCache struct {
	fetcher sdk.PublicKeyFetcher
	logger  *slog.Logger

	// keys holds address → public key. Written once per address.
	keys sync.Map
	// entries counts keys, which sync.Map will not do, to enforce the cap.
	entries atomic.Int64
	// fullLogged keeps the cache-full warning to one line per process.
	fullLogged atomic.Bool

	mu sync.Mutex
	// nilSince records when an address was last found to have no public key.
	nilSince map[string]time.Time
	// lastRefetch records when an address last spent a forced refetch.
	lastRefetch map[string]time.Time
}

// newPubKeyCache wraps fetcher with an in-process cache.
func newPubKeyCache(fetcher sdk.PublicKeyFetcher, logger *slog.Logger) *pubKeyCache {
	return &pubKeyCache{
		fetcher:     fetcher,
		logger:      logger.With("component", "pubkey_cache"),
		nilSince:    make(map[string]time.Time),
		lastRefetch: make(map[string]time.Time),
	}
}

// GetPubKeyFromAddress implements sdk.PublicKeyFetcher.
//
// A nil key with a nil error is a valid answer and means the account has no
// public key onchain; the SDK turns that into
// ErrRelayResponseValidationNilSupplierPubKey.
func (c *pubKeyCache) GetPubKeyFromAddress(ctx context.Context, address string) (cryptotypes.PubKey, error) {
	if cached, ok := c.keys.Load(address); ok {
		return cached.(cryptotypes.PubKey), nil
	}
	if c.nilCached(address) {
		return nil, nil
	}

	pubKey, err := c.fetcher.GetPubKeyFromAddress(ctx, address)
	if err != nil {
		return nil, err
	}
	if pubKey == nil {
		c.markNil(address)
		return nil, nil
	}

	c.store(address, pubKey)
	return pubKey, nil
}

// invalidate drops every cached answer for address, so the next lookup queries
// the full node. Called when a signature fails to verify against the key we
// hold: the key itself cannot have changed, but a nil answer cached before the
// supplier's first transaction can be stale.
func (c *pubKeyCache) invalidate(address string) {
	if _, loaded := c.keys.LoadAndDelete(address); loaded {
		c.entries.Add(-1)
	}
	c.mu.Lock()
	delete(c.nilSince, address)
	c.mu.Unlock()
}

// allowRefetch reports whether address may spend another full-node query on a
// second verification attempt, and records the attempt when it says yes.
func (c *pubKeyCache) allowRefetch(address string) bool {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if last, ok := c.lastRefetch[address]; ok && now.Sub(last) < pubKeyRefetchInterval {
		return false
	}
	if len(c.lastRefetch) >= negativeMapSweepThreshold {
		sweepExpired(c.lastRefetch, now, pubKeyRefetchInterval)
	}
	c.lastRefetch[address] = now
	return true
}

// nilCached reports whether a recent lookup found no public key for address.
func (c *pubKeyCache) nilCached(address string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	since, ok := c.nilSince[address]
	return ok && time.Since(since) < nilPubKeyTTL
}

// markNil records that address has no public key onchain.
func (c *pubKeyCache) markNil(address string) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.nilSince) >= negativeMapSweepThreshold {
		sweepExpired(c.nilSince, now, nilPubKeyTTL)
	}
	c.nilSince[address] = now
}

// store caches a public key, unless the cache is at capacity.
func (c *pubKeyCache) store(address string, pubKey cryptotypes.PubKey) {
	if c.entries.Load() >= maxPubKeyCacheEntries {
		if c.fullLogged.CompareAndSwap(false, true) {
			c.logger.Warn("public key cache is full; further keys will be fetched per relay",
				"max_entries", maxPubKeyCacheEntries,
			)
		}
		return
	}
	if _, loaded := c.keys.LoadOrStore(address, pubKey); !loaded {
		c.entries.Add(1)
	}
}

// sweepExpired drops entries older than ttl. Callers hold the mutex.
func sweepExpired(m map[string]time.Time, now time.Time, ttl time.Duration) {
	for addr, ts := range m {
		if now.Sub(ts) >= ttl {
			delete(m, addr)
		}
	}
}
