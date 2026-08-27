package featureflag

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/redis/go-redis/v9"
)

const (
	defaultCacheTTL = 5 * time.Second
	keyPrefix       = "sage:flags:"
)

// RedisClient is the subset of redis.Cmdable used by RedisStore.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
}

type cacheEntry struct {
	value     bool
	found     bool // false: key absent in Redis (negative entry) — fall back to defaults
	expiresAt time.Time
}

// RedisStore is a Redis-backed FlagStore with a local cache.
type RedisStore struct {
	client   RedisClient
	cacheTTL time.Duration

	// defaults is the config file's override layer, below the Redis keys and
	// above DefaultFlags.
	//
	// A pointer swapped whole rather than a plain map because a config reload
	// removes entries from it (see DeleteGlobal) while relays are reading it.
	// Copy-on-write keeps the read side free of locking, which matters: this
	// is consulted on every flag check that misses the cache.
	defaults atomic.Pointer[map[string]bool]

	mu    sync.RWMutex
	cache map[string]cacheEntry
}

// RedisStoreOption configures a RedisStore.
type RedisStoreOption func(*RedisStore)

// WithCacheTTL sets the local cache TTL.
func WithCacheTTL(ttl time.Duration) RedisStoreOption {
	return func(s *RedisStore) { s.cacheTTL = ttl }
}

// NewRedisStore creates a Redis-backed flag store.
// If client is nil, all reads fall back to defaults.
//
// overrides are the flags set in config (a partial map); any flag not present
// falls back to DefaultFlags at read time. Pass nil to use the compiled defaults
// for everything.
func NewRedisStore(client RedisClient, overrides map[string]bool, opts ...RedisStoreOption) *RedisStore {
	s := &RedisStore{
		client:   client,
		cacheTTL: defaultCacheTTL,
		cache:    make(map[string]cacheEntry),
	}
	copied := make(map[string]bool, len(overrides))
	for flag, enabled := range overrides {
		copied[flag] = enabled
	}
	s.defaults.Store(&copied)
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// configOverride reports the config file's value for a flag, if it set one.
func (s *RedisStore) configOverride(flag string) (bool, bool) {
	overrides := s.defaults.Load()
	if overrides == nil {
		return false, false
	}
	value, ok := (*overrides)[flag]
	return value, ok
}

// IsEnabled resolves a flag in precedence order: the per-service key in Redis,
// the global key in Redis, the config overrides, then compiled DefaultFlags.
//
// Reads go through the local cache, which caches misses and Redis errors as
// well as hits — so an unreachable Redis degrades to defaults for one cache
// TTL rather than putting a round trip on every relay. A nil client is the same
// path, permanently. Redis is optional here as everywhere on the hot path.
func (s *RedisStore) IsEnabled(ctx context.Context, flag string, serviceID domain.ServiceID) bool {
	// Try per-service override first.
	if serviceID != "" {
		if val, ok := s.get(ctx, serviceKey(flag, serviceID)); ok {
			return val
		}
	}

	// Try global.
	if val, ok := s.get(ctx, globalKey(flag)); ok {
		return val
	}

	// Fall back to config defaults, then compiled defaults.
	if val, ok := s.configOverride(flag); ok {
		return val
	}
	return DefaultFlags[flag]
}

// Set changes a flag globally across every instance sharing this Redis. The
// local cache is updated first, so the calling instance sees the change even if
// the write fails; peers pick it up within their own cache TTL.
func (s *RedisStore) Set(ctx context.Context, flag string, enabled bool) error {
	return s.set(ctx, globalKey(flag), enabled)
}

// SetForService overrides a flag for one service, taking precedence over the
// global key for that service only.
func (s *RedisStore) SetForService(ctx context.Context, flag string, serviceID domain.ServiceID, enabled bool) error {
	return s.set(ctx, serviceKey(flag, serviceID), enabled)
}

// GetAll returns the effective state of every known flag, layering the Redis
// keys onto the config overrides and DefaultFlags.
//
// Unlike IsEnabled this bypasses the cache and scans Redis, so it reflects
// what peers have set right now — it is an admin read, not a hot path. On a
// scan error it returns the defaults it has along with the error rather than
// nothing, so the caller can still show something useful.
func (s *RedisStore) GetAll(ctx context.Context) (map[string]FlagState, error) {
	result := make(map[string]FlagState)

	// Seed with defaults.
	for flag, enabled := range DefaultFlags {
		result[flag] = FlagState{Enabled: enabled}
	}
	for flag, enabled := range *s.defaults.Load() {
		st := result[flag]
		st.Enabled = enabled
		result[flag] = st
	}

	if s.client == nil {
		return result, nil
	}

	// Scan Redis keys for global and per-service flags.
	keys, err := s.client.Keys(ctx, keyPrefix+"*").Result()
	if err != nil {
		return result, fmt.Errorf("redis keys scan: %w", err)
	}

	for _, key := range keys {
		val, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		enabled := val == "1"
		flag, svcID := parseKey(key)
		if flag == "" {
			continue
		}

		st := result[flag]
		if svcID == "" {
			st.Enabled = enabled
		} else {
			if st.ServiceOverrides == nil {
				st.ServiceOverrides = make(map[domain.ServiceID]bool)
			}
			st.ServiceOverrides[domain.ServiceID(svcID)] = enabled
		}
		result[flag] = st
	}

	return result, nil
}

// Delete removes a flag key so it falls back to the config override or the
// compiled default. An empty serviceID targets the global key; a set one
// targets that service's override.
func (s *RedisStore) Delete(ctx context.Context, flag string, serviceID domain.ServiceID) error {
	var key string
	if serviceID != "" {
		key = serviceKey(flag, serviceID)
	} else {
		key = globalKey(flag)
	}

	s.mu.Lock()
	delete(s.cache, key)
	s.mu.Unlock()

	if s.client == nil {
		return nil
	}
	return s.client.Del(ctx, key).Err()
}

// DeleteGlobal removes every non-per-service source of a flag's value: the
// global Redis key and the config file's override. Per-service keys are left
// alone.
//
// Dropping the config layer too is the whole point. It is captured from the
// file at construction, so deleting only the Redis key would leave a reload
// falling straight back onto the value the operator just removed from the
// file — a removal that reports success and changes nothing.
func (s *RedisStore) DeleteGlobal(ctx context.Context, flag string) error {
	for {
		current := s.defaults.Load()
		if _, present := (*current)[flag]; !present {
			break
		}
		next := make(map[string]bool, len(*current))
		for name, enabled := range *current {
			if name != flag {
				next[name] = enabled
			}
		}
		if s.defaults.CompareAndSwap(current, &next) {
			break
		}
	}

	key := globalKey(flag)
	s.mu.Lock()
	delete(s.cache, key)
	s.mu.Unlock()

	if s.client == nil {
		return nil
	}
	return s.client.Del(ctx, key).Err()
}

// get reads from cache first, then Redis. Returns (value, found).
func (s *RedisStore) get(ctx context.Context, key string) (bool, bool) {
	// Check cache.
	s.mu.RLock()
	if entry, ok := s.cache[key]; ok && time.Now().Before(entry.expiresAt) {
		s.mu.RUnlock()
		return entry.value, entry.found
	}
	s.mu.RUnlock()

	if s.client == nil {
		return false, false
	}

	// Read from Redis.
	val, err := s.client.Get(ctx, key).Result()
	if err != nil {
		// Cache the miss (and errors) too: IsEnabled probes two keys per flag
		// and a dozen flag-gated middlewares sit on the relay path, so an
		// uncached miss means Redis round trips on every relay. A Redis outage
		// degrades to defaults for one TTL instead of stalling the hot path.
		s.mu.Lock()
		s.cache[key] = cacheEntry{expiresAt: time.Now().Add(s.cacheTTL)}
		s.mu.Unlock()
		return false, false
	}

	enabled := val == "1"

	// Update cache.
	s.mu.Lock()
	s.cache[key] = cacheEntry{value: enabled, found: true, expiresAt: time.Now().Add(s.cacheTTL)}
	s.mu.Unlock()

	return enabled, true
}

func (s *RedisStore) set(ctx context.Context, key string, enabled bool) error {
	val := "0"
	if enabled {
		val = "1"
	}

	s.mu.Lock()
	s.cache[key] = cacheEntry{value: enabled, found: true, expiresAt: time.Now().Add(s.cacheTTL)}
	s.mu.Unlock()

	if s.client == nil {
		return nil
	}
	return s.client.Set(ctx, key, val, 0).Err()
}

func globalKey(flag string) string {
	return keyPrefix + flag
}

func serviceKey(flag string, serviceID domain.ServiceID) string {
	return keyPrefix + flag + ":" + string(serviceID)
}

// parseKey extracts flag name and optional serviceID from a Redis key.
// Key format: "sage:flags:{flag}" or "sage:flags:{flag}:{serviceID}"
func parseKey(key string) (flag string, serviceID string) {
	if len(key) <= len(keyPrefix) {
		return "", ""
	}
	rest := key[len(keyPrefix):]
	// Find the first colon which separates flag from serviceID.
	for i := 0; i < len(rest); i++ {
		if rest[i] == ':' {
			return rest[:i], rest[i+1:]
		}
	}
	return rest, ""
}
