package featureflag

import (
	"context"
	"fmt"
	"sync"
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
	defaults map[string]bool

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
		defaults: overrides,
		cache:    make(map[string]cacheEntry),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

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
	if val, ok := s.defaults[flag]; ok {
		return val
	}
	return DefaultFlags[flag]
}

func (s *RedisStore) Set(ctx context.Context, flag string, enabled bool) error {
	return s.set(ctx, globalKey(flag), enabled)
}

func (s *RedisStore) SetForService(ctx context.Context, flag string, serviceID domain.ServiceID, enabled bool) error {
	return s.set(ctx, serviceKey(flag, serviceID), enabled)
}

func (s *RedisStore) GetAll(ctx context.Context) (map[string]FlagState, error) {
	result := make(map[string]FlagState)

	// Seed with defaults.
	for flag, enabled := range DefaultFlags {
		result[flag] = FlagState{Enabled: enabled}
	}
	for flag, enabled := range s.defaults {
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
