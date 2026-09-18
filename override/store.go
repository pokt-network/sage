// Package override persists operator overrides — the things an admin route
// changes on a running gateway — so they outlive the process that took the
// call and reach every replica.
//
// Until 2026-09-13 the runtime seams were per process and in memory: a
// log-level change, a replaced external block source, a tuning override
// lived on the pod that received the PUT and died with it, so a fix made at
// 11:16 was silently undone by the 12:43 roll, and had to be made on each pod
// by hand. Feature flags did not have that problem because they went through
// Redis. This package gives the other seams the same home.
//
// The store is a flat string map under a key prefix. Each seam owns a key
// space ("log_level", "external_sources/<service>", "tuning/<knob>[/service]",
// "config") and decides what the value means. Watch polls a key space and
// hands the whole map to an apply function when it changes, which is how a
// replica that did not take the call learns about it, and how a restarted
// process re-applies what was set before it came up.
//
// Without Redis the memory store stands in: same interface, per process,
// lost on restart — the admin routes say which they have (Shared).
package override

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/internal/safego"
)

// Store is a durable string map. Keys are slash-separated paths chosen by the
// seam that owns them.
type Store interface {
	// Get returns the value under key; ok is false when there is none.
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	// Set writes value under key.
	Set(ctx context.Context, key, value string) error
	// Delete removes key; removing an absent key is not an error.
	Delete(ctx context.Context, key string) error
	// List returns every key with the prefix and its value.
	List(ctx context.Context, prefix string) (map[string]string, error)
	// Shared reports whether writes reach other replicas and survive a
	// restart (Redis) or live in this process only (memory).
	Shared() bool
}

// MemoryStore is the per-process fallback when Redis is not configured or
// not reachable. Safe for concurrent use.
type MemoryStore struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{m: make(map[string]string)} }

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok, nil
}

// Set implements Store.
func (s *MemoryStore) Set(_ context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

// List implements Store.
func (s *MemoryStore) List(_ context.Context, prefix string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string)
	for k, v := range s.m {
		if strings.HasPrefix(k, prefix) {
			out[k] = v
		}
	}
	return out, nil
}

// Shared implements Store: a MemoryStore is this process only.
func (s *MemoryStore) Shared() bool { return false }

// keyPrefix namespaces the store in Redis, beside the flag store's
// "sage:flags:".
const keyPrefix = "sage:overrides:"

// prefixOr returns the store's prefix, or the historical default when it was
// built without one.
func (s *RedisStore) prefixOr() string {
	if s.prefix == "" {
		return keyPrefix
	}
	return s.prefix
}

// RedisClient is the slice of go-redis the RedisStore uses; *redis.Client
// satisfies it, and so does a fake in tests.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
}

// RedisStore keeps overrides in Redis, shared by every replica on the same
// database and kept across restarts. Values do not expire: an override
// stands until an operator deletes it, which is the point.
type RedisStore struct {
	client RedisClient
	// prefix namespaces this store's keys. Empty means keyPrefix, the literal
	// every release before the prefix was configurable used.
	prefix string
}

// NewRedisStore wraps a client.
func NewRedisStore(client RedisClient, prefix string) *RedisStore {
	return &RedisStore{client: client, prefix: prefix}
}

// Get implements Store.
func (s *RedisStore) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := s.client.Get(ctx, s.prefixOr()+key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// Set implements Store.
func (s *RedisStore) Set(ctx context.Context, key, value string) error {
	return s.client.Set(ctx, s.prefixOr()+key, value, 0).Err()
}

// Delete implements Store.
func (s *RedisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.prefixOr()+key).Err()
}

// List implements Store. A SCAN over the prefix, then one GET per key; the
// key spaces here hold tens of entries, not thousands.
func (s *RedisStore) List(ctx context.Context, prefix string) (map[string]string, error) {
	out := make(map[string]string)
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, s.prefixOr()+prefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			v, err := s.client.Get(ctx, k).Result()
			if errors.Is(err, redis.Nil) {
				continue // deleted between the scan and the get
			}
			if err != nil {
				return nil, err
			}
			out[strings.TrimPrefix(k, s.prefixOr())] = v
		}
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

// Shared implements Store: Redis reaches every replica and survives restarts.
func (s *RedisStore) Shared() bool { return true }

// DefaultWatchInterval is how often Watch polls; the same order as the flag
// store's cache TTL, so a change made on one replica reaches the others in
// seconds.
const DefaultWatchInterval = 5 * time.Second

// Watch polls store for every key under prefix every interval and calls apply
// with the whole map when it differs from the last one applied. The first
// poll always applies, so a process that starts with overrides in the store
// picks them up. apply runs on the watcher's goroutine and must be quick and
// idempotent: it sees the desired state, not a delta. Watch returns at once;
// the goroutine ends when ctx is cancelled. A store error is logged at debug
// and the previous state stands.
func Watch(ctx context.Context, logger *slog.Logger, store Store, prefix string, interval time.Duration, apply func(map[string]string)) {
	if store == nil || apply == nil {
		return
	}
	if interval <= 0 {
		interval = DefaultWatchInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	safego.Go(logger, "override.watch."+strings.Trim(prefix, "/"), func() {
		var last map[string]string
		first := true
		tick := func() {
			cur, err := store.List(ctx, prefix)
			if err != nil {
				logger.Debug("override watch: list failed; keeping the last applied state", "prefix", prefix, "error", err)
				return
			}
			if !first && maps.Equal(cur, last) {
				return
			}
			first = false
			last = cur
			safego.Run(logger, "override.apply."+strings.Trim(prefix, "/"), func() { apply(cur) })
		}
		tick()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				tick()
			}
		}
	})
}
