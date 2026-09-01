package blocklist

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// hashKey is the one Redis HASH every admin-set ban lives in, field = domain.
// One key, not one per ban, for the reason drain gives: the poll runs on every
// replica every interval, and one HGETALL costs what the bans cost.
const hashKey = "sage:blocked_domains"

// RedisClient is the subset of redis.Cmdable RedisBackend uses.
type RedisClient interface {
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
}

var _ RedisClient = (*redis.Client)(nil)

// RedisBackend persists admin entries in Redis so every replica sees them
// and they outlive a restart.
type RedisBackend struct {
	client RedisClient
}

// NewRedisBackend returns a backend over client.
func NewRedisBackend(client RedisClient) *RedisBackend {
	return &RedisBackend{client: client}
}

// Save implements Backend.
func (b *RedisBackend) Save(ctx context.Context, e Entry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if err := b.client.HSet(ctx, hashKey, e.Domain, string(raw)).Err(); err != nil {
		return fmt.Errorf("redis HSet %s: %w", e.Domain, err)
	}
	return nil
}

// Delete implements Backend.
func (b *RedisBackend) Delete(ctx context.Context, domain string) error {
	if err := b.client.HDel(ctx, hashKey, domain).Err(); err != nil {
		return fmt.Errorf("redis HDel %s: %w", domain, err)
	}
	return nil
}

// Load implements Backend. A field that does not decode is skipped, not
// guessed at; the others still load.
func (b *RedisBackend) Load(ctx context.Context) ([]Entry, error) {
	all, err := b.client.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGetAll: %w", err)
	}
	out := make([]Entry, 0, len(all))
	for field, raw := range all {
		var e Entry
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			continue
		}
		if e.Domain == "" {
			e.Domain = field
		}
		out = append(out, e)
	}
	return out, nil
}
