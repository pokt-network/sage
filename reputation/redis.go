package reputation

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RedisStorage stores reputation scores in Redis using a HASH.
// Scores are stored as fields in a hash key identified by the configured prefix.
type RedisStorage struct {
	client  redis.Cmdable
	hashKey string
}

// NewRedisStorage creates a new Redis storage backend.
// The hashKey parameter determines the Redis HASH key used for all scores
// (e.g., "path:reputation:scores").
// Returns an error if client is nil.
func NewRedisStorage(client redis.Cmdable, hashKey string) (*RedisStorage, error) {
	if client == nil {
		return nil, errors.New("redis client must not be nil")
	}
	return &RedisStorage{
		client:  client,
		hashKey: hashKey,
	}, nil
}

// ScoreField returns the Redis HASH field name for a given score key.
func ScoreField(key string) string {
	return key
}

// GetScore retrieves the score for the given key from the Redis HASH.
func (r *RedisStorage) GetScore(ctx context.Context, key string) (float64, error) {
	val, err := r.client.HGet(ctx, r.hashKey, ScoreField(key)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, ErrScoreNotFound
		}
		return 0, fmt.Errorf("redis HGet %s: %w", key, err)
	}
	score, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("parse score for %s: %w", key, err)
	}
	return score, nil
}

// SetScore stores the score for the given key in the Redis HASH.
func (r *RedisStorage) SetScore(ctx context.Context, key string, score float64) error {
	err := r.client.HSet(ctx, r.hashKey, ScoreField(key), strconv.FormatFloat(score, 'f', -1, 64)).Err()
	if err != nil {
		return fmt.Errorf("redis HSet %s: %w", key, err)
	}
	return nil
}

// GetScores retrieves all scores from the Redis HASH whose field names begin
// with the given prefix.
func (r *RedisStorage) GetScores(ctx context.Context, prefix string) (map[string]float64, error) {
	all, err := r.client.HGetAll(ctx, r.hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGetAll: %w", err)
	}
	result := make(map[string]float64)
	for field, val := range all {
		if len(prefix) > 0 && len(field) < len(prefix) {
			continue
		}
		if len(prefix) > 0 && field[:len(prefix)] != prefix {
			continue
		}
		score, parseErr := strconv.ParseFloat(val, 64)
		if parseErr != nil {
			continue
		}
		result[field] = score
	}
	return result, nil
}

// DeleteScore removes the score for the given key from the Redis HASH.
func (r *RedisStorage) DeleteScore(ctx context.Context, key string) error {
	err := r.client.HDel(ctx, r.hashKey, ScoreField(key)).Err()
	if err != nil {
		return fmt.Errorf("redis HDel %s: %w", key, err)
	}
	return nil
}
