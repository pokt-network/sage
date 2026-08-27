package reputation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RedisStorage stores reputation state in Redis using a HASH.
// States are stored as JSON-encoded fields in a hash key identified by the
// configured prefix.
type RedisStorage struct {
	client  redis.Cmdable
	hashKey string
}

// NewRedisStorage creates a new Redis storage backend.
// The hashKey parameter determines the Redis HASH key used for all states
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

// encodeState is the hash field value: JSON, so a field can grow without a
// key-format migration.
func encodeState(st State) string {
	b, _ := json.Marshal(st)
	return string(b)
}

// decodeState accepts the JSON form and the bare float that fields written
// before the rate term hold, so a rolling deploy reads the old pods' scores
// as {score: v}.
func decodeState(val string) (State, error) {
	if len(val) > 0 && val[0] == '{' {
		var st State
		if err := json.Unmarshal([]byte(val), &st); err != nil {
			return State{}, fmt.Errorf("parse state: %w", err)
		}
		return st, nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return State{}, fmt.Errorf("parse legacy score: %w", err)
	}
	return State{Score: f}, nil
}

// GetState retrieves the state for the given key from the Redis HASH.
func (r *RedisStorage) GetState(ctx context.Context, key string) (State, error) {
	val, err := r.client.HGet(ctx, r.hashKey, ScoreField(key)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return State{}, ErrStateNotFound
		}
		return State{}, fmt.Errorf("redis HGet %s: %w", key, err)
	}
	st, err := decodeState(val)
	if err != nil {
		return State{}, fmt.Errorf("decode state for %s: %w", key, err)
	}
	return st, nil
}

// SetState stores the state for the given key in the Redis HASH.
func (r *RedisStorage) SetState(ctx context.Context, key string, st State) error {
	err := r.client.HSet(ctx, r.hashKey, ScoreField(key), encodeState(st)).Err()
	if err != nil {
		return fmt.Errorf("redis HSet %s: %w", key, err)
	}
	return nil
}

// GetStates retrieves all states from the Redis HASH whose field names begin
// with the given prefix. Fields that fail to decode are skipped.
func (r *RedisStorage) GetStates(ctx context.Context, prefix string) (map[string]State, error) {
	all, err := r.client.HGetAll(ctx, r.hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGetAll: %w", err)
	}
	result := make(map[string]State)
	for field, val := range all {
		if len(prefix) > 0 && len(field) < len(prefix) {
			continue
		}
		if len(prefix) > 0 && field[:len(prefix)] != prefix {
			continue
		}
		st, parseErr := decodeState(val)
		if parseErr != nil {
			continue
		}
		result[field] = st
	}
	return result, nil
}

// DeleteState removes the state for the given key from the Redis HASH.
func (r *RedisStorage) DeleteState(ctx context.Context, key string) error {
	err := r.client.HDel(ctx, r.hashKey, ScoreField(key)).Err()
	if err != nil {
		return fmt.Errorf("redis HDel %s: %w", key, err)
	}
	return nil
}
