package reputation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient is the subset of redis.Cmdable that RedisStorage uses. Hash
// commands only — the whole store is one HASH, so nothing keyspace-wide is
// ever needed (no SCAN, no KEYS; see drain.RedisStore for why that matters).
type RedisClient interface {
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd
}

var _ RedisClient = (redis.Cmdable)(nil)

const (
	// staleScanCount is the HSCAN page size. 1,000 fields of ~100 bytes is a
	// ~100KB reply, small enough to be one round trip and large enough that a
	// 100k-field hash is swept in ~100 calls.
	staleScanCount = 1000
	// staleDelBatch bounds one HDEL's argument list.
	staleDelBatch = 500
)

// RedisStorage stores reputation state in Redis using a HASH.
// States are stored as JSON-encoded fields in a hash key identified by the
// configured prefix.
type RedisStorage struct {
	client  RedisClient
	hashKey string
}

// NewRedisStorage creates a new Redis storage backend.
// The hashKey parameter determines the Redis HASH key used for all states
// (e.g., "path:reputation:scores").
// Returns an error if client is nil.
func NewRedisStorage(client RedisClient, hashKey string) (*RedisStorage, error) {
	if client == nil {
		return nil, errors.New("redis client must not be nil")
	}
	return &RedisStorage{
		client:  client,
		hashKey: hashKey,
	}, nil
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

// operatorHashKey is the HASH holding per-operator evidence, beside the
// per-key scores. A separate hash rather than reserved fields in the same one:
// the score hash is swept by an idle TTL tuned to keys that rotate out of a
// session, and operator evidence must not be swept on that rule — an operator
// is still the same operator after a quiet hour.
func (r *RedisStorage) operatorHashKey() string {
	return r.hashKey + "operators"
}

// GetOperatorStats reads every stored operator stat.
func (r *RedisStorage) GetOperatorStats(ctx context.Context) (map[string]OperatorStat, error) {
	all, err := r.client.HGetAll(ctx, r.operatorHashKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGetAll %s: %w", r.operatorHashKey(), err)
	}
	out := make(map[string]OperatorStat, len(all))
	for field, val := range all {
		var st OperatorStat
		if err := json.Unmarshal([]byte(val), &st); err != nil {
			continue // a field we cannot read is skipped, like a bad state
		}
		out[field] = st
	}
	return out, nil
}

// SetOperatorStat writes one operator stat.
func (r *RedisStorage) SetOperatorStat(ctx context.Context, field string, st OperatorStat) error {
	b, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encode operator stat %s: %w", field, err)
	}
	if err := r.client.HSet(ctx, r.operatorHashKey(), field, string(b)).Err(); err != nil {
		return fmt.Errorf("redis HSet %s: %w", field, err)
	}
	return nil
}

// GetState retrieves the state for the given key from the Redis HASH.
func (r *RedisStorage) GetState(ctx context.Context, key string) (State, error) {
	val, err := r.client.HGet(ctx, r.hashKey, key).Result()
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
	err := r.client.HSet(ctx, r.hashKey, key, encodeState(st)).Err()
	if err != nil {
		return fmt.Errorf("redis HSet %s: %w", key, err)
	}
	return nil
}

// SetStates writes every state in one HSET.
//
// The whole store is one hash and a state is one field in it, so a batch needs
// no MULTI and no pipeline: HSET takes field/value pairs variadically and Redis
// applies them atomically as one command. That turns the write-behind's ceiling
// from one round trip per write into one per batch.
func (r *RedisStorage) SetStates(ctx context.Context, states map[string]State) error {
	if len(states) == 0 {
		return nil
	}
	pairs := make([]any, 0, len(states)*2)
	for key, st := range states {
		pairs = append(pairs, key, encodeState(st))
	}
	if err := r.client.HSet(ctx, r.hashKey, pairs...).Err(); err != nil {
		return fmt.Errorf("redis HSet %d fields: %w", len(states), err)
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
	err := r.client.HDel(ctx, r.hashKey, key).Err()
	if err != nil {
		return fmt.Errorf("redis HDel %s: %w", key, err)
	}
	return nil
}

// DeleteStale implements StaleDeleter: one HSCAN pass over the hash, deleting
// fields whose UpdatedAt is before olderThan in batches. A field that does not
// decode, or carries no UpdatedAt (written before the stamp existed), counts
// as stale — this is what drains the hash a pre-stamp pod filled.
//
// A hash field cannot expire on its own before Redis 7.4 (HEXPIRE), and the
// gateway does not get to assume the Redis version, so expiry is the field's
// own timestamp and a sweep, the same shape as drain.RedisStore.
func (r *RedisStorage) DeleteStale(ctx context.Context, olderThan time.Time) (int, error) {
	cutoff := olderThan.Unix()
	deleted := 0
	var cursor uint64
	stale := make([]string, 0, staleDelBatch)
	flush := func() error {
		if len(stale) == 0 {
			return nil
		}
		n, err := r.client.HDel(ctx, r.hashKey, stale...).Result()
		if err != nil {
			return fmt.Errorf("redis HDel stale: %w", err)
		}
		deleted += int(n)
		stale = stale[:0]
		return nil
	}
	for {
		page, next, err := r.client.HScan(ctx, r.hashKey, cursor, "", staleScanCount).Result()
		if err != nil {
			return deleted, fmt.Errorf("redis HScan: %w", err)
		}
		// HSCAN returns field, value, field, value, ...
		for i := 0; i+1 < len(page); i += 2 {
			st, decErr := decodeState(page[i+1])
			if decErr != nil || st.UpdatedAt < cutoff {
				stale = append(stale, page[i])
				if len(stale) >= staleDelBatch {
					if err := flush(); err != nil {
						return deleted, err
					}
				}
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return deleted, flush()
}

// partiesKey holds the leader's priced parties: one small JSON value in its
// own hash (partiesField), beside the score and operator hashes and, like the
// operator hash, outside the score sweep.
func (r *RedisStorage) partiesKey() string {
	return r.hashKey + "parties"
}

// partiesField is the one field of partiesKey.
const partiesField = "v"

// GetPartyPenalties reads the stored priced parties; none when absent.
func (r *RedisStorage) GetPartyPenalties(ctx context.Context) (PartyPenalties, error) {
	val, err := r.client.HGet(ctx, r.partiesKey(), partiesField).Result()
	if errors.Is(err, redis.Nil) {
		return PartyPenalties{}, nil
	}
	if err != nil {
		return PartyPenalties{}, fmt.Errorf("redis HGET %s: %w", r.partiesKey(), err)
	}
	var p PartyPenalties
	if err := json.Unmarshal([]byte(val), &p); err != nil {
		return PartyPenalties{}, fmt.Errorf("decode %s: %w", r.partiesKey(), err)
	}
	return p, nil
}

// notesKey holds each pod's notification counts, one field per pod
// (NotificationCountStore).
func (r *RedisStorage) notesKey() string {
	return r.hashKey + "notifications"
}

// PutNotificationCounts replaces one pod's notification counts.
func (r *RedisStorage) PutNotificationCounts(ctx context.Context, pod string, c NotificationCounts) error {
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode %s: %w", r.notesKey(), err)
	}
	if err := r.client.HSet(ctx, r.notesKey(), pod, string(b)).Err(); err != nil {
		return fmt.Errorf("redis HSET %s: %w", r.notesKey(), err)
	}
	return nil
}

// NotificationCounts reads every pod's notification counts; a field that does
// not decode is skipped.
func (r *RedisStorage) NotificationCounts(ctx context.Context) (map[string]NotificationCounts, error) {
	raw, err := r.client.HGetAll(ctx, r.notesKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGETALL %s: %w", r.notesKey(), err)
	}
	out := make(map[string]NotificationCounts, len(raw))
	for pod, val := range raw {
		var c NotificationCounts
		if json.Unmarshal([]byte(val), &c) == nil {
			out[pod] = c
		}
	}
	return out, nil
}

// DeleteNotificationCounts drops the named pods' counts.
func (r *RedisStorage) DeleteNotificationCounts(ctx context.Context, pods ...string) error {
	if len(pods) == 0 {
		return nil
	}
	if err := r.client.HDel(ctx, r.notesKey(), pods...).Err(); err != nil {
		return fmt.Errorf("redis HDEL %s: %w", r.notesKey(), err)
	}
	return nil
}

// SetPartyPenalties replaces the stored priced parties.
func (r *RedisStorage) SetPartyPenalties(ctx context.Context, p PartyPenalties) error {
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode %s: %w", r.partiesKey(), err)
	}
	if err := r.client.HSet(ctx, r.partiesKey(), partiesField, string(b)).Err(); err != nil {
		return fmt.Errorf("redis HSET %s: %w", r.partiesKey(), err)
	}
	return nil
}

// policyKey holds the policy penalties, one field per party
// (PolicyPenaltyStore).
func (r *RedisStorage) policyKey() string {
	return r.hashKey + "policy"
}

// PutPolicyPenalty stores a party's policy penalty, replacing its previous one.
func (r *RedisStorage) PutPolicyPenalty(ctx context.Context, p PolicyPenalty) error {
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode %s: %w", r.policyKey(), err)
	}
	if err := r.client.HSet(ctx, r.policyKey(), p.Party, string(b)).Err(); err != nil {
		return fmt.Errorf("redis HSET %s: %w", r.policyKey(), err)
	}
	return nil
}

// PolicyPenalties reads every stored policy penalty; a field that does not
// decode is skipped.
func (r *RedisStorage) PolicyPenalties(ctx context.Context) ([]PolicyPenalty, error) {
	raw, err := r.client.HGetAll(ctx, r.policyKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGETALL %s: %w", r.policyKey(), err)
	}
	out := make([]PolicyPenalty, 0, len(raw))
	for _, val := range raw {
		var p PolicyPenalty
		if json.Unmarshal([]byte(val), &p) == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

// DeletePolicyPenalty removes a party's policy penalty.
func (r *RedisStorage) DeletePolicyPenalty(ctx context.Context, party string) (bool, error) {
	n, err := r.client.HDel(ctx, r.policyKey(), party).Result()
	if err != nil {
		return false, fmt.Errorf("redis HDEL %s: %w", r.policyKey(), err)
	}
	return n > 0, nil
}
