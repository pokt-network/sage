// Package cooldown holds keys refused for a while, on every replica.
//
// A key held on one replica must be refused on all of them: a client a
// replica closes reconnects through the load balancer, and most likely lands
// on another one. The Redis copy is that reach; a process-local copy answers
// the replica that wrote it without a round trip, and is all there is when
// Redis is not configured.
//
// Reads fail open. A Redis that errors or is slow costs the cooldown, never a
// client its connection.
package cooldown

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisTimeout bounds one Redis call; a cooldown is read on the upgrade path.
const redisTimeout = 250 * time.Millisecond

// RedisClient is the slice of the Redis API the store uses.
type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	PTTL(ctx context.Context, key string) *redis.DurationCmd
}

var _ RedisClient = (*redis.Client)(nil)

// Store holds keys until their cooldown ends. Safe for concurrent use.
type Store struct {
	client RedisClient // nil: this replica only
	prefix string
	now    func() time.Time

	mu   sync.Mutex
	held map[string]time.Time // key -> end of its cooldown
}

// New returns a Store that shares its keys through client under prefix; a nil
// client keeps them on this replica.
func New(client RedisClient, prefix string) *Store {
	return &Store{client: client, prefix: prefix, now: time.Now, held: make(map[string]time.Time)}
}

// Hold refuses key for d. It is held on this replica whatever Redis says; the
// error is Redis's, and means the other replicas do not hold it.
func (s *Store) Hold(ctx context.Context, key string, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	s.hold(key, s.now().Add(d))
	if s.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	return s.client.Set(ctx, s.prefix+key, 1, d).Err()
}

// Remaining is how long key is still refused, 0 when it is not. The error is
// Redis's; the answer is then this replica's alone.
func (s *Store) Remaining(ctx context.Context, key string) (time.Duration, error) {
	now := s.now()
	s.mu.Lock()
	until, ok := s.held[key]
	s.mu.Unlock()
	if ok && until.After(now) {
		return until.Sub(now), nil
	}
	if s.client == nil {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	left, err := s.client.PTTL(ctx, s.prefix+key).Result()
	if err != nil || left <= 0 { // -2: no key, -1: no expiry, neither of which Hold writes
		return 0, err
	}
	// Kept here too, so a client retrying through its cooldown costs one
	// Redis read per replica rather than one per attempt.
	s.hold(key, now.Add(left))
	return left, nil
}

// hold records key until until, dropping the entries that have ended so the
// map holds only live cooldowns.
func (s *Store) hold(key string, until time.Time) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, u := range s.held {
		if !u.After(now) {
			delete(s.held, k)
		}
	}
	s.held[key] = until
}
