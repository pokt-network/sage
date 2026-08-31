package healthcheck

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/internal/safego"
)

const (
	leaderKey     = "sage:leader:healthcheck"
	leaderTTL     = 30 * time.Second
	renewInterval = leaderTTL / 3
)

// LeaderElector uses Redis SET NX to elect a single leader among replicas.
// When Redis is unavailable, the instance acts as leader (local-only mode).
type LeaderElector struct {
	redis    *redis.Client
	key      string
	id       string
	ttl      time.Duration
	logger   *slog.Logger
	isLeader atomic.Bool
	cancel   context.CancelFunc
}

// NewLeaderElector creates a LeaderElector. Pass nil for redisClient to run
// in local-only mode (always leader).
func NewLeaderElector(redisClient *redis.Client, logger *slog.Logger) *LeaderElector {
	return &LeaderElector{
		redis:  redisClient,
		key:    leaderKey,
		id:     instanceID(),
		ttl:    leaderTTL,
		logger: logger,
	}
}

// Start launches the background election loop. The first acquire attempt
// happens immediately.
func (l *LeaderElector) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	// No Redis → always leader.
	if l.redis == nil {
		l.isLeader.Store(true)
		l.logger.Info("healthcheck leader: local-only mode, always leader")
		return
	}

	go func() {
		// Attempt immediately, then on every renewInterval tick.
		l.tryAcquire(ctx)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				safego.Run(l.logger, "healthcheck.leader.renew", func() { l.tryAcquire(ctx) })
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop releases the Redis lock and cancels the background loop.
func (l *LeaderElector) Stop() error {
	if l.cancel != nil {
		l.cancel()
	}
	if l.redis == nil || !l.isLeader.Load() {
		return nil
	}
	// Release only if we still hold the lock.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return l.release(ctx)
}

// ID is this instance's election identity, also used to tag what it
// publishes so it does not apply its own probe results twice.
func (l *LeaderElector) ID() string { return l.id }

// IsLeader returns true if this instance currently holds the leader lock.
func (l *LeaderElector) IsLeader() bool {
	return l.isLeader.Load()
}

// tryAcquire attempts to acquire or renew the leader lock.
func (l *LeaderElector) tryAcquire(ctx context.Context) {
	if l.isLeader.Load() {
		// Already leader: renew the TTL.
		ok, err := l.renew(ctx)
		if err != nil {
			l.logger.Warn("healthcheck leader: renew failed", "error", err)
			l.isLeader.Store(false)
			return
		}
		if !ok {
			// Another instance claimed the key.
			l.isLeader.Store(false)
			l.logger.Info("healthcheck leader: lost leadership")
		}
		return
	}

	// Not yet leader: try to acquire.
	ok, err := l.acquire(ctx)
	if err != nil {
		l.logger.Warn("healthcheck leader: acquire failed", "error", err)
		return
	}
	if ok {
		l.isLeader.Store(true)
		l.logger.Info("healthcheck leader: acquired leadership", "id", l.id)
	}
}

// acquire runs SET key id NX EX ttl.
func (l *LeaderElector) acquire(ctx context.Context) (bool, error) {
	//nolint:staticcheck // SetNX is the intended atomic SET key id NX EX ttl; the go-redis deprecation is cosmetic and the semantics are exactly what leader election needs.
	ok, err := l.redis.SetNX(ctx, l.key, l.id, l.ttl).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("leader acquire: %w", err)
	}
	return ok, nil
}

// renew extends the TTL only if we still hold the key (Lua CAS).
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`)

func (l *LeaderElector) renew(ctx context.Context) (bool, error) {
	ttlSec := int64(l.ttl.Seconds())
	res, err := renewScript.Run(ctx, l.redis, []string{l.key}, l.id, ttlSec).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("leader renew: %w", err)
	}
	return res == 1, nil
}

// release removes the key if we still own it.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

func (l *LeaderElector) release(ctx context.Context) error {
	_, err := releaseScript.Run(ctx, l.redis, []string{l.key}, l.id).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("leader release: %w", err)
	}
	return nil
}

// instanceID generates a unique ID from hostname + random suffix.
func instanceID() string {
	host, _ := os.Hostname()
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}
