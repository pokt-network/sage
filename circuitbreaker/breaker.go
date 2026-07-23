package circuitbreaker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Default configuration values.
const (
	defaultCacheTTL  = 5 * time.Second
	defaultMinTTL    = 1 * time.Minute
	defaultMaxTTL    = 30 * time.Minute
	defaultKeyPrefix = "sage:circuit:"
	escalationFactor = 2
)

// BrokenState holds the state of a circuit-broken domain.
type BrokenState struct {
	Expiry   time.Time `json:"expiry"`
	HitCount int       `json:"hit_count"`
	Reason   string    `json:"reason"`
}

// IsExpired returns true if the broken state has expired.
func (b BrokenState) IsExpired() bool {
	return time.Now().After(b.Expiry)
}

// Breaker is a domain-level circuit breaker with optional Redis backing.
// When Redis is nil, it operates in local-only mode.
type Breaker struct {
	mu          sync.RWMutex
	broken      map[string]map[string]BrokenState // serviceID -> domain -> state
	redis       *redis.Client                     // nil = local-only mode
	logger      *slog.Logger
	keyPrefix   string
	cacheTTL    time.Duration
	lastRefresh map[string]time.Time
}

// Option configures a Breaker.
type Option func(*Breaker)

// WithRedis sets the Redis client for cross-pod sharing.
func WithRedis(client *redis.Client) Option {
	return func(b *Breaker) {
		b.redis = client
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(b *Breaker) {
		b.logger = logger
	}
}

// WithKeyPrefix sets the Redis key prefix.
func WithKeyPrefix(prefix string) Option {
	return func(b *Breaker) {
		b.keyPrefix = prefix
	}
}

// WithCacheTTL sets how long to cache Redis reads before refreshing.
func WithCacheTTL(ttl time.Duration) Option {
	return func(b *Breaker) {
		b.cacheTTL = ttl
	}
}

// New creates a new Breaker.
func New(opts ...Option) *Breaker {
	b := &Breaker{
		broken:      make(map[string]map[string]BrokenState),
		keyPrefix:   defaultKeyPrefix,
		cacheTTL:    defaultCacheTTL,
		lastRefresh: make(map[string]time.Time),
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// IsBroken returns true if the given domain is circuit-broken for the service.
// It lazily refreshes from Redis if the cache is stale.
func (b *Breaker) IsBroken(serviceID, domain string) bool {
	b.maybeRefreshFromRedis(serviceID)

	b.mu.RLock()
	defer b.mu.RUnlock()

	domains, ok := b.broken[serviceID]
	if !ok {
		return false
	}
	state, ok := domains[domain]
	if !ok {
		return false
	}
	if state.IsExpired() {
		// Expired entries are cleaned up lazily; not broken.
		return false
	}
	return true
}

// MarkBroken marks a domain as circuit-broken for the given service.
// TTL escalates: 1m → 2m → 4m → 8m → 16m → 30m (cap).
func (b *Breaker) MarkBroken(serviceID, domain, reason string) {
	b.mu.Lock()

	if b.broken[serviceID] == nil {
		b.broken[serviceID] = make(map[string]BrokenState)
	}

	existing := b.broken[serviceID][domain]
	ttl := b.escalateTTL(existing.HitCount)
	state := BrokenState{
		Expiry:   time.Now().Add(ttl),
		HitCount: existing.HitCount + 1,
		Reason:   reason,
	}
	b.broken[serviceID][domain] = state

	b.mu.Unlock()

	// Persist to Redis asynchronously (best-effort).
	if b.redis != nil {
		b.persistToRedis(serviceID, domain, state, ttl)
	}
}

// Clear removes all circuit breaker state for a service. Returns the count of cleared domains.
func (b *Breaker) Clear(serviceID string) int {
	b.mu.Lock()
	domains := b.broken[serviceID]
	count := len(domains)
	delete(b.broken, serviceID)
	delete(b.lastRefresh, serviceID)
	b.mu.Unlock()

	// Clear from Redis too.
	if b.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		key := b.redisKey(serviceID)
		if err := b.redis.Del(ctx, key).Err(); err != nil {
			b.logger.Error("failed to clear circuit breaker from Redis",
				"service_id", serviceID,
				"error", err,
			)
		}
	}

	return count
}

// GetBroken returns all non-expired broken domains for a service.
func (b *Breaker) GetBroken(serviceID string) map[string]BrokenState {
	b.maybeRefreshFromRedis(serviceID)

	b.mu.RLock()
	defer b.mu.RUnlock()

	domains, ok := b.broken[serviceID]
	if !ok {
		return nil
	}

	now := time.Now()
	result := make(map[string]BrokenState, len(domains))
	for domain, state := range domains {
		if !now.After(state.Expiry) {
			result[domain] = state
		}
	}
	return result
}

// BrokenDomains returns the domains currently circuit-broken for a service.
//
// Expired entries are excluded (see GetBroken), which is what lets this be read
// at scrape time and always be current: breaks expire lazily, with no event to
// hang a pushed gauge off.
func (b *Breaker) BrokenDomains(serviceID string) []string {
	broken := b.GetBroken(serviceID)
	if len(broken) == 0 {
		return nil
	}
	domains := make([]string, 0, len(broken))
	for d := range broken {
		domains = append(domains, d)
	}
	return domains
}

// escalateTTL returns the TTL for the next break based on hit count.
// Escalation: 1m → 2m → 4m → 8m → 16m → 30m (cap).
func (b *Breaker) escalateTTL(hitCount int) time.Duration {
	ttl := defaultMinTTL
	for i := 0; i < hitCount; i++ {
		ttl *= escalationFactor
		if ttl >= defaultMaxTTL {
			return defaultMaxTTL
		}
	}
	return ttl
}

// redisKey returns the Redis key for a service's circuit breaker state.
func (b *Breaker) redisKey(serviceID string) string {
	return fmt.Sprintf("%s%s", b.keyPrefix, serviceID)
}

// maybeRefreshFromRedis triggers a background refresh of the local cache from
// Redis if it's stale. It never blocks the caller: IsBroken sits on the relay
// hot path (per endpoint, per relay), so the Redis round trip runs in a
// goroutine and callers serve the current local state (stale-while-revalidate).
// lastRefresh is claimed up front — whether the fetch later succeeds or not —
// so a Redis outage costs at most one attempt per cacheTTL instead of a
// blocking call per IsBroken.
func (b *Breaker) maybeRefreshFromRedis(serviceID string) {
	if b.redis == nil {
		return
	}

	b.mu.RLock()
	lastRefresh, exists := b.lastRefresh[serviceID]
	b.mu.RUnlock()

	if exists && time.Since(lastRefresh) < b.cacheTTL {
		return
	}

	// Claim the refresh under the write lock (double-checked) so only one
	// goroutine per service per TTL hits Redis.
	b.mu.Lock()
	lastRefresh, exists = b.lastRefresh[serviceID]
	if exists && time.Since(lastRefresh) < b.cacheTTL {
		b.mu.Unlock()
		return
	}
	b.lastRefresh[serviceID] = time.Now()
	b.mu.Unlock()

	go b.refreshFromRedis(serviceID)
}

// refreshFromRedis fetches the service's broken-domain state from Redis and
// merges it into the local cache. Runs off the hot path.
func (b *Breaker) refreshFromRedis(serviceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key := b.redisKey(serviceID)
	entries, err := b.redis.HGetAll(ctx, key).Result()
	if err != nil {
		b.logger.Error("failed to refresh circuit breaker from Redis",
			"service_id", serviceID,
			"error", err,
		)
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.broken[serviceID] == nil {
		b.broken[serviceID] = make(map[string]BrokenState)
	}

	now := time.Now()

	// Merge Redis state into local state.
	for domain, raw := range entries {
		var state BrokenState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			b.logger.Error("failed to unmarshal circuit breaker state",
				"service_id", serviceID,
				"domain", domain,
				"error", err,
			)
			continue
		}
		// Skip expired entries.
		if now.After(state.Expiry) {
			continue
		}
		// Merge: keep the entry with the later expiry.
		if existing, ok := b.broken[serviceID][domain]; ok {
			if state.Expiry.After(existing.Expiry) {
				b.broken[serviceID][domain] = state
			}
		} else {
			b.broken[serviceID][domain] = state
		}
	}
}

// persistToRedis writes a broken state to Redis.
func (b *Breaker) persistToRedis(serviceID, domain string, state BrokenState, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	data, err := json.Marshal(state)
	if err != nil {
		b.logger.Error("failed to marshal circuit breaker state",
			"service_id", serviceID,
			"domain", domain,
			"error", err,
		)
		return
	}

	key := b.redisKey(serviceID)
	if err := b.redis.HSet(ctx, key, domain, string(data)).Err(); err != nil {
		b.logger.Error("failed to persist circuit breaker to Redis",
			"service_id", serviceID,
			"domain", domain,
			"error", err,
		)
		return
	}

	// Set expiry on the hash key to auto-cleanup. Use the max possible TTL
	// so the hash isn't deleted while some fields are still active.
	// Individual field expiry isn't supported in standard Redis, so we set
	// the key expiry to the max TTL and rely on application-level expiry checks.
	if err := b.redis.Expire(ctx, key, defaultMaxTTL+time.Minute).Err(); err != nil {
		b.logger.Error("failed to set Redis key expiry",
			"service_id", serviceID,
			"error", err,
		)
	}
}
