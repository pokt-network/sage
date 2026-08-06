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

// Failure-rate gate defaults.
//
// The trigger used to be first-error: one relay whose heuristic result asked
// for a circuit break removed the entire hostname from the pool for at least a
// minute. That is volume-sensitive rather than quality-sensitive — the operator
// serving the most traffic reaches its first error soonest after every TTL
// expiry, so the healthiest high-volume host is the one broken most often, and
// a hostname fronting many endpoints takes a large share of the pool down with
// it. Batch fan-out makes it worse: one bad upstream moment produces one
// MarkBroken per sub-relay, and the old code escalated the TTL on every one of
// them, driving a transient blip straight to the 30-minute cap.
//
// Gating on RATE fixes both: a domain must fail enough times to be
// statistically meaningful (minFailures) AND often enough to be genuinely
// unhealthy (failureRateThreshold) before it is removed.
const (
	// defaultFailureWindow is the sliding window over which failures and
	// successes are compared. Short enough to react to a real outage, long
	// enough that a burst of concurrent batch items is not a trend.
	defaultFailureWindow = 30 * time.Second
	// defaultMinFailures is the floor below which no rate is trusted. Without
	// it, the first failure on a quiet domain is a 100% failure rate.
	defaultMinFailures = 5
	// defaultFailureRateThreshold is the fraction of attempts that must fail
	// before a domain is removed. Well above the error rate healthy high-volume
	// operators sustain (<1%), well below what a broken host produces (~100%).
	defaultFailureRateThreshold = 0.20
	// defaultEscalationMemory is how long a domain's break history survives
	// after it recovers. Escalation should punish a domain that breaks AGAIN
	// after being let back in; without memory across expiry, every episode
	// looks like a first offence.
	defaultEscalationMemory = 60 * time.Minute
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

// outcomeWindow is a sliding count of relay outcomes for one domain, plus the
// memory of its last break episode.
//
// windowStart/failures/successes reset together once the window elapses, so the
// counts always describe recent behavior rather than lifetime totals — a domain
// that failed heavily hours ago must not stay gated on that history.
//
// lastEpisodeAt/lastHitCount deliberately survive both the window and the
// break's own expiry: they are what makes escalation mean "broke again after we
// let it back in" instead of "was marked many times during one incident".
type outcomeWindow struct {
	windowStart time.Time
	failures    int
	successes   int

	lastEpisodeAt time.Time
	lastHitCount  int
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

	// Failure-rate gate. statsMu is kept separate from mu so recording an
	// outcome never contends with the IsBroken read path.
	failureWindow        time.Duration
	minFailures          int
	failureRateThreshold float64
	escalationMemory     time.Duration
	statsMu              sync.Mutex
	stats                map[string]map[string]*outcomeWindow // serviceID -> domain
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

// WithFailureRateGate overrides the failure-rate gate that guards MarkBroken.
// minFailures <= 1 combined with threshold <= 0 restores first-error breaking.
func WithFailureRateGate(window time.Duration, minFailures int, threshold float64) Option {
	return func(b *Breaker) {
		b.failureWindow = window
		b.minFailures = minFailures
		b.failureRateThreshold = threshold
	}
}

// WithEscalationMemory sets how long a domain's break history survives after
// the break itself expires, for the purpose of TTL escalation.
func WithEscalationMemory(d time.Duration) Option {
	return func(b *Breaker) {
		b.escalationMemory = d
	}
}

// New creates a new Breaker.
func New(opts ...Option) *Breaker {
	b := &Breaker{
		broken:               make(map[string]map[string]BrokenState),
		keyPrefix:            defaultKeyPrefix,
		cacheTTL:             defaultCacheTTL,
		lastRefresh:          make(map[string]time.Time),
		logger:               slog.Default(),
		failureWindow:        defaultFailureWindow,
		minFailures:          defaultMinFailures,
		failureRateThreshold: defaultFailureRateThreshold,
		escalationMemory:     defaultEscalationMemory,
		stats:                make(map[string]map[string]*outcomeWindow),
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

// RecordSuccess reports a successful relay against a domain. It forms the
// denominator of the failure-rate gate: without it the gate sees only failures,
// and any failure looks like a 100% failure rate — which is exactly the
// first-error behavior the gate replaces.
func (b *Breaker) RecordSuccess(serviceID, domain string) {
	if b == nil || domain == "" {
		return
	}
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.windowLocked(serviceID, domain, time.Now()).successes++
}

// windowLocked returns the outcome window for a domain, rolling it over if the
// current one has elapsed. Caller must hold statsMu.
func (b *Breaker) windowLocked(serviceID, domain string, now time.Time) *outcomeWindow {
	byDomain, ok := b.stats[serviceID]
	if !ok {
		byDomain = make(map[string]*outcomeWindow)
		b.stats[serviceID] = byDomain
	}
	w, ok := byDomain[domain]
	if !ok {
		w = &outcomeWindow{windowStart: now}
		byDomain[domain] = w
		return w
	}
	if now.Sub(w.windowStart) > b.failureWindow {
		// Roll the window. Episode memory survives on purpose — it is not part
		// of the rate calculation, it is what escalation counts.
		w.windowStart = now
		w.failures = 0
		w.successes = 0
	}
	return w
}

// shouldBreak records a failure and reports whether the domain's recent failure
// RATE justifies removing it from the pool, plus the hit count for this episode.
//
// Both conditions must hold: at least minFailures in the window (so a single
// failure on a quiet domain is not a 100% rate) and a failure fraction at or
// above failureRateThreshold (so a high-volume domain with a low error rate is
// never removed, no matter how many raw failures that volume produces).
func (b *Breaker) shouldBreak(serviceID, domain string, now time.Time) (bool, int) {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()

	w := b.windowLocked(serviceID, domain, now)
	w.failures++

	total := w.failures + w.successes
	if w.failures < b.minFailures || total == 0 {
		return false, 0
	}
	if float64(w.failures)/float64(total) < b.failureRateThreshold {
		return false, 0
	}

	// Breaking. Escalate only if this domain broke recently BEFORE this episode
	// — i.e. it was let back in and failed again. Duplicate marks within one
	// episode are filtered in MarkBroken and never reach here.
	hitCount := 1
	if !w.lastEpisodeAt.IsZero() && now.Sub(w.lastEpisodeAt) <= b.escalationMemory {
		hitCount = w.lastHitCount + 1
	}
	w.lastEpisodeAt = now
	w.lastHitCount = hitCount

	// The break consumes the window: count from scratch so the domain is judged
	// on behavior after it returns, not on the failures that removed it.
	w.windowStart = now
	w.failures = 0
	w.successes = 0

	return true, hitCount
}

// MarkBroken reports a circuit-break-worthy failure against a domain and
// returns whether it actually removed the domain from the pool.
//
// A single failure is NOT grounds for removal — see the failure-rate gate. The
// return value is false when the failure was recorded but the gate declined, or
// when the domain is already broken; callers should only count a break metric
// when it is true.
//
// TTL escalates per episode: 1m → 2m → 4m → 8m → 16m → 30m (cap).
func (b *Breaker) MarkBroken(serviceID, domain, reason string) bool {
	now := time.Now()

	// Already broken? This failure belongs to the episode already in effect.
	// Batch sub-relays and hedge arms fail concurrently, so one incident
	// produces many of these. Do not escalate, do not extend the expiry.
	b.mu.RLock()
	if domains, ok := b.broken[serviceID]; ok {
		if existing, exists := domains[domain]; exists && existing.Expiry.After(now) {
			b.mu.RUnlock()
			return false
		}
	}
	b.mu.RUnlock()

	breakIt, hitCount := b.shouldBreak(serviceID, domain, now)
	if !breakIt {
		return false
	}

	ttl := b.escalateTTL(hitCount - 1)
	state := BrokenState{
		Expiry:   now.Add(ttl),
		HitCount: hitCount,
		Reason:   reason,
	}

	b.mu.Lock()
	if b.broken[serviceID] == nil {
		b.broken[serviceID] = make(map[string]BrokenState)
	}
	b.broken[serviceID][domain] = state
	b.mu.Unlock()

	// Persist to Redis asynchronously (best-effort).
	if b.redis != nil {
		b.persistToRedis(serviceID, domain, state, ttl)
	}
	return true
}

// Clear removes all circuit breaker state for a service. Returns the count of cleared domains.
func (b *Breaker) Clear(serviceID string) int {
	b.mu.Lock()
	domains := b.broken[serviceID]
	count := len(domains)
	delete(b.broken, serviceID)
	delete(b.lastRefresh, serviceID)
	b.mu.Unlock()

	// Drop the rate-gate history too. Clear exists to let an operator undo a
	// false-positive lockout; leaving the window and episode memory behind
	// would re-break the domain on the next few failures and escalate its TTL
	// as a repeat offender.
	b.statsMu.Lock()
	delete(b.stats, serviceID)
	b.statsMu.Unlock()

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
