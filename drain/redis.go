package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/redis/go-redis/v9"
)

const (
	// keyPrefix namespaces drain keys: sage:drain:<service>:<operator>:<rpc>.
	keyPrefix = "sage:drain:"

	// allRPCTypes is the key segment standing in for an empty RPCType. An
	// empty segment would make the key ambiguous with a trailing colon, and
	// no domain.RPCType is spelled "all", so there is nothing to collide with.
	allRPCTypes = "all"

	// defaultCacheTTL is how often the refresh loop re-reads Redis, and so the
	// worst-case delay before a drain set on another replica is honored here.
	defaultCacheTTL = 5 * time.Second
)

// ErrPropagation reports that a drain change applied to this instance but did
// not reach Redis, so peers do not know about it. It is deliberately not fatal:
// the operator who asked for the drain gets it on the instance that took the
// request, and is told plainly that the other replicas did not. Silently
// succeeding would be worse — an operator would believe a supplier was benched
// fleet-wide when it was benched on one pod.
//
// The drain it describes is real and lasts its full duration on this instance:
// the entry is held as pending and re-inserted after every refresh until it
// either propagates or expires. "This pod only" is a promise, not a caveat.
var ErrPropagation = errors.New("drain did not propagate to redis")

// RedisClient is the subset of redis.Cmdable that RedisStore uses.
//
// It is declared here rather than shared with featureflag.RedisClient because
// the two need different commands: this store enumerates its own key space
// (Scan + MGet) and never issues a single-key Get. An interface is cheaper to
// declare twice than to widen for a consumer that does not need the extra
// method.
//
// Scan rather than Keys: the refresh loop runs on every replica every cache
// TTL, and KEYS walks the entire keyspace in one blocking pass — on a Redis
// shared with anything else, that is a server-wide stall every few seconds,
// caused by the drain feature and paid for by whatever else uses that Redis.
type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
}

var _ RedisClient = (*redis.Client)(nil)

// RedisStore shares drains across every gateway instance pointed at the same
// Redis, while answering the hot-path check from process memory.
//
// It embeds *MemoryStore rather than wrapping it: the embedded store is both
// the read path (Drained and Active never touch Redis — the check runs once per
// candidate endpoint per selection) and the cache a refresh loop rebuilds.
// Writes go local first and Redis second, so the instance that took the admin
// request honors the drain even when propagation fails.
type RedisStore struct {
	*MemoryStore

	client   RedisClient
	cacheTTL time.Duration
	logger   *slog.Logger

	// keys tracks what this instance is trying to make Redis say, for the few
	// drains where that is not already settled. See keyState.
	keysMu sync.Mutex
	keys   map[Key]*keyState
}

// keyState is this instance's view of one drain key while there is still
// something to remember about it: a drain that failed to propagate, or a write
// currently on the wire. A key with neither is deleted, so the map is bounded
// by the drains actually in trouble rather than by every drain ever set.
type keyState struct {
	// entry is the drain this instance last decided on. Meaningful when pending.
	entry Entry

	// pending marks a drain that applies locally but is not in Redis, so the
	// refresh loop must keep re-inserting it locally and retrying the write.
	pending bool

	// inflight counts the writes for this key currently on the wire.
	inflight int

	// gen increments on every Set and Release. A write that started before one
	// of those lands afterwards, and last write wins in Redis — so a completing
	// write compares the generation it started at against this one to find out
	// whether its result is still the answer or is already stale.
	gen uint64

	// released records that the most recent decision for this key was a
	// Release. A stale write completing after it has to be undone: the
	// Release's own Del has already run, so the write would resurrect a drain
	// on every replica.
	released bool
}

// RedisOption configures a RedisStore.
type RedisOption func(*RedisStore)

// WithCacheTTL sets how often the refresh loop re-reads Redis. It is also the
// worst-case lag before a change made on another replica takes effect here.
func WithCacheTTL(d time.Duration) RedisOption {
	return func(s *RedisStore) {
		if d > 0 {
			s.cacheTTL = d
		}
	}
}

// WithLogger sets the logger the refresh loop reports Redis failures through.
// A nil logger falls back to slog.Default.
func WithLogger(l *slog.Logger) RedisOption {
	return func(s *RedisStore) { s.logger = l }
}

// NewRedisStore returns a Redis-backed drain store. A nil client is allowed and
// degrades the store to a plain MemoryStore — Redis is optional here as
// everywhere on the hot path, and a gateway that cannot reach it must still be
// able to bench an operator on itself.
func NewRedisStore(client RedisClient, opts ...RedisOption) *RedisStore {
	s := &RedisStore{
		MemoryStore: NewMemoryStore(),
		client:      client,
		cacheTTL:    defaultCacheTTL,
		keys:        make(map[Key]*keyState),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start runs the refresh loop until ctx is done. It is a no-op without a
// client. Each tick runs under safego.Run so one failed refresh does not stop
// the loop and leave every replica frozen on a stale view.
//
// The first refresh happens immediately rather than one tick in: a replica that
// has just booted into a fleet with a live drain would otherwise route to the
// benched operator for a full cache TTL, which is exactly the window a rollout
// puts every new pod through.
func (s *RedisStore) Start(ctx context.Context) {
	if s.client == nil {
		return
	}
	safego.GoCtx(ctx, s.logger, "drain.refresh", func(ctx context.Context) {
		safego.Run(s.logger, "drain.refresh", func() { s.refresh(ctx) })

		ticker := time.NewTicker(s.cacheTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				safego.Run(s.logger, "drain.refresh", func() { s.refresh(ctx) })
			}
		}
	})
}

// Set installs or refreshes a drain locally and then in Redis, with the Redis
// key expiring when the drain does — a replica that never sees the release
// still stops honoring it on time. An Until that has passed is a release, here
// as in MemoryStore.
//
// A Redis failure returns an error wrapping ErrPropagation and parks the entry
// as pending: the local drain applies for its full duration, and the refresh
// loop keeps retrying the write until it lands or the drain expires.
func (s *RedisStore) Set(ctx context.Context, e Entry) error {
	e.Operator = strings.ToLower(e.Operator)
	if err := s.MemoryStore.Set(ctx, e); err != nil {
		return err
	}
	if s.client == nil {
		return nil
	}

	if time.Until(e.Until) <= 0 {
		// MemoryStore treated this as a release; Redis must agree, or the next
		// refresh would resurrect the drain from the key still sitting there.
		s.beginRelease(e.Key)
		return s.del(ctx, e.Key)
	}

	gen := s.beginSet(e)
	err := s.propagate(ctx, e)
	s.finishWrite(ctx, e, gen, err)
	return err
}

// propagate writes one entry to Redis with the drain's own expiry. A Redis
// failure wraps ErrPropagation; an encoding failure does not, because no amount
// of retrying will fix it.
//
// It is the only place that writes a drain key, and it is always bracketed by
// a begin (beginSet, or takePending for a retry) and finishWrite — that pair is
// what lets a write overtaken while it was on the wire notice and undo itself.
func (s *RedisStore) propagate(ctx context.Context, e Entry) error {
	ttl := time.Until(e.Until)
	if ttl <= 0 {
		return nil
	}
	value, err := json.Marshal(payload{Until: e.Until, Reason: e.Reason})
	if err != nil {
		return fmt.Errorf("encoding drain %s: %w", redisKey(e.Key), err)
	}
	if err := s.client.Set(ctx, redisKey(e.Key), value, ttl).Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrPropagation, err)
	}
	return nil
}

// state returns k's tracking record, creating it if needed. Caller holds keysMu.
func (s *RedisStore) state(k Key) *keyState {
	st, ok := s.keys[k]
	if !ok {
		st = &keyState{}
		s.keys[k] = st
	}
	return st
}

// forget drops a record with nothing left to remember. Caller holds keysMu.
func (s *RedisStore) forget(k Key, st *keyState) {
	if !st.pending && st.inflight == 0 {
		delete(s.keys, k)
	}
}

// beginSet records a Set as the current decision for its key and marks its
// write in flight, returning the generation to check on completion.
func (s *RedisStore) beginSet(e Entry) uint64 {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()
	st := s.state(e.Key)
	st.gen++
	st.released = false
	st.entry = e
	st.inflight++
	return st.gen
}

// beginRelease records a Release as the current decision for its key. The
// record survives as long as a write is still in flight, so that write can find
// out on completion that it was overtaken and undo itself.
func (s *RedisStore) beginRelease(k Key) {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()
	st := s.state(k)
	st.gen++
	st.released = true
	st.pending = false
	st.entry = Entry{}
	s.forget(k, st)
}

// finishWrite applies one write's result to its key's record and reports the
// entry the local map should now hold for that key, if any.
//
// A generation that moved while the write was on the wire means a Set or a
// Release overtook it, and Redis resolves that by last write wins — which,
// without this, is the write that started first. Three outcomes:
//
//   - Overtaken by a Release: the Release's own Del has already run, so this
//     write has resurrected the drain everywhere. Undo it with a compensating
//     Del and report nothing to keep, so the refresh does not re-install it
//     locally either.
//   - Overtaken by a newer Set: that Set's value may have been clobbered by
//     this stale one. Re-queue the newer entry so the next tick rewrites it,
//     and report the newer entry rather than this one.
//   - Not overtaken: apply the result. A Redis failure keeps the entry pending;
//     an encoding failure does not, because retrying it forever is noise.
func (s *RedisStore) finishWrite(ctx context.Context, e Entry, gen uint64, writeErr error) (live Entry, keep bool) {
	s.keysMu.Lock()
	st := s.state(e.Key)
	st.inflight--

	var compensate bool
	switch {
	case st.gen != gen && st.released:
		// Compensate even when the write reported an error: a request can fail
		// on the client side after the server applied it, and a Del of a key
		// that is already gone costs one round trip and nothing else.
		compensate = true
	case st.gen != gen:
		st.pending = true
		live, keep = st.entry, true
	default:
		if writeErr != nil {
			st.pending = errors.Is(writeErr, ErrPropagation)
			if st.pending {
				st.entry = e
			}
		} else {
			st.pending = false
		}
		live, keep = e, true
	}
	s.forget(e.Key, st)
	s.keysMu.Unlock()

	if compensate {
		if err := s.client.Del(ctx, redisKey(e.Key)).Err(); err != nil {
			s.log().Warn("drain: could not undo a write that raced a release",
				"key", redisKey(e.Key), "error", err)
		}
	}
	return live, keep
}

// pendingCount reports how many drains are still local-only. Test-facing, and
// the natural hook for a future gauge.
func (s *RedisStore) pendingCount() int {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()
	n := 0
	for _, st := range s.keys {
		if st.pending {
			n++
		}
	}
	return n
}

// Release removes the drain locally and in Redis. A Redis failure returns an
// error wrapping ErrPropagation: the operator's own instance has stopped
// draining, but peers will keep the operator benched until the key's own expiry
// runs out.
func (s *RedisStore) Release(ctx context.Context, k Key) error {
	k.Operator = strings.ToLower(k.Operator)
	if err := s.MemoryStore.Release(ctx, k); err != nil {
		return err
	}
	// Record the release before deleting the key. A release that left the
	// pending entry behind would have the refresh loop faithfully re-install the
	// drain the admin just lifted, and one that did not mark the key released
	// would let a retry already on the wire land afterwards and win.
	s.beginRelease(k)
	if s.client == nil {
		return nil
	}
	return s.del(ctx, k)
}

func (s *RedisStore) del(ctx context.Context, k Key) error {
	if err := s.client.Del(ctx, redisKey(k)).Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrPropagation, err)
	}
	return nil
}

// refresh rebuilds the local map from a Scan of the drain namespace plus one
// MGet, then lays the pending (local-only) drains back on top.
//
// It replaces the map rather than merging into it, which is the whole point: a
// release performed on another replica shows up as a key that is simply gone,
// and merging would keep the drain alive here forever. Pending entries are the
// one exception, and they are an exception this instance owns rather than one
// inferred from a missing key — see reconcilePending.
//
// A Redis error leaves the local map alone. An unreachable Redis must not
// un-bench an operator an admin deliberately benched — degrading to a stale
// drain is safe, degrading to no drain is not.
func (s *RedisStore) refresh(ctx context.Context) {
	if s.client == nil {
		return
	}

	keys, err := s.scanKeys(ctx)
	if err != nil {
		s.log().Warn("drain refresh: listing redis keys failed, keeping local drains", "error", err)
		return
	}

	var values []interface{}
	if len(keys) > 0 {
		values, err = s.client.MGet(ctx, keys...).Result()
		if err != nil {
			s.log().Warn("drain refresh: reading redis values failed, keeping local drains", "error", err)
			return
		}
	}

	now := time.Now()
	next := make(map[Key]Entry, len(keys))
	for i, key := range keys {
		if i >= len(values) {
			break
		}
		raw, ok := values[i].(string)
		if !ok {
			continue // nil: the key expired between the Keys and the MGet.
		}
		k, ok := parseRedisKey(key)
		if !ok {
			continue
		}
		var p payload
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			continue // One malformed value must not cost the other drains.
		}
		if !p.Until.After(now) {
			continue
		}
		next[k] = Entry{Key: k, Until: p.Until, Reason: p.Reason}
	}

	s.reconcilePending(ctx, next, now)
	s.replaceAll(next)
}

// scanCount is the COUNT hint given to each SCAN cursor step. Drain keys are
// few — one per benched operator per service — so this is about bounding the
// work Redis does per call, not about the round trips: a fleet with a handful
// of drains finishes in one.
const scanCount = 256

// scanKeys enumerates the drain namespace with SCAN, following the cursor to
// completion. Duplicates are dropped: SCAN may return a key twice when the
// keyspace is rehashed mid-iteration, and MGetting the same key twice would
// be pointless work.
//
// A failure part-way through is returned rather than salvaged. A partial view
// of the namespace is worse than no view: refresh replaces the local map, so
// half a scan would silently un-bench every operator whose key happened to sit
// after the failing cursor step.
func (s *RedisStore) scanKeys(ctx context.Context) ([]string, error) {
	var (
		keys   []string
		seen   map[string]struct{}
		cursor uint64
	)
	for {
		page, next, err := s.client.Scan(ctx, cursor, keyPrefix+"*", scanCount).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range page {
			if seen == nil {
				seen = make(map[string]struct{}, len(page))
			}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
		cursor = next
		if cursor == 0 {
			return keys, nil
		}
	}
}

// reconcilePending retries the Redis write for every drain that failed to
// propagate, and puts the ones that are still this instance's decision back
// into the replacement map.
//
// Without this, replace-not-merge would wipe a failed drain on the next tick and
// ErrPropagation would be promising a drain the store does not deliver. With it,
// "this pod only" means what it says: the drain lasts its full duration here,
// and reaches the fleet the moment Redis is reachable again. An entry that has
// expired is dropped rather than retried — nobody needs a drain that is over.
//
// Nothing is written into next before its retry finishes. An admin can Release
// a drain while its retry is on the wire, and deciding up front would re-install
// the drain locally moments after the admin lifted it; finishWrite makes that
// decision after the write lands, holding the lock, from the key's current
// state.
func (s *RedisStore) reconcilePending(ctx context.Context, next map[Key]Entry, now time.Time) {
	for _, r := range s.takePending(now) {
		err := s.propagate(ctx, r.entry)
		if err != nil {
			s.log().Warn("drain refresh: retrying a drain that failed to propagate",
				"key", redisKey(r.entry.Key), "error", err)
		}
		if live, keep := s.finishWrite(ctx, r.entry, r.gen, err); keep && live.Until.After(now) {
			next[live.Key] = live
		}
	}
}

// retry is one pending entry taken for a write attempt, with the generation it
// was taken at.
type retry struct {
	entry Entry
	gen   uint64
}

// takePending collects the drains still waiting to reach Redis, marking a write
// in flight for each and capturing the generation to check on completion, and
// drops the ones that have expired in the meantime. It does not bump the
// generation: a retry is the same decision again, not a new one.
func (s *RedisStore) takePending(now time.Time) []retry {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()
	var out []retry
	for k, st := range s.keys {
		if !st.pending {
			continue
		}
		if !st.entry.Until.After(now) {
			st.pending = false
			st.entry = Entry{}
			s.forget(k, st)
			continue
		}
		st.inflight++
		out = append(out, retry{entry: st.entry, gen: st.gen})
	}
	return out
}

func (s *RedisStore) log() *slog.Logger {
	if s.logger == nil {
		return slog.Default()
	}
	return s.logger
}

// payload is the JSON stored at a drain key. The key carries the service,
// operator and RPC type, so the value only has to carry what the key cannot.
type payload struct {
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
}

// redisKey renders k as sage:drain:<service>:<operator>:<rpc-or-all>.
func redisKey(k Key) string {
	rpc := string(k.RPCType)
	if rpc == "" {
		rpc = allRPCTypes
	}
	return keyPrefix + string(k.ServiceID) + ":" + strings.ToLower(k.Operator) + ":" + rpc
}

// parseRedisKey is redisKey's inverse. It reports false for anything that does
// not have exactly the three expected segments — a key written by a future
// version, or by something else sharing the namespace, is skipped rather than
// guessed at.
func parseRedisKey(key string) (Key, bool) {
	if !strings.HasPrefix(key, keyPrefix) {
		return Key{}, false
	}
	parts := strings.Split(key[len(keyPrefix):], ":")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Key{}, false
	}
	k := Key{ServiceID: domain.ServiceID(parts[0]), Operator: parts[1]}
	if parts[2] != allRPCTypes {
		k.RPCType = domain.RPCType(parts[2])
	}
	return k, true
}

var _ Store = (*RedisStore)(nil)
