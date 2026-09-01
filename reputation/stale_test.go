package reputation

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStorage_DeleteStale(t *testing.T) {
	m := NewMemoryStorage()
	now := time.Unix(10_000, 0)
	ctx := context.Background()
	require.NoError(t, m.SetState(ctx, "eth:old", State{Score: 50, UpdatedAt: now.Add(-2 * time.Hour).Unix()}))
	require.NoError(t, m.SetState(ctx, "eth:unstamped", State{Score: 50}))
	require.NoError(t, m.SetState(ctx, "eth:fresh", State{Score: 50, UpdatedAt: now.Add(-time.Minute).Unix()}))

	n, err := m.DeleteStale(ctx, now.Add(-time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	_, err = m.GetState(ctx, "eth:fresh")
	assert.NoError(t, err, "fresh key must survive")
	_, err = m.GetState(ctx, "eth:old")
	assert.ErrorIs(t, err, ErrStateNotFound)
	_, err = m.GetState(ctx, "eth:unstamped")
	assert.ErrorIs(t, err, ErrStateNotFound, "a field with no stamp predates the stamp and is stale")
}

// fakeHash is the hash subset of a Redis client, enough to drive DeleteStale
// through real HSCAN paging: pages of pageSize fields, a non-zero cursor
// between them.
type fakeHash struct {
	mu       sync.Mutex
	fields   map[string]string
	pageSize int
	hdels    int
}

func (f *fakeHash) HGet(_ context.Context, _, field string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.fields[field]
	if !ok {
		cmd := redis.NewStringResult("", redis.Nil)
		return cmd
	}
	return redis.NewStringResult(v, nil)
}

func (f *fakeHash) HSet(_ context.Context, _ string, values ...interface{}) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i+1 < len(values); i += 2 {
		f.fields[values[i].(string)] = values[i+1].(string)
	}
	return redis.NewIntResult(1, nil)
}

func (f *fakeHash) HGetAll(_ context.Context, _ string) *redis.MapStringStringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.fields))
	for k, v := range f.fields {
		out[k] = v
	}
	return redis.NewMapStringStringResult(out, nil)
}

func (f *fakeHash) HDel(_ context.Context, _ string, fields ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hdels++
	var n int64
	for _, k := range fields {
		if _, ok := f.fields[k]; ok {
			delete(f.fields, k)
			n++
		}
	}
	return redis.NewIntResult(n, nil)
}

// HScan pages over a snapshot in sorted order; cursor is the offset into it.
func (f *fakeHash) HScan(_ context.Context, _ string, cursor uint64, _ string, _ int64) *redis.ScanCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.fields))
	for k := range f.fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	start := int(cursor)
	end := start + f.pageSize
	if end > len(keys) {
		end = len(keys)
	}
	var page []string
	for _, k := range keys[start:end] {
		page = append(page, k, f.fields[k])
	}
	next := uint64(end)
	if end >= len(keys) {
		next = 0
	}
	return redis.NewScanCmdResult(page, next, nil)
}

func TestRedisStorage_DeleteStale(t *testing.T) {
	now := time.Unix(10_000, 0)
	fake := &fakeHash{fields: map[string]string{}, pageSize: 3}
	r, err := NewRedisStorage(fake, "sage:reputation:")
	require.NoError(t, err)
	ctx := context.Background()

	// Seven fields across three HSCAN pages: stamped-fresh, stamped-stale,
	// unstamped JSON, and legacy bare floats — the last two are what a
	// pre-stamp pod left behind.
	require.NoError(t, r.SetState(ctx, "eth:fresh1", State{Score: 90, UpdatedAt: now.Add(-time.Minute).Unix()}))
	require.NoError(t, r.SetState(ctx, "eth:fresh2", State{Score: 90, UpdatedAt: now.Add(-59 * time.Minute).Unix()}))
	require.NoError(t, r.SetState(ctx, "eth:stale1", State{Score: 90, UpdatedAt: now.Add(-61 * time.Minute).Unix()}))
	require.NoError(t, r.SetState(ctx, "eth:stale2", State{Score: 90, UpdatedAt: now.Add(-24 * time.Hour).Unix()}))
	require.NoError(t, r.SetState(ctx, "eth:unstamped", State{Score: 90}))
	fake.fields["eth:legacy"] = "87.5"
	fake.fields["eth:garbage"] = "{not json"

	n, err := r.DeleteStale(ctx, now.Add(-time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	left, err := r.GetStates(ctx, "eth:")
	require.NoError(t, err)
	keys := make([]string, 0, len(left))
	for k := range left {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{"eth:fresh1", "eth:fresh2"}, keys)
}

func TestRedisStorage_DeleteStale_EmptyHash(t *testing.T) {
	fake := &fakeHash{fields: map[string]string{}, pageSize: 10}
	r, err := NewRedisStorage(fake, "h")
	require.NoError(t, err)
	n, err := r.DeleteStale(context.Background(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, fake.hdels, "no HDEL with nothing to delete")
}

func TestLeaderOnlyStorage_DeleteStaleOnlyOnLeader(t *testing.T) {
	m := NewMemoryStorage()
	ctx := context.Background()
	require.NoError(t, m.SetState(ctx, "eth:old", State{Score: 1}))

	leader := false
	s := NewLeaderOnlyStorage(m, func() bool { return leader })
	n, err := s.DeleteStale(ctx, time.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, n, "follower must not sweep")

	leader = true
	n, err = s.DeleteStale(ctx, time.Now())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// sweepRecorder is a Storage that records DeleteStale cutoffs.
type sweepRecorder struct {
	MemoryStorage
	mu      sync.Mutex
	cutoffs []time.Time
}

func (r *sweepRecorder) DeleteStale(ctx context.Context, olderThan time.Time) (int, error) {
	r.mu.Lock()
	r.cutoffs = append(r.cutoffs, olderThan)
	r.mu.Unlock()
	return r.MemoryStorage.DeleteStale(ctx, olderThan)
}

// The write-behind goroutine stamps every write and runs the sweep on its
// interval with now-StateIdleTTL as the cutoff.
func TestService_StampsWritesAndSweepsStorage(t *testing.T) {
	store := &sweepRecorder{MemoryStorage: *NewMemoryStorage()}
	svc := NewService(store, nil, ServiceConfig{
		StateIdleTTL:       time.Hour,
		StateSweepInterval: 10 * time.Millisecond,
	})
	svc.Start()
	defer svc.Stop()

	before := time.Now().Unix()
	require.NoError(t, svc.RecordSignal(context.Background(), "eth", "sup1-https://a.example.com", "json_rpc", Signal{Type: SignalMajorError, Timestamp: time.Now()}))

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.cutoffs) >= 2
	}, 2*time.Second, 5*time.Millisecond, "sweep did not run on its interval")

	states, err := store.GetStates(context.Background(), "eth:")
	require.NoError(t, err)
	require.Len(t, states, 1, "a just-written key must survive a sweep at 1h TTL")
	for _, st := range states {
		assert.GreaterOrEqual(t, st.UpdatedAt, before, "write must be stamped")
	}
	store.mu.Lock()
	cutoff := store.cutoffs[0]
	store.mu.Unlock()
	assert.WithinDuration(t, time.Now().Add(-time.Hour), cutoff, 2*time.Second)
}

// A negative TTL turns the sweep off entirely.
func TestService_NegativeTTLDisablesSweep(t *testing.T) {
	store := &sweepRecorder{MemoryStorage: *NewMemoryStorage()}
	svc := NewService(store, nil, ServiceConfig{
		StateIdleTTL:       -1,
		StateSweepInterval: 5 * time.Millisecond,
	})
	svc.Start()
	time.Sleep(50 * time.Millisecond)
	svc.Stop()
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Empty(t, store.cutoffs)
}
