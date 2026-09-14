package featureflag

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// snapshotRedis is the slice of go-redis the store uses, over a map, with a
// count of GETs so a test can prove the hot path stopped touching Redis.
type snapshotRedis struct {
	mu   sync.Mutex
	m    map[string]string
	gets atomic.Int64
}

func (f *snapshotRedis) Get(_ context.Context, key string) *redis.StringCmd {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[key]
	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(v, nil)
}

func (f *snapshotRedis) Set(_ context.Context, key string, value interface{}, _ time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value.(string)
	return redis.NewStatusResult("OK", nil)
}

func (f *snapshotRedis) Del(_ context.Context, keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := f.m[k]; ok {
			delete(f.m, k)
			n++
		}
	}
	return redis.NewIntResult(n, nil)
}

func (f *snapshotRedis) Scan(_ context.Context, _ uint64, match string, _ int64) *redis.ScanCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := strings.TrimSuffix(match, "*")
	var keys []string
	for k := range f.m {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return redis.NewScanCmdResult(keys, 0, nil)
}

// Once Start has taken a snapshot, IsEnabled answers from it with no Redis
// round trip, sees another replica's write within a refresh, and sees its
// own writes and deletes at once.
func TestRedisStore_SnapshotServesTheHotPath(t *testing.T) {
	shared := &snapshotRedis{m: map[string]string{}}
	writer := NewRedisStore(shared, nil)
	if err := writer.SetForService(context.Background(), FlagDebugLog, "kava", true); err != nil {
		t.Fatal(err)
	}

	reader := NewRedisStore(shared, nil, func(s *RedisStore) { s.cacheTTL = 20 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader.Start(ctx)
	if !reader.IsEnabled(ctx, FlagDebugLog, "kava") {
		t.Fatal("the initial snapshot should carry the other replica's write")
	}

	before := shared.gets.Load()
	for i := 0; i < 50; i++ {
		_ = reader.IsEnabled(ctx, FlagDebugLog, "kava")
		_ = reader.IsEnabled(ctx, FlagHeuristic, "osmosis")
		_ = reader.IsEnabled(ctx, FlagCache, "")
	}
	if got := shared.gets.Load() - before; got != 0 {
		t.Fatalf("hot path made %d Redis GETs with a snapshot present, want 0", got)
	}

	// Another replica flips it; the reader follows within a refresh.
	_ = writer.SetForService(context.Background(), FlagDebugLog, "kava", false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && reader.IsEnabled(ctx, FlagDebugLog, "kava") {
		time.Sleep(5 * time.Millisecond)
	}
	if reader.IsEnabled(ctx, FlagDebugLog, "kava") {
		t.Fatal("reader did not pick up the other replica's write")
	}

	// Its own write and delete are visible immediately, no refresh needed.
	_ = reader.Set(ctx, FlagShadowMode, true)
	if !reader.IsEnabled(ctx, FlagShadowMode, "") {
		t.Fatal("own write must show at once")
	}
	_ = reader.Delete(ctx, FlagShadowMode, "")
	if reader.IsEnabled(ctx, FlagShadowMode, "") {
		t.Fatal("own delete must show at once")
	}
}
