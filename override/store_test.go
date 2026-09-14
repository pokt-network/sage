package override

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMemoryStore_CRUDAndList(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	if _, ok, _ := s.Get(ctx, "log_level"); ok {
		t.Fatal("empty store should have no value")
	}
	_ = s.Set(ctx, "log_level", "debug")
	_ = s.Set(ctx, "tuning/retry.max_retries", "5")
	_ = s.Set(ctx, "tuning/retry.max_retries/eth", "1")
	if v, ok, _ := s.Get(ctx, "log_level"); !ok || v != "debug" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
	got, _ := s.List(ctx, "tuning/")
	if len(got) != 2 || got["tuning/retry.max_retries"] != "5" || got["tuning/retry.max_retries/eth"] != "1" {
		t.Fatalf("List(tuning/) = %v", got)
	}
	_ = s.Delete(ctx, "log_level")
	_ = s.Delete(ctx, "never-there")
	if _, ok, _ := s.Get(ctx, "log_level"); ok {
		t.Fatal("deleted key still present")
	}
	if s.Shared() {
		t.Fatal("a memory store is not shared")
	}
}

// fakeRedis is the slice of go-redis the store uses, over a map.
type fakeRedis struct {
	mu sync.Mutex
	m  map[string]string
}

func (f *fakeRedis) Get(_ context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[key]
	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(v, nil)
}

func (f *fakeRedis) Set(_ context.Context, key string, value interface{}, _ time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value.(string)
	return redis.NewStatusResult("OK", nil)
}

func (f *fakeRedis) Del(_ context.Context, keys ...string) *redis.IntCmd {
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

func (f *fakeRedis) Scan(_ context.Context, _ uint64, match string, _ int64) *redis.ScanCmd {
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

func TestRedisStore_PrefixesKeysAndLists(t *testing.T) {
	r := &fakeRedis{m: map[string]string{}}
	s := NewRedisStore(r)
	ctx := context.Background()
	if err := s.Set(ctx, "external_sources/sui", `{"sources":[]}`); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.m["sage:overrides:external_sources/sui"]; !ok {
		t.Fatalf("key not namespaced: %v", r.m)
	}
	_ = s.Set(ctx, "external_sources/near", "x")
	_ = s.Set(ctx, "log_level", "info")
	got, err := s.List(ctx, "external_sources/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["external_sources/sui"] == "" || got["external_sources/near"] != "x" {
		t.Fatalf("List = %v", got)
	}
	if v, ok, _ := s.Get(ctx, "log_level"); !ok || v != "info" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
	if _, ok, err := s.Get(ctx, "missing"); ok || err != nil {
		t.Fatalf("missing key: ok=%v err=%v, want false,nil", ok, err)
	}
	_ = s.Delete(ctx, "log_level")
	if _, ok, _ := s.Get(ctx, "log_level"); ok {
		t.Fatal("deleted key still present")
	}
	if !s.Shared() {
		t.Fatal("a redis store is shared")
	}
}

// Watch applies on the first poll and again only when the key space changes.
func TestWatch_AppliesFirstThenOnChange(t *testing.T) {
	s := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Set(ctx, "tuning/a", "1")

	var mu sync.Mutex
	var seen []map[string]string
	Watch(ctx, slog.Default(), s, "tuning/", 20*time.Millisecond, func(m map[string]string) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, m)
	})
	waitFor := func(n int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := len(seen)
			mu.Unlock()
			if got >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("apply called %d times, want at least %d", len(seen), n)
	}
	waitFor(1)
	time.Sleep(80 * time.Millisecond) // several unchanged polls
	mu.Lock()
	if len(seen) != 1 || seen[0]["tuning/a"] != "1" {
		mu.Unlock()
		t.Fatalf("unchanged polls must not re-apply: %v", seen)
	}
	mu.Unlock()

	_ = s.Set(ctx, "tuning/b", "2")
	waitFor(2)
	mu.Lock()
	if len(seen[1]) != 2 {
		mu.Unlock()
		t.Fatalf("second apply should carry the whole map: %v", seen[1])
	}
	mu.Unlock()

	_ = s.Delete(ctx, "tuning/a")
	_ = s.Delete(ctx, "tuning/b")
	waitFor(3)
	mu.Lock()
	if len(seen[2]) != 0 {
		mu.Unlock()
		t.Fatalf("an emptied key space must be applied as empty: %v", seen[2])
	}
	mu.Unlock()
}
