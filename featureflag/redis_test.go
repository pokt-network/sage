package featureflag

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/domain"
)

func TestRedisStore_KeyFormat(t *testing.T) {
	tests := []struct {
		name      string
		flag      string
		serviceID string
		wantKey   string
	}{
		{"global", "retry", "", "sage:flags:retry"},
		{"service", "retry", "eth", "sage:flags:retry:eth"},
		{"complex flag name", "circuit_breaker", "poly", "sage:flags:circuit_breaker:poly"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.serviceID == "" {
				got := globalKey(tt.flag)
				if got != tt.wantKey {
					t.Errorf("globalKey(%q) = %q, want %q", tt.flag, got, tt.wantKey)
				}
			} else {
				got := serviceKey(tt.flag, domain.ServiceID(tt.serviceID))
				if got != tt.wantKey {
					t.Errorf("serviceKey(%q, %q) = %q, want %q", tt.flag, tt.serviceID, got, tt.wantKey)
				}
			}
		})
	}
}

func TestRedisStore_ParseKey(t *testing.T) {
	tests := []struct {
		key       string
		wantFlag  string
		wantSvcID string
	}{
		{"sage:flags:retry", "retry", ""},
		{"sage:flags:retry:eth", "retry", "eth"},
		{"sage:flags:circuit_breaker:poly", "circuit_breaker", "poly"},
		{"sage:flags:", "", ""},
		{"short", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			flag, svcID := parseKey(tt.key)
			if flag != tt.wantFlag || svcID != tt.wantSvcID {
				t.Errorf("parseKey(%q) = (%q, %q), want (%q, %q)", tt.key, flag, svcID, tt.wantFlag, tt.wantSvcID)
			}
		})
	}
}

func TestRedisStore_NilClient_FallsBackToDefaults(t *testing.T) {
	// Overrides are a partial map; anything absent falls back to DefaultFlags.
	store := NewRedisStore(nil, map[string]bool{
		FlagTracing: true, // override the compiled default (false)
	})
	ctx := context.Background()

	if !store.IsEnabled(ctx, FlagRetry, "eth") {
		t.Error("expected retry enabled from compiled default")
	}
	if !store.IsEnabled(ctx, FlagTracing, "eth") {
		t.Error("expected tracing enabled from override")
	}
	// A flag the operator did not set keeps its compiled default without being
	// dragged to false — the all-or-nothing bug the old struct had.
	if !store.IsEnabled(ctx, FlagCache, "eth") {
		t.Error("expected cache to keep its default (true) though only tracing was set")
	}
	if store.IsEnabled(ctx, "nonexistent", "eth") {
		t.Error("expected unknown flag disabled")
	}
}

func TestRedisStore_NilClient_SetAndGet(t *testing.T) {
	store := NewRedisStore(nil, nil)
	ctx := context.Background()

	// Set should not error with nil client (caches locally).
	if err := store.Set(ctx, FlagTracing, true); err != nil {
		t.Fatal(err)
	}
	if !store.IsEnabled(ctx, FlagTracing, "eth") {
		t.Error("expected tracing enabled from local cache")
	}
}

func TestRedisStore_NilClient_GetAll(t *testing.T) {
	store := NewRedisStore(nil, map[string]bool{FlagTracing: true})
	ctx := context.Background()

	all, err := store.GetAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !all[FlagTracing].Enabled {
		t.Error("expected tracing enabled in GetAll")
	}
	if !all[FlagRetry].Enabled {
		t.Error("expected retry enabled in GetAll")
	}
}

// TestRedisStore_DeleteGlobal_DropsTheConfigLayer is the one that matters for
// a config reload.
//
// The config overrides are captured from the file at construction and sit
// below the Redis keys. Deleting only the Redis key would leave a reload
// falling straight back onto the value the operator just removed from the
// file — a removal that reports success and changes nothing.
func TestRedisStore_DeleteGlobal_DropsTheConfigLayer(t *testing.T) {
	ctx := context.Background()
	store := NewRedisStore(nil, map[string]bool{FlagTracing: true})

	if !store.IsEnabled(ctx, FlagTracing, "eth") {
		t.Fatal("the config override should be in effect before the delete")
	}
	if err := store.DeleteGlobal(ctx, FlagTracing); err != nil {
		t.Fatalf("delete global: %v", err)
	}
	if store.IsEnabled(ctx, FlagTracing, "eth") {
		t.Fatal("the config override survived DeleteGlobal, so removing the line from the file did nothing")
	}
}

// TestRedisStore_DeleteGlobal_KeepsServiceOverrides: see
// FlagStore.DeleteGlobal — a deleted config line must not revoke a
// per-service decision.
func TestRedisStore_DeleteGlobal_KeepsServiceOverrides(t *testing.T) {
	ctx := context.Background()
	store := NewRedisStore(nil, map[string]bool{FlagTracing: true})
	if err := store.SetForService(ctx, FlagTracing, "eth", true); err != nil {
		t.Fatalf("set for service: %v", err)
	}

	if err := store.DeleteGlobal(ctx, FlagTracing); err != nil {
		t.Fatalf("delete global: %v", err)
	}

	if !store.IsEnabled(ctx, FlagTracing, "eth") {
		t.Error("DeleteGlobal cleared the per-service override")
	}
	if store.IsEnabled(ctx, FlagTracing, "poly") {
		t.Error("the global/config value survived DeleteGlobal")
	}
}

// fakeFlagRedis is the slice of RedisClient GetAll touches, with SCAN paging
// two keys per step and one key repeated across steps, as a rehash mid-walk
// can produce.
type fakeFlagRedis struct {
	data      map[string]string
	scanCalls int
}

func (f *fakeFlagRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	v, ok := f.data[key]
	if !ok {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	cmd.SetVal(v)
	return cmd
}

func (f *fakeFlagRedis) Set(ctx context.Context, key string, value interface{}, _ time.Duration) *redis.StatusCmd {
	f.data[key] = fmt.Sprint(value)
	return redis.NewStatusCmd(ctx)
}

func (f *fakeFlagRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	for _, k := range keys {
		delete(f.data, k)
	}
	return redis.NewIntCmd(ctx)
}

func (f *fakeFlagRedis) Scan(ctx context.Context, cursor uint64, match string, _ int64) *redis.ScanCmd {
	f.scanCalls++
	prefix := strings.TrimSuffix(match, "*")
	var all []string
	for k := range f.data {
		if strings.HasPrefix(k, prefix) {
			all = append(all, k)
		}
	}
	sort.Strings(all)
	start := int(cursor)
	if start > len(all) {
		start = len(all)
	}
	end := min(start+2, len(all))
	page := append([]string(nil), all[start:end]...)
	if start > 0 {
		page = append(page, all[start-1]) // A repeat from the previous step.
	}
	next := uint64(end)
	if end >= len(all) {
		next = 0
	}
	cmd := redis.NewScanCmd(ctx, nil)
	cmd.SetVal(page, next)
	return cmd
}

// GetAll walks the namespace with SCAN — never KEYS, which blocks the whole
// server and runs on every replica — following the cursor across pages and
// ignoring a key the walk repeats.
func TestRedisStore_GetAllScansTheNamespace(t *testing.T) {
	fake := &fakeFlagRedis{data: map[string]string{
		globalKey(FlagHedge):          "0",
		globalKey(FlagCache):          "1",
		serviceKey(FlagHedge, "eth"):  "1",
		serviceKey(FlagRetry, "poly"): "0",
		"sage:other:namespace":        "1",
	}}
	store := NewRedisStore(fake, nil)

	all, err := store.GetAll(context.Background())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if fake.scanCalls < 2 {
		t.Fatalf("expected the cursor to be followed across pages, got %d Scan call(s)", fake.scanCalls)
	}
	if all[FlagHedge].Enabled {
		t.Error("hedge global key = 0 must read as disabled")
	}
	if !all[FlagHedge].ServiceOverrides["eth"] {
		t.Error("hedge eth override = 1 must read as enabled")
	}
	if !all[FlagCache].Enabled {
		t.Error("cache global key = 1 must read as enabled")
	}
	if on, ok := all[FlagRetry].ServiceOverrides["poly"]; !ok || on {
		t.Errorf("retry poly override = 0 must be present and disabled, got %v/%v", on, ok)
	}
}
