package cooldown

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeRedis keeps SET ... PX and PTTL against a clock the test moves.
type fakeRedis struct {
	mu    sync.Mutex
	now   *time.Time
	until map[string]time.Time
	err   error
	reads int
}

func (f *fakeRedis) Set(ctx context.Context, key string, _ interface{}, d time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStatusCmd(ctx)
	if f.err != nil {
		cmd.SetErr(f.err)
		return cmd
	}
	f.until[key] = f.now.Add(d)
	return cmd
}

func (f *fakeRedis) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	cmd := redis.NewDurationCmd(ctx, time.Millisecond)
	if f.err != nil {
		cmd.SetErr(f.err)
		return cmd
	}
	u, ok := f.until[key]
	if !ok || !u.After(*f.now) {
		cmd.SetVal(-2)
		return cmd
	}
	cmd.SetVal(u.Sub(*f.now))
	return cmd
}

// A key held on one replica is refused on another through Redis, for what is
// left of it, and on neither once it ends; the second replica keeps the answer
// so a client retrying through its cooldown costs one Redis read.
func TestStore_SharedThroughRedis(t *testing.T) {
	now := time.Unix(1_000, 0)
	rdb := &fakeRedis{now: &now, until: map[string]time.Time{}}
	a, b := New(rdb, "p:"), New(rdb, "p:")
	a.now = func() time.Time { return now }
	b.now = a.now
	ctx := context.Background()

	if left, err := b.Remaining(ctx, "bsc|192.0.2.1"); left != 0 || err != nil {
		t.Fatalf("Remaining before any Hold = %v, %v; want 0", left, err)
	}
	if err := a.Hold(ctx, "bsc|192.0.2.1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := rdb.until["p:bsc|192.0.2.1"]; !ok {
		t.Fatalf("Hold wrote %v, want the prefixed key", rdb.until)
	}
	now = now.Add(20 * time.Second)
	reads := rdb.reads
	for range 3 {
		if left, err := b.Remaining(ctx, "bsc|192.0.2.1"); left != 40*time.Second || err != nil {
			t.Fatalf("another replica's Remaining = %v, %v; want 40s", left, err)
		}
	}
	if rdb.reads-reads != 1 {
		t.Errorf("%d Redis reads for three checks, want 1", rdb.reads-reads)
	}
	if left, _ := b.Remaining(ctx, "eth|192.0.2.1"); left != 0 {
		t.Errorf("another key is held for %v", left)
	}
	now = now.Add(41 * time.Second)
	for name, s := range map[string]*Store{"writer": a, "reader": b} {
		if left, _ := s.Remaining(ctx, "bsc|192.0.2.1"); left != 0 {
			t.Errorf("%s still holds the key %v after it ended", name, left)
		}
	}
}

// Redis failing costs the other replicas the key, never the replica that
// held it, and a read fails open.
func TestStore_RedisDown(t *testing.T) {
	now := time.Unix(1_000, 0)
	rdb := &fakeRedis{now: &now, until: map[string]time.Time{}, err: errors.New("down")}
	s := New(rdb, "")
	s.now = func() time.Time { return now }
	ctx := context.Background()
	if err := s.Hold(ctx, "k", time.Minute); err == nil {
		t.Error("Hold hid Redis's error")
	}
	if left, err := s.Remaining(ctx, "k"); left != time.Minute || err != nil {
		t.Errorf("the holding replica's Remaining = %v, %v; want 1m from memory", left, err)
	}
	if left, err := New(rdb, "").Remaining(ctx, "k"); left != 0 || err == nil {
		t.Errorf("another replica's Remaining = %v, %v; want 0 and the error", left, err)
	}
}

// Without Redis a key is this replica's, and expired keys are dropped as new
// ones are held.
func TestStore_LocalOnly(t *testing.T) {
	now := time.Unix(1_000, 0)
	s := New(nil, "")
	s.now = func() time.Time { return now }
	ctx := context.Background()
	_ = s.Hold(ctx, "a", time.Second)
	_ = s.Hold(ctx, "zero", 0)
	if left, _ := s.Remaining(ctx, "a"); left != time.Second {
		t.Errorf("Remaining = %v, want 1s", left)
	}
	now = now.Add(2 * time.Second)
	_ = s.Hold(ctx, "b", time.Second)
	if len(s.held) != 1 {
		t.Errorf("%d keys held, want only the live one", len(s.held))
	}
}
