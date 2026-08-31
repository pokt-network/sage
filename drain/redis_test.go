package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/redis/go-redis/v9"
)

// fakeRedis is an in-memory stand-in for the three hash commands
// drain.RedisClient names, keyed by field: the store only ever touches one
// hash, so the hash key itself is checked and otherwise ignored.
type fakeRedis struct {
	mu   sync.Mutex
	data map[string]string

	hsetErr    error // when non-nil, every HSet fails with it
	hgetallErr error // when non-nil, every HGetAll fails with it
	hdelErr    error // when non-nil, every HDel fails with it

	// hgetallCalls counts the HGetAll calls, so a test can pin that a refresh
	// is one round trip however many drains there are.
	hgetallCalls int

	// setEntered/setGate let a test hold one HSet on the wire: HSet signals
	// setEntered and then blocks until setGate is closed. Both are read before
	// f.mu is taken, so an HDel issued meanwhile is not serialized behind the
	// blocked write — that serialization would hide the very race under test.
	// The gate is one-shot: it catches the first HSet and disarms, so a second
	// write issued while the first is stalled provably lands first.
	setEntered chan struct{}
	setGate    chan struct{}

	// afterHGetAll, when set, runs after HGetAll has been answered and f.mu
	// released — the window between a refresh's snapshot and its replace.
	afterHGetAll func()
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{data: make(map[string]string)}
}

// blockSet arms the gate that holds the next HSet on the wire, and clears
// hsetErr so that write succeeds. The returned channels are (entered, gate).
func (f *fakeRedis) blockSet() (chan struct{}, chan struct{}) {
	entered, gate := make(chan struct{}, 1), make(chan struct{})
	f.mu.Lock()
	f.hsetErr = nil
	f.setEntered, f.setGate = entered, gate
	f.mu.Unlock()
	return entered, gate
}

func (f *fakeRedis) checkKey(t string) {
	if t != hashKey {
		panic("fakeRedis: unexpected hash key " + t)
	}
}

func (f *fakeRedis) HSet(_ context.Context, key string, values ...interface{}) *redis.IntCmd {
	f.checkKey(key)
	f.mu.Lock()
	entered, gate := f.setEntered, f.setGate
	f.setEntered, f.setGate = nil, nil
	f.mu.Unlock()
	if gate != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hsetErr != nil {
		return redis.NewIntResult(0, f.hsetErr)
	}
	if len(values)%2 != 0 {
		panic("fakeRedis: HSet wants field/value pairs")
	}
	var n int64
	for i := 0; i < len(values); i += 2 {
		field := values[i].(string)
		var s string
		switch v := values[i+1].(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		default:
			s = fmt.Sprint(v)
		}
		if _, exists := f.data[field]; !exists {
			n++
		}
		f.data[field] = s
	}
	return redis.NewIntResult(n, nil)
}

func (f *fakeRedis) HDel(_ context.Context, key string, fields ...string) *redis.IntCmd {
	f.checkKey(key)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hdelErr != nil {
		return redis.NewIntResult(0, f.hdelErr)
	}
	var n int64
	for _, k := range fields {
		if _, ok := f.data[k]; ok {
			delete(f.data, k)
			n++
		}
	}
	return redis.NewIntResult(n, nil)
}

func (f *fakeRedis) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	f.checkKey(key)
	cmd := f.hgetall(ctx)
	if f.afterHGetAll != nil {
		f.afterHGetAll()
	}
	return cmd
}

func (f *fakeRedis) hgetall(ctx context.Context) *redis.MapStringStringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hgetallCalls++
	cmd := redis.NewMapStringStringCmd(ctx)
	if f.hgetallErr != nil {
		cmd.SetErr(f.hgetallErr)
		return cmd
	}
	out := make(map[string]string, len(f.data))
	for k, v := range f.data {
		out[k] = v
	}
	cmd.SetVal(out)
	return cmd
}

// put writes a field as if another replica had done it.
func (f *fakeRedis) put(field, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[field] = value
}

func (f *fakeRedis) get(field string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[field]
	return v, ok
}

func (f *fakeRedis) hgetallCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hgetallCalls
}

func (f *fakeRedis) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}

// wipe empties the fake, standing in for a release performed on another replica.
func (f *fakeRedis) wipe() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = make(map[string]string)
}

func TestRedisStore_SetWritesFieldAndValue(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	until := time.Now().Add(30 * time.Minute)

	err := s.Set(context.Background(), Entry{
		Key:    Key{ServiceID: "eth", Operator: "Slow.Example", RPCType: domain.RPCTypeWebSocket},
		Until:  until,
		Reason: "flaky",
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	const wantKey = "eth:slow.example:websocket"
	raw, ok := fake.get(wantKey)
	if !ok {
		t.Fatalf("no key %q in redis; have %d keys", wantKey, fake.len())
	}

	var got struct {
		Until  time.Time `json:"until"`
		Reason string    `json:"reason"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("value %q is not the documented JSON: %v", raw, err)
	}
	if got.Until.Sub(until).Abs() > time.Second {
		t.Errorf("until = %v, want ~%v", got.Until, until)
	}
	if got.Reason != "flaky" {
		t.Errorf("reason = %q, want %q", got.Reason, "flaky")
	}

	// No per-field TTL exists in a hash: the until IN the value is what every
	// replica checks on read, and refresh deletes expired fields. See
	// TestRedisStore_RefreshDeletesExpiredFields.
}

func TestRedisStore_UnscopedDrainUsesAllSegment(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)

	if err := s.Set(context.Background(), Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example"},
		Until: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := fake.get("eth:slow.example:all"); !ok {
		t.Fatal(`an unscoped drain must use the "all" segment`)
	}
}

func TestRedisStore_DrainedReadsLocal(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	ctx := context.Background()

	if err := s.Set(ctx, Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("Drained must answer from the local cache immediately after Set")
	}
	active := s.Active(ctx, "eth")
	if len(active) != 1 || active[0].Operator != "slow.example" {
		t.Fatalf("Active = %+v, want one entry for slow.example", active)
	}
}

func TestRedisStore_ReleaseDeletesBoth(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Release(ctx, k); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Error("released drain still matches locally")
	}
	if fake.len() != 0 {
		t.Errorf("released drain still in redis: %d keys", fake.len())
	}
}

func TestRedisStore_PropagationFailureStillDrainsLocally(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)

	err := s.Set(context.Background(), Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(time.Minute),
	})
	if !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("Set error %q must name the underlying Redis failure", err)
	}
	// "This pod only": the operator is told the drain did not propagate, but
	// the instance that took the request honors it regardless.
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("a drain whose Redis write failed must still apply locally")
	}
}

func TestRedisStore_ReleasePropagationFailureStillReleasesLocally(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fake.hdelErr = errors.New("connection refused")

	if err := s.Release(ctx, k); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Release error = %v, want one wrapping ErrPropagation", err)
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("a release whose Redis delete failed must still take effect locally")
	}
}

func TestRedisStore_PendingDrainSurvivesRefresh(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()

	err := s.Set(ctx, Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(time.Minute),
	})
	if !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}
	if s.pendingCount() != 1 {
		t.Fatalf("pendingCount = %d, want 1", s.pendingCount())
	}

	// The fake holds no keys, so the refresh rebuilds an empty map. The drain
	// must outlive that: ErrPropagation promised a drain on this pod, and one
	// cache TTL is not the duration the admin asked for.
	s.refresh(ctx)

	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("a drain that failed to propagate must survive the refresh that rebuilds from redis")
	}
	if s.pendingCount() != 1 {
		t.Errorf("pendingCount = %d, want the entry still pending while redis is down", s.pendingCount())
	}
}

func TestRedisStore_PendingDrainPropagatesWhenRedisRecovers(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()
	e := Entry{
		Key:    Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until:  time.Now().Add(time.Minute),
		Reason: "flaky",
	}
	if err := s.Set(ctx, e); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}

	fake.hsetErr = nil // redis comes back
	s.refresh(ctx)

	raw, ok := fake.get("eth:slow.example:json_rpc")
	if !ok {
		t.Fatal("the refresh must retry the write that failed, not just hold the drain locally")
	}
	if !strings.Contains(raw, "flaky") {
		t.Errorf("propagated value %q lost the reason", raw)
	}
	if s.pendingCount() != 0 {
		t.Errorf("pendingCount = %d, want 0 once the entry propagated", s.pendingCount())
	}
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Error("the drain must not blink off on the tick that propagates it")
	}
}

func TestRedisStore_ReleaseClearsAPendingDrain(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}
	if err := s.Release(ctx, k); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if s.pendingCount() != 0 {
		t.Fatalf("pendingCount = %d, want the released entry gone", s.pendingCount())
	}

	fake.hsetErr = nil
	s.refresh(ctx)

	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("the refresh loop re-installed a drain the admin had released")
	}
	if fake.len() != 0 {
		t.Errorf("a released pending entry must not be written to redis on retry: %d keys", fake.len())
	}
}

func TestRedisStore_ExpiredPendingDrainIsDropped(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()

	if err := s.Set(ctx, Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(20 * time.Millisecond),
	}); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}

	time.Sleep(40 * time.Millisecond)
	fake.hsetErr = nil
	s.refresh(ctx)

	if s.pendingCount() != 0 {
		t.Errorf("pendingCount = %d, want an expired pending entry dropped", s.pendingCount())
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Error("an expired pending entry must not be resurrected by the refresh")
	}
	if fake.len() != 0 {
		t.Error("an expired pending entry must not be propagated")
	}
}

func TestRedisStore_SetPropagatingLaterClearsPending(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}

	fake.hsetErr = nil
	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(2 * time.Minute)}); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	if s.pendingCount() != 0 {
		t.Errorf("pendingCount = %d, want 0 after a Set that reached redis", s.pendingCount())
	}
}

func TestRedisStore_EncodingFailureIsNotAPropagationError(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)

	// time.Time refuses to marshal a year outside [0,9999].
	err := s.Set(context.Background(), Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("Set must report an unencodable entry")
	}
	// Retrying a deterministic encoding failure every tick forever is noise, not
	// resilience — so it is neither an ErrPropagation nor a pending entry.
	if errors.Is(err, ErrPropagation) {
		t.Errorf("encoding failure = %v, want a plain error, not ErrPropagation", err)
	}
	if s.pendingCount() != 0 {
		t.Errorf("pendingCount = %d, want an unencodable entry not queued for retry", s.pendingCount())
	}
}

func TestRedisStore_ReleaseBeatsAnInFlightRetry(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}

	// Redis "recovers", but the retry's write stalls on the wire.
	entered, gate := fake.blockSet()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.refresh(ctx)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry never reached the redis write")
	}

	// The admin releases the drain while the retry is still on the wire. Its
	// Del lands first; the stale write lands second, and last write wins.
	if err := s.Release(ctx, k); err != nil {
		t.Fatalf("Release: %v", err)
	}

	close(gate)
	<-done

	if _, ok := fake.get("eth:slow.example:json_rpc"); ok {
		t.Error("a write that raced a release must be undone, not left to win in redis")
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("the refresh re-installed locally a drain the admin released mid-flight")
	}
	if s.pendingCount() != 0 {
		t.Errorf("pendingCount = %d, want the released entry gone", s.pendingCount())
	}
}

func TestRedisStore_NewerSetBeatsAnInFlightRetry(t *testing.T) {
	fake := newFakeRedis()
	fake.hsetErr = errors.New("connection refused")
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute), Reason: "old"}); !errors.Is(err, ErrPropagation) {
		t.Fatalf("Set error = %v, want one wrapping ErrPropagation", err)
	}

	entered, gate := fake.blockSet()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.refresh(ctx)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry never reached the redis write")
	}

	// A second Set overtakes the stalled retry. The gate is one-shot, so this
	// write goes straight through and the stale retry is the one that lands
	// last — exactly the ordering redis resolves as "last write wins".
	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(2 * time.Minute), Reason: "new"}); err != nil {
		t.Fatalf("second Set: %v", err)
	}

	close(gate)
	<-done

	// The newer drain is still the one in force here, and is queued to rewrite
	// whatever the stale write left behind in redis.
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("the newer drain was lost when a stale retry landed after it")
	}
	active := s.Active(ctx, "eth")
	if len(active) != 1 || active[0].Reason != "new" {
		t.Fatalf("Active = %+v, want the newer entry", active)
	}
	if s.pendingCount() != 1 {
		t.Fatalf("pendingCount = %d, want the newer entry re-queued after a stale write clobbered it", s.pendingCount())
	}

	// One more tick and redis agrees with this instance again.
	s.refresh(ctx)
	raw, ok := fake.get("eth:slow.example:json_rpc")
	if !ok || !strings.Contains(raw, "new") {
		t.Fatalf("redis holds %q, want the newer drain rewritten over the stale one", raw)
	}
	if s.pendingCount() != 0 {
		t.Errorf("pendingCount = %d, want 0 once the rewrite landed", s.pendingCount())
	}
}

func TestRedisStore_RefreshAdoptsAnotherReplicasDrain(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	until := time.Now().Add(time.Minute)
	body, _ := json.Marshal(map[string]any{"until": until, "reason": "set elsewhere"})
	fake.put("eth:other.example:all", string(body))

	s.refresh(context.Background())

	if !s.Drained("eth", "other.example", domain.RPCTypeJSONRPC) {
		t.Fatal("a drain set on another replica must arrive on refresh")
	}
	active := s.Active(context.Background(), "eth")
	if len(active) != 1 || active[0].Reason != "set elsewhere" {
		t.Fatalf("Active = %+v, want the peer's entry with its reason", active)
	}
}

func TestRedisStore_RefreshDropsDrainReleasedElsewhere(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	ctx := context.Background()

	if err := s.Set(ctx, Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fake.wipe() // another replica released it

	s.refresh(ctx)

	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("refresh must replace the local map, not merge into it, or a release elsewhere never lands")
	}
}

func TestRedisStore_RefreshSkipsExpiredAndMalformedEntries(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	past, _ := json.Marshal(map[string]any{"until": time.Now().Add(-time.Minute)})
	live, _ := json.Marshal(map[string]any{"until": time.Now().Add(time.Minute)})
	fake.put("eth:expired.example:all", string(past))
	fake.put("eth:garbage.example:all", "not json")
	fake.put("eth:short", string(live)) // malformed key
	fake.put("eth:good.example:all", string(live))

	s.refresh(context.Background())

	if s.Drained("eth", "expired.example", domain.RPCTypeJSONRPC) {
		t.Error("an already-expired redis entry must not become a live drain")
	}
	if s.Drained("eth", "garbage.example", domain.RPCTypeJSONRPC) {
		t.Error("an unparseable value must not become a live drain")
	}
	if !s.Drained("eth", "good.example", domain.RPCTypeJSONRPC) {
		t.Error("one bad entry must not cost the good ones")
	}
}

// TestRedisStore_RefreshIsOneHGetAll pins the read command. The previous
// layout, one key per drain, had to be enumerated with SCAN over the whole
// keyspace; measured against a Redis holding 500k unrelated keys that was
// ~1,950 SCAN calls and ~158 ms of Redis CPU per refresh tick per replica.
// One HGETALL costs what the drains cost, and drain.RedisClient no longer
// names SCAN or KEYS at all, so the compiler is the first line of this defence
// and this test is the second.
func TestRedisStore_RefreshIsOneHGetAll(t *testing.T) {
	fake := newFakeRedis()
	body, err := json.Marshal(payload{Until: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	operators := []string{"a.example", "b.example", "c.example", "d.example", "e.example"}
	for _, op := range operators {
		fake.put("eth:"+op+":all", string(body))
	}

	s := NewRedisStore(fake)
	s.refresh(context.Background())

	if got := fake.hgetallCallCount(); got != 1 {
		t.Fatalf("HGETALL calls = %d, want exactly 1 for %d drains", got, len(operators))
	}
	for _, op := range operators {
		if !s.Drained("eth", op, domain.RPCTypeJSONRPC) {
			t.Errorf("%s is not drained: the read lost a field", op)
		}
	}
}

// A hash field cannot expire on its own. Refresh must delete the fields whose
// until has passed, or the hash holds every drain ever set.
func TestRedisStore_RefreshDeletesExpiredFields(t *testing.T) {
	fake := newFakeRedis()
	past, _ := json.Marshal(payload{Until: time.Now().Add(-time.Minute)})
	live, _ := json.Marshal(payload{Until: time.Now().Add(time.Minute)})
	fake.put("eth:expired.example:all", string(past))
	fake.put("eth:live.example:all", string(live))

	s := NewRedisStore(fake)
	s.refresh(context.Background())

	if _, still := fake.get("eth:expired.example:all"); still {
		t.Error("an expired field must be deleted from the hash on refresh")
	}
	if _, kept := fake.get("eth:live.example:all"); !kept {
		t.Error("a live field must be left alone")
	}
}

func TestRedisStore_RefreshKeepsLocalStateOnRedisError(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	ctx := context.Background()
	if err := s.Set(ctx, Entry{
		Key:   Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		Until: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	fake.hgetallErr = errors.New("redis down")
	s.refresh(ctx)
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("an HGETALL failure must not un-drain what is already benched")
	}
}

// waitDrained polls until the drain lands or the test fails.
func waitDrained(t *testing.T, s *RedisStore, operator, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !s.Drained("eth", operator, domain.RPCTypeJSONRPC) {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRedisStore_StartRefreshesImmediately(t *testing.T) {
	fake := newFakeRedis()
	body, _ := json.Marshal(map[string]any{"until": time.Now().Add(time.Minute)})
	fake.put("eth:other.example:all", string(body))

	// A TTL far longer than the test: only an immediate first refresh can make
	// this pass. A pod that boots into a fleet with a live drain must not route
	// to the benched operator for a whole cache TTL first.
	s := NewRedisStore(fake, WithCacheTTL(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)
	waitDrained(t, s, "other.example", "Start must refresh once before waiting out the first tick")
}

func TestRedisStore_StartKeepsRefreshingOnTheTicker(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake, WithCacheTTL(2*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	// Written well after the initial refresh, so only a later tick can see it.
	time.Sleep(20 * time.Millisecond)
	body, _ := json.Marshal(map[string]any{"until": time.Now().Add(time.Minute)})
	fake.put("eth:other.example:all", string(body))

	waitDrained(t, s, "other.example", "Start's refresh loop never picked up a peer's drain")
}

func TestRedisStore_NilClientIsAPlainMemoryStore(t *testing.T) {
	s := NewRedisStore(nil)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("Set with nil client: %v", err)
	}
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("nil client must still drain locally")
	}
	if err := s.Release(ctx, k); err != nil {
		t.Fatalf("Release with nil client: %v", err)
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("nil client must still release locally")
	}

	// Start must be a no-op rather than a ticker calling into a nil client.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	s.refresh(ctx)
}

func TestRedisStore_SetInThePastReleases(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	ctx := context.Background()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}

	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Set(ctx, Entry{Key: k, Until: time.Now().Add(-time.Second)}); err != nil {
		t.Fatalf("releasing Set: %v", err)
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Error("a Set whose Until has passed must release, not install")
	}
	if fake.len() != 0 {
		t.Error("a releasing Set must delete the redis key too, not leave a stale one for peers")
	}
}

func TestRedisStore_KeyRoundTrip(t *testing.T) {
	for _, k := range []Key{
		{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC},
		{ServiceID: "poly", Operator: "a.b.example", RPCType: ""},
		{ServiceID: "xrplevm-testnet", Operator: "node.example.co.uk", RPCType: domain.RPCTypeWebSocket},
	} {
		got, ok := parseRedisField(redisField(k))
		if !ok {
			t.Fatalf("parseRedisField(%q) failed", redisField(k))
		}
		if got != k {
			t.Errorf("round trip of %+v gave %+v", k, got)
		}
	}
	for _, bad := range []string{"", ":", "eth:op", "other:prefix:a:b:c", "eth:op:all:extra", ":op:all"} {
		if _, ok := parseRedisField(bad); ok {
			t.Errorf("parseRedisField(%q) must reject a malformed key", bad)
		}
	}
}

func TestRedisStore_SatisfiesStore(t *testing.T) {
	var _ Store = NewRedisStore(nil)
}

// A Set that lands after a refresh took its snapshot and before it replaced the
// local map used to be wiped for one tick: the snapshot did not have it, and
// replaceAll installed the snapshot. The entry is newer than the snapshot and
// must survive until the next refresh finds it in Redis.
func TestRedisStore_SetDuringRefreshSurvivesTheReplace(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	entry := Entry{
		Key:   Key{ServiceID: "eth", Operator: "late.example"},
		Until: time.Now().Add(time.Minute),
	}
	fake.afterHGetAll = func() {
		fake.afterHGetAll = nil // Once: the Set below writes to Redis too.
		if err := s.Set(context.Background(), entry); err != nil {
			t.Errorf("Set: %v", err)
		}
	}

	s.refresh(context.Background())

	if !s.Drained("eth", "late.example", domain.RPCTypeJSONRPC) {
		t.Fatal("a drain set while the refresh was in flight was wiped by the replace")
	}
	// The next refresh sees it in Redis and keeps it the ordinary way.
	s.refresh(context.Background())
	if !s.Drained("eth", "late.example", domain.RPCTypeJSONRPC) {
		t.Fatal("drain lost on the refresh after the one it raced")
	}
}

// TestRedisStore_ReleaseDuringRefreshIsNotResurrected is the release-side twin
// of TestRedisStore_SetDuringRefreshSurvivesTheReplace: a key the scan saw and
// the admin released after the values were read but before the replace must
// not come back for one tick.
func TestRedisStore_ReleaseDuringRefreshIsNotResurrected(t *testing.T) {
	fake := newFakeRedis()
	s := NewRedisStore(fake)
	entry := Entry{
		Key:   Key{ServiceID: "eth", Operator: "gone.example"},
		Until: time.Now().Add(time.Minute),
	}
	if err := s.Set(context.Background(), entry); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fake.afterHGetAll = func() {
		fake.afterHGetAll = nil
		if err := s.Release(context.Background(), entry.Key); err != nil {
			t.Errorf("Release: %v", err)
		}
	}

	s.refresh(context.Background())

	if s.Drained("eth", "gone.example", domain.RPCTypeJSONRPC) {
		t.Fatal("a drain released while the refresh was in flight was resurrected by the replace")
	}
}
