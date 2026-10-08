package reputation

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// dropCounts records what the write drop hook is told.
type dropCounts struct {
	mu sync.Mutex
	n  map[string]int
}

func (d *dropCounts) hook(reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.n == nil {
		d.n = map[string]int{}
	}
	d.n[reason]++
}

func (d *dropCounts) get(reason string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n[reason]
}

// A storm of signals on a few hundred keys holds one pending write per key,
// never more, and storage ends with each key's latest state: the fixed queue
// it replaced dropped 4,315 leader writes in a day (mainnet, 2026-10-01).
func TestWriteBehind_StormHoldsOneWritePerKey(t *testing.T) {
	store := &slowStorage{MemoryStorage: NewMemoryStorage(), batched: true}
	svc := NewService(store, nil, ServiceConfig{StateIdleTTL: -1})
	var drops dropCounts
	svc.SetWriteDropHook(drops.hook)

	const keys, signals = 500, 10_000
	for i := 0; i < signals; i++ {
		svc.enqueue(writeOp{key: scoreKey("eth", "https://backend-"+strconv.Itoa(i%keys)+".example"), state: State{Score: float64(i)}})
	}
	if got := svc.WriteQueueDepth(); got != keys {
		t.Fatalf("pending = %d, want one per key (%d)", got, keys)
	}
	svc.flushPending()
	writes, calls := store.counts()
	if writes != keys || calls != 1 {
		t.Fatalf("writes=%d calls=%d, want %d writes in one call", writes, calls, keys)
	}
	if n := len(drops.n); n != 0 {
		t.Fatalf("drops: %v", drops.n)
	}
	last := store.last[scoreKey("eth", "https://backend-7.example")]
	if last.Score != float64(signals-keys+7) || last.UpdatedAt == 0 {
		t.Fatalf("latest state not written: %+v", last)
	}
}

// A follower holds nothing: storage would discard it.
func TestWriteBehind_FollowerHoldsNothing(t *testing.T) {
	store := NewLeaderOnlyStorage(NewMemoryStorage(), func() bool { return false })
	svc := NewService(store, nil, ServiceConfig{})
	_ = svc.RecordSignal(context.Background(), "eth", "supA-https://node.example.com", domain.RPCTypeJSONRPC, NewSignal(SignalSuccess, "ok", 0))
	if got := svc.WriteQueueDepth(); got != 0 {
		t.Fatalf("follower pending = %d, want 0", got)
	}
}

// failingStorage refuses every write, as Redis does when it is unreachable.
type failingStorage struct{ *MemoryStorage }

func (failingStorage) SetState(context.Context, string, State) error {
	return errors.New("connection refused")
}

// A write storage refuses is lost as surely as one the queue refused.
func TestWriteBehind_StorageErrorCountsDrops(t *testing.T) {
	svc := NewService(failingStorage{NewMemoryStorage()}, nil, ServiceConfig{})
	var drops dropCounts
	svc.SetWriteDropHook(drops.hook)
	svc.Start()
	defer svc.Stop()

	_ = svc.RecordSignal(context.Background(), "eth", "supA-https://node.example.com", domain.RPCTypeJSONRPC, NewSignal(SignalSuccess, "ok", 0))
	deadline := time.Now().Add(2 * time.Second)
	for drops.get(WriteDropStorageError) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := drops.get(WriteDropStorageError); got != 1 {
		t.Errorf("storage_error drops = %d, want 1", got)
	}
}

// A follower's writes are discarded by LeaderOnlyStorage by design. That is
// not a drop, and it is not counted.
func TestWriteBehind_FollowerDiscardIsNotADrop(t *testing.T) {
	store := NewLeaderOnlyStorage(NewMemoryStorage(), func() bool { return false })
	svc := NewService(store, nil, ServiceConfig{})
	var drops dropCounts
	svc.SetWriteDropHook(drops.hook)
	svc.Start()
	defer svc.Stop()

	_ = svc.RecordSignal(context.Background(), "eth", "supA-https://node.example.com", domain.RPCTypeJSONRPC, NewSignal(SignalSuccess, "ok", 0))
	deadline := time.Now().Add(2 * time.Second)
	for svc.WriteQueueDepth() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if drops.get(WriteDropStorageError) != 0 {
		t.Errorf("a follower's discarded write was counted as a drop: %v", drops.n)
	}
}

// slowStorage answers every call after rtt, so a test can put a real Redis's
// round trip in front of the write-behind. batched decides whether a pass costs
// one round trip or one per key. The operator and party halves of Storage are
// the embedded MemoryStorage's; only the per-key state is slowed.
type slowStorage struct {
	*MemoryStorage
	rtt     time.Duration
	batched bool

	mu     sync.Mutex
	writes int
	calls  int
	last   map[string]State
}

func (s *slowStorage) GetStates(context.Context) (map[string]State, error) {
	return map[string]State{}, nil
}

func (s *slowStorage) SetState(_ context.Context, _ string, _ State) error {
	time.Sleep(s.rtt)
	s.mu.Lock()
	s.writes++
	s.calls++
	s.mu.Unlock()
	return nil
}

func (s *slowStorage) SetStates(_ context.Context, states map[string]State) error {
	time.Sleep(s.rtt)
	s.mu.Lock()
	s.writes += len(states)
	s.calls++
	if s.last == nil {
		s.last = map[string]State{}
	}
	for k, v := range states {
		s.last[k] = v
	}
	s.mu.Unlock()
	return nil
}

func (s *slowStorage) counts() (writes, calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes, s.calls
}

// unbatchedStorage is a Storage and nothing more: no SetStates to promote, so
// the service's BatchWriter assertion fails and it falls back to one write per
// round trip — the behaviour before batching. It must NOT embed slowStorage,
// which would promote SetStates and quietly make both halves of the test batch.
type unbatchedStorage struct {
	*MemoryStorage // the operator and party halves; MemoryStorage has no SetStates
	inner          *slowStorage
}

func (u unbatchedStorage) GetStates(ctx context.Context) (map[string]State, error) {
	return u.inner.GetStates(ctx)
}

func (u unbatchedStorage) SetState(ctx context.Context, key string, st State) error {
	return u.inner.SetState(ctx, key, st)
}

var (
	_ Storage     = unbatchedStorage{}
	_ BatchWriter = (*slowStorage)(nil)
)

// The fallback has to be the fallback: a storage that cannot batch must not
// somehow reach SetStates, or the load test below proves nothing.
func TestWriteBehind_StorageWithoutBatchingFallsBackPerKey(t *testing.T) {
	slow := &slowStorage{MemoryStorage: NewMemoryStorage()}
	svc := NewService(unbatchedStorage{MemoryStorage: NewMemoryStorage(), inner: slow}, nil, ServiceConfig{StateIdleTTL: -1})
	svc.Start()
	svc.enqueue(writeOp{key: scoreKey("eth", "a"), state: State{Score: 1}})
	svc.Stop()

	writes, calls := slow.counts()
	if writes != 1 || calls != 1 {
		t.Errorf("writes=%d calls=%d, want one SetState per write", writes, calls)
	}
}
