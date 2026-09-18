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

// A full queue used to lose writes silently. With nothing draining a queue of
// two, the third and later signals are counted as queue_full drops.
func TestWriteBehind_FullQueueCountsDrops(t *testing.T) {
	svc := NewService(NewMemoryStorage(), nil, ServiceConfig{WriteQueueSize: 2})
	var drops dropCounts
	svc.SetWriteDropHook(drops.hook)

	for i := range 5 {
		ep := domain.EndpointAddr("supA-https://node" + string(rune('a'+i)) + ".example.com")
		if err := svc.RecordSignal(context.Background(), "eth", ep, domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0)); err != nil {
			t.Fatal(err)
		}
	}
	if got := svc.WriteQueueDepth(); got != 2 {
		t.Errorf("queue depth = %d, want 2 (full)", got)
	}
	if got := drops.get(WriteDropQueueFull); got != 3 {
		t.Errorf("queue_full drops = %d, want 3", got)
	}
}

// failingStorage refuses every write, as Redis does when it is unreachable.
type failingStorage struct{ *MemoryStorage }

func (failingStorage) SetState(context.Context, string, State) error {
	return errors.New("connection refused")
}

// A write storage refuses is lost as surely as one the queue refused.
func TestWriteBehind_StorageErrorCountsDrops(t *testing.T) {
	svc := NewService(failingStorage{NewMemoryStorage()}, nil, DefaultServiceConfig())
	var drops dropCounts
	svc.SetWriteDropHook(drops.hook)
	svc.Start()
	defer svc.Stop()

	_ = svc.RecordSignal(context.Background(), "eth", "supA-https://node.example.com", domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0))
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
	svc := NewService(store, nil, DefaultServiceConfig())
	var drops dropCounts
	svc.SetWriteDropHook(drops.hook)
	svc.Start()
	defer svc.Stop()

	_ = svc.RecordSignal(context.Background(), "eth", "supA-https://node.example.com", domain.RPCTypeJSONRPC, NewSuccessSignal("ok", 0))
	deadline := time.Now().Add(2 * time.Second)
	for svc.WriteQueueDepth() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if drops.get(WriteDropQueueFull)+drops.get(WriteDropStorageError) != 0 {
		t.Errorf("a follower's discarded write was counted as a drop: %v", drops.n)
	}
}

// slowStorage answers every call after rtt, so a test can put a real Redis's
// round trip in front of the write-behind. batched decides whether a pass costs
// one round trip or one per key.
type slowStorage struct {
	rtt     time.Duration
	batched bool

	mu     sync.Mutex
	writes int
	calls  int
}

func (s *slowStorage) GetState(context.Context, string) (State, error) {
	return State{}, ErrStateNotFound
}

func (s *slowStorage) GetStates(context.Context, string) (map[string]State, error) {
	return map[string]State{}, nil
}

func (s *slowStorage) DeleteState(context.Context, string) error { return nil }

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
type unbatchedStorage struct{ inner *slowStorage }

func (u unbatchedStorage) GetState(ctx context.Context, key string) (State, error) {
	return u.inner.GetState(ctx, key)
}

func (u unbatchedStorage) GetStates(ctx context.Context, prefix string) (map[string]State, error) {
	return u.inner.GetStates(ctx, prefix)
}

func (u unbatchedStorage) DeleteState(ctx context.Context, key string) error {
	return u.inner.DeleteState(ctx, key)
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
	slow := &slowStorage{rtt: 0}
	svc := NewService(unbatchedStorage{slow}, nil, ServiceConfig{WriteQueueSize: 8, StateIdleTTL: -1})
	svc.Start()
	svc.enqueue(writeOp{key: scoreKey("eth", "a"), state: State{Score: 1}})
	svc.Stop()

	writes, calls := slow.counts()
	if writes != 1 || calls != 1 {
		t.Errorf("writes=%d calls=%d, want one SetState per write", writes, calls)
	}
}

// The write-behind must keep up with mainnet's arrival rate. On 2026-09-18 the
// leader took 3,119 writes/s against a 0.38ms Redis and lost 472/s to a full
// queue, because one round trip per write caps the drain at 1/RTT. Batching
// makes the ceiling one round trip per PASS, so the same rate costs nothing.
//
// Both halves run here: the unbatched storage must drop, or the test is not
// measuring what it claims, and the batched one must not.
func TestWriteBehind_KeepsUpWithMainnetWriteRate(t *testing.T) {
	if testing.Short() {
		t.Skip("paced load test; runs in make test_all")
	}
	const (
		rtt      = 380 * time.Microsecond // mainnet's measured Redis round trip
		perSec   = 3100                   // mainnet's arrival rate
		duration = 2 * time.Second
		// Small on purpose: mainnet's 4,096 slots take 8.7s to fill at the
		// observed deficit, and the deficit is the thing under test, not how
		// many seconds of it a queue can hide.
		queue = 256
		keys  = 2000
	)

	run := func(t *testing.T, storage Storage) (dropped, peakDepth int) {
		t.Helper()
		svc := NewService(storage, nil, ServiceConfig{WriteQueueSize: queue, StateIdleTTL: -1})
		var drops dropCounts
		svc.SetWriteDropHook(drops.hook)
		svc.Start()
		defer svc.Stop()

		const perTick = 31
		ticks := int(duration / (10 * time.Millisecond)) // 31 per 10ms = 3,100/s
		sent := 0
		for i := 0; i < ticks; i++ {
			for j := 0; j < perTick; j++ {
				svc.enqueue(writeOp{
					key:   scoreKey("eth", "https://backend-"+strconv.Itoa(sent%keys)+".example"),
					state: State{Score: float64(sent % 100)},
				})
				sent++
			}
			if d := svc.WriteQueueDepth(); d > peakDepth {
				peakDepth = d
			}
			time.Sleep(10 * time.Millisecond)
		}
		if sent != perSec*int(duration/time.Second) {
			t.Fatalf("test paced %d writes, meant to pace %d", sent, perSec*int(duration/time.Second))
		}
		return drops.get(WriteDropQueueFull), peakDepth
	}

	slow := &slowStorage{rtt: rtt}
	t.Run("one round trip per write drops", func(t *testing.T) {
		dropped, peak := run(t, unbatchedStorage{inner: slow})
		writes, calls := slow.counts()
		t.Logf("unbatched: dropped=%d peak_depth=%d writes=%d calls=%d", dropped, peak, writes, calls)
		if dropped == 0 {
			t.Error("a write per round trip cannot keep up with 3,100/s at 0.38ms; the test is not loading it")
		}
	})

	t.Run("one round trip per pass does not", func(t *testing.T) {
		batched := &slowStorage{rtt: rtt, batched: true}
		dropped, peak := run(t, batched)
		writes, calls := batched.counts()
		t.Logf("batched: dropped=%d peak_depth=%d writes=%d calls=%d", dropped, peak, writes, calls)
		if dropped != 0 {
			t.Errorf("dropped %d writes at mainnet's rate; the batch is meant to make the drain keep up", dropped)
		}
		if writes == 0 {
			t.Fatal("nothing was written")
		}
		if calls >= writes {
			t.Errorf("calls=%d writes=%d: the pass is not batching", calls, writes)
		}
	})
}
