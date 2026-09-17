package reputation

import (
	"context"
	"errors"
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
