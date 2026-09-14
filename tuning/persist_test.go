package tuning

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/override"
)

// An override written through one store is what a second store, sharing the
// persistence, reads back on its first reload: the shape of a second replica
// or a restarted process.
func TestStore_PersistsAndReloads(t *testing.T) {
	shared := override.NewMemoryStore()
	a := NewStore(WithPersistence(shared, nil))
	if err := a.Set(KnobRetryMaxRetries, "", "5"); err != nil {
		t.Fatal(err)
	}
	if err := a.Set(KnobRetryMaxRetries, "eth", "1"); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := shared.Get(context.Background(), "tuning/retry.max_retries/eth"); !ok || v != "1" {
		t.Fatalf("persisted = %q,%v", v, ok)
	}

	b := NewStore(WithPersistence(shared, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Int(KnobRetryMaxRetries, "eth", 9) == 1 && b.Int(KnobRetryMaxRetries, "poly", 9) == 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.Int(KnobRetryMaxRetries, "eth", 9); got != 1 {
		t.Fatalf("second store eth = %d, want the persisted 1", got)
	}
	if got := b.Int(KnobRetryMaxRetries, "poly", 9); got != 5 {
		t.Fatalf("second store global = %d, want the persisted 5", got)
	}

	// A delete on one side reaches the other.
	a.Delete(KnobRetryMaxRetries, "eth")
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && b.Int(KnobRetryMaxRetries, "eth", 9) != 5 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := b.Int(KnobRetryMaxRetries, "eth", 9); got != 5 {
		t.Fatalf("after delete, eth = %d, want the global 5", got)
	}
	if a.Persistent() {
		t.Fatal("a memory persistence store is not shared, so Persistent must be false")
	}
}

// A persisted value that no longer parses (a knob renamed, a bad hand edit)
// is skipped with a warning, not fatal to the rest.
func TestStore_ReloadSkipsUnparseable(t *testing.T) {
	s := NewStore(WithPersistence(override.NewMemoryStore(), nil))
	s.applyPersisted(map[string]string{
		"tuning/retry.max_retries":     "5",
		"tuning/no.such.knob":          "1",
		"tuning/retry.max_retries/eth": "lots",
	})
	if got := s.Int(KnobRetryMaxRetries, "eth", 9); got != 5 {
		t.Fatalf("eth = %d, want the global 5 with the bad per-service value skipped", got)
	}
}
