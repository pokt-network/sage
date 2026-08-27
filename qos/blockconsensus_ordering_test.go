package qos

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// setStoreHook installs h for the duration of the test. Serialised by the
// caller's own sequencing: every test that uses it joins the goroutines it
// started before returning, so the clear below cannot race a live hook read.
func setStoreHook(t *testing.T, h func(string)) {
	t.Helper()
	storeHook.Store(&h)
	t.Cleanup(func() { storeHook.Store(nil) })
}

// TestBlockConsensus_ResetIsNotUndoneByAnInFlightObservation pins the ordering
// bug this test file exists for: AddObservation used to compute the perceived
// height under mu and store it after unlocking, so a Reset could take the
// lock, clear everything, publish 0 and return "reset: true" — and then the
// observation that was already in flight would store the poisoned height it
// had computed a moment earlier, on top of the reset.
//
// The window is a handful of instructions wide, so racing goroutines
// reproduce it only by luck. The hook makes it deterministic: it wedges the
// test between the computation and the store, and the whole question is
// whether mu is still held at that point. With the store inside the lock the
// Reset launched from the hook blocks until AddObservation is done and its 0
// is the last word; with the store outside, the Reset completes inside the
// hook and the observation lands after it.
func TestBlockConsensus_ResetIsNotUndoneByAnInFlightObservation(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)

	var once sync.Once
	var wg sync.WaitGroup
	setStoreHook(t, func(op string) {
		if op != "add" {
			return
		}
		once.Do(func() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bc.Reset()
			}()
			// Give the reset every chance to run. Holding mu, it cannot; not
			// holding mu, it certainly will.
			time.Sleep(50 * time.Millisecond)
		})
	})

	bc.AddObservation(domain.EndpointAddr("endpoint-1"), 1_000_000)
	wg.Wait()

	if got := bc.PerceivedBlock(); got != 0 {
		t.Fatalf("perceived height after a reset that raced an in-flight observation = %d, want 0: "+
			"the reset was undone by the observation it was meant to discard", got)
	}
}

// TestBlockConsensus_ResetDoesNotEraseALaterObservation is the same ordering
// seen from the other side: Reset used to publish its zeroes after unlocking,
// so an observation that took the lock the instant Reset released it would
// have its freshly computed height overwritten by the reset's trailing 0 —
// leaving the plugin reading a cold start it had already recovered from.
func TestBlockConsensus_ResetDoesNotEraseALaterObservation(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)

	var once sync.Once
	var wg sync.WaitGroup
	setStoreHook(t, func(op string) {
		if op != "reset" {
			return
		}
		once.Do(func() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bc.AddObservation(domain.EndpointAddr("endpoint-1"), 900)
			}()
			time.Sleep(50 * time.Millisecond)
		})
	})

	bc.Reset()
	wg.Wait()

	if got := bc.PerceivedBlock(); got != 900 {
		t.Fatalf("perceived height = %d, want 900: the reset's trailing store erased an observation "+
			"that arrived after it had already released the lock", got)
	}
}

// TestBlockConsensus_ResetRacesObservations is the concurrency guard rather
// than the ordering proof: it exists so -race walks both critical sections
// against each other. It is not discriminating on its own — after the
// observer has been joined there is no in-flight store left for a final Reset
// to lose to, which is exactly why the two tests above use the hook.
func TestBlockConsensus_ResetRacesObservations(t *testing.T) {
	bc := NewBlockConsensus(nil, 10)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for h := uint64(1); !stop.Load(); h++ {
			bc.AddObservation(domain.EndpointAddr("endpoint-1"), h)
		}
	}()

	for i := 0; i < 2000; i++ {
		bc.Reset()
	}

	stop.Store(true)
	wg.Wait()

	bc.Reset()
	if got := bc.PerceivedBlock(); got != 0 {
		t.Fatalf("perceived height after a final quiet reset = %d, want 0", got)
	}
}
