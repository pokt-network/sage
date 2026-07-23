package websockets

import (
	"sync"
	"testing"
)

func TestConnectionLimiter_AcquireRelease(t *testing.T) {
	l := NewConnectionLimiter(2)

	if !l.Acquire() {
		t.Fatal("the first acquire must succeed")
	}
	if !l.Acquire() {
		t.Fatal("the second acquire must succeed at a cap of 2")
	}
	if l.Acquire() {
		t.Error("the third acquire must fail at a cap of 2")
	}
	if got := l.Active(); got != 2 {
		t.Errorf("Active() = %d, want 2", got)
	}

	l.Release()
	if !l.Acquire() {
		t.Error("a released slot must be reusable")
	}
}

// A nil limiter is the "no cap" case, and it has to be usable without any
// caller checking for nil.
func TestConnectionLimiter_NilIsUnlimited(t *testing.T) {
	var l *ConnectionLimiter

	for i := 0; i < 100; i++ {
		if !l.Acquire() {
			t.Fatalf("nil limiter refused acquire %d", i)
		}
	}
	if got := l.Active(); got != 0 {
		t.Errorf("nil limiter Active() = %d, want 0", got)
	}
	l.Release() // must not panic
}

func TestNewConnectionLimiter_NonPositiveDisables(t *testing.T) {
	for _, max := range []int{0, -1, -10000} {
		if l := NewConnectionLimiter(max); l != nil {
			t.Errorf("NewConnectionLimiter(%d) = %v, want nil (disabled)", max, l)
		}
	}
}

// The cap must hold under contention, and — because Acquire uses a CAS loop
// rather than add-then-rollback — Active() must never be observed above it.
// An optimistic add would overshoot transiently, making the counter wrong
// exactly when something is reading it to decide whether to reject.
func TestConnectionLimiter_ConcurrentAcquireNeverExceedsCap(t *testing.T) {
	const (
		cap        = 50
		goroutines = 500
	)
	l := NewConnectionLimiter(cap)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		granted  int
		maxSeen  int64
		overshot bool
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !l.Acquire() {
				return
			}
			// Hold the slot: this counts peak concurrent grants, not throughput.
			active := l.Active()
			mu.Lock()
			granted++
			if active > maxSeen {
				maxSeen = active
			}
			if active > cap {
				overshot = true
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if granted != cap {
		t.Errorf("granted = %d, want exactly %d — no slot may be lost or double-issued", granted, cap)
	}
	if overshot {
		t.Errorf("Active() was observed above the cap (peak %d); the counter overshot", maxSeen)
	}
	if got := l.Active(); got != cap {
		t.Errorf("Active() = %d, want %d", got, cap)
	}
}

// Release must return slots even while others are contending for them.
func TestConnectionLimiter_ConcurrentAcquireRelease(t *testing.T) {
	l := NewConnectionLimiter(10)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Acquire() {
				l.Release()
			}
		}()
	}
	wg.Wait()

	if got := l.Active(); got != 0 {
		t.Errorf("Active() = %d after every acquire was released, want 0", got)
	}
}
