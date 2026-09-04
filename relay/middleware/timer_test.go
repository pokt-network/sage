package middleware

import (
	"testing"
	"time"
)

// The pool hands out timers that fire once at the requested delay and can be
// returned in either state — fired or not — without a stale tick leaking
// into the next borrower. Hedge relies on both.
func TestTimerPool_FiresOnceAndReturnsClean(t *testing.T) {
	tm := acquireTimer(5 * time.Millisecond)
	select {
	case <-tm.C:
	case <-time.After(time.Second):
		t.Fatal("timer never fired")
	}
	releaseTimer(tm) // fired: nothing left to drain

	again := acquireTimer(time.Hour)
	select {
	case <-again.C:
		t.Fatal("a reacquired timer delivered a stale tick from its previous life")
	default:
	}
	releaseTimer(again) // not fired: Stop succeeds, channel stays empty

	third := acquireTimer(time.Millisecond)
	select {
	case <-third.C:
	case <-time.After(time.Second):
		t.Fatal("a timer released unfired must still fire when reset")
	}
	releaseTimer(third)
}
