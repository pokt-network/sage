package middleware

import (
	"sync"
	"time"
)

// timerPool reuses time.Timer instances to avoid allocations in the hedge
// hot path.
var timerPool = sync.Pool{
	New: func() any {
		t := time.NewTimer(0)
		// Drain the channel so the timer is ready to be reset.
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		return t
	},
}

func acquireTimer(d time.Duration) *time.Timer {
	t := timerPool.Get().(*time.Timer)
	t.Reset(d)
	return t
}

func releaseTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}
