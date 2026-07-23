package websockets

import "sync/atomic"

// ConnectionLimiter bounds how many WebSocket connections a gateway holds open
// at once.
//
// A WebSocket connection is nothing like an HTTP request: it is long-lived, and
// each one costs several goroutines, two TCP sockets, and read/write buffers.
// Nothing else counts them. Rate-limiting upgrade *attempts* at the edge does
// not help, because the cost here is in connections that never leave — a slow
// drain of clients that connect and simply stay grows goroutines and file
// descriptors without bound until the process dies.
//
// A nil *ConnectionLimiter means no limit: every method is nil-safe and Acquire
// always succeeds. That keeps the limiter optional without making every caller
// and test construct one.
type ConnectionLimiter struct {
	max    int64
	active atomic.Int64
}

// NewConnectionLimiter returns a limiter capping concurrent connections at max.
// A max <= 0 returns nil, which disables limiting.
func NewConnectionLimiter(max int) *ConnectionLimiter {
	if max <= 0 {
		return nil
	}
	return &ConnectionLimiter{max: int64(max)}
}

// Acquire reserves a slot. It returns true when one was reserved — the caller
// must then Release exactly once — and false when the limiter is already full,
// in which case the caller must reject the connection and must NOT Release.
//
// A nil limiter always returns true.
func (l *ConnectionLimiter) Acquire() bool {
	if l == nil {
		return true
	}
	// CAS loop rather than add-then-rollback: only increment when strictly below
	// the cap, so the counter never overshoots even for an instant. An optimistic
	// add would let a concurrent Active() read report more than max, which makes
	// the number untrustworthy exactly when it is being looked at.
	for {
		cur := l.active.Load()
		if cur >= l.max {
			return false
		}
		if l.active.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// Release frees a slot from a successful Acquire. It must be called exactly
// once per successful Acquire. A nil limiter is a no-op.
func (l *ConnectionLimiter) Release() {
	if l == nil {
		return
	}
	l.active.Add(-1)
}

// Active returns how many slots are currently held. A nil limiter reports 0.
func (l *ConnectionLimiter) Active() int64 {
	if l == nil {
		return 0
	}
	return l.active.Load()
}
