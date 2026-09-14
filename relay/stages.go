package relay

import (
	"sync"
	"time"
)

// StageTimes accumulates, per middleware name, the time a client request spent
// in that stage exclusive of the stages nested inside it. One per client
// request, allocated by NewContext and shared by the request's clones (hedge
// arms, batch items) under its own lock, so it is safe for the shallow Clone.
//
// It exists for one question the per-attempt relay latency cannot answer:
// where does a request spend the time that is not the upstream call. On the
// 2026-09-13 canary SAGE's client p50 on osmosis was 0.170 s against an
// upstream-attempt p50 of 0.098 s, and PATH's two numbers were 10 ms apart.
// The exported sum per stage, divided by client requests, is that split.
//
// Exclusive time is the stage's own wall time minus the wall time of every
// call it made into the rest of the chain. A stage that calls next several
// times (retry) sums them; hedge arms run in parallel, so their inner time
// can exceed the stage's own and the exclusive value clamps at zero for the
// stage that raced them.
type StageTimes struct {
	mu    sync.Mutex
	total map[string]time.Duration
	inner map[string]time.Duration
}

// NewStageTimes returns an empty accumulator.
func NewStageTimes() *StageTimes {
	return &StageTimes{total: make(map[string]time.Duration, 32), inner: make(map[string]time.Duration, 32)}
}

func (s *StageTimes) addTotal(name string, d time.Duration) {
	s.mu.Lock()
	s.total[name] += d
	s.mu.Unlock()
}

func (s *StageTimes) addInner(name string, d time.Duration) {
	s.mu.Lock()
	s.inner[name] += d
	s.mu.Unlock()
}

// Add records time under a name outside the chain (the router's own write).
func (s *StageTimes) Add(name string, d time.Duration) {
	if s == nil {
		return
	}
	s.addTotal(name, d)
}

// Exclusive returns each stage's own time, a copy.
func (s *StageTimes) Exclusive() map[string]time.Duration {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]time.Duration, len(s.total))
	for name, t := range s.total {
		d := t - s.inner[name]
		if d < 0 {
			d = 0
		}
		out[name] = d
	}
	return out
}

// timed wraps a middleware so the chain records its exclusive time under
// name. A context without a StageTimes (tests that build one by hand) pays
// nothing.
func timed(name string, mw Middleware) Middleware {
	return func(next Handler) Handler {
		nextTimed := HandlerFunc(func(ctx *Context) error {
			if ctx.Stages == nil {
				return next.HandleRelay(ctx)
			}
			start := time.Now()
			err := next.HandleRelay(ctx)
			ctx.Stages.addInner(name, time.Since(start))
			return err
		})
		h := mw(nextTimed)
		return HandlerFunc(func(ctx *Context) error {
			if ctx.Stages == nil {
				return h.HandleRelay(ctx)
			}
			start := time.Now()
			err := h.HandleRelay(ctx)
			ctx.Stages.addTotal(name, time.Since(start))
			return err
		})
	}
}
