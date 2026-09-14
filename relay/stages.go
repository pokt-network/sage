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
// The exported sum per stage, divided by client requests, is that split; the
// first read of it found a flat ~1 ms of Redis per flag-gated stage.
//
// Exclusive time is the stage's own wall time minus the wall time of every
// call it made into the rest of the chain. A stage that calls next several
// times (retry) sums them. A stage that races calls on other goroutines
// (hedge) may return while a call is still running; the running call's time
// so far is subtracted at that moment, so the stage is not charged for an
// arm it left behind, and the arm's remainder, which falls outside the
// stage's window, is not charged to it either. Overlapping arms can still
// push the subtraction past the stage's own time; exclusive clamps at zero.
type StageTimes struct {
	mu      sync.Mutex
	total   map[string]time.Duration
	inner   map[string]time.Duration
	running map[string][]*inflight
}

// inflight is one call into the rest of the chain that has not returned.
type inflight struct {
	start    time.Time
	credited time.Duration // already subtracted from the parent's window
}

// NewStageTimes returns an empty accumulator.
func NewStageTimes() *StageTimes {
	return &StageTimes{
		total:   make(map[string]time.Duration, 32),
		inner:   make(map[string]time.Duration, 32),
		running: make(map[string][]*inflight, 4),
	}
}

// addTotal records a stage's own wall time and settles its running calls:
// whatever they have consumed so far is inner time inside this window.
func (s *StageTimes) addTotal(name string, d time.Duration) {
	now := time.Now()
	s.mu.Lock()
	s.total[name] += d
	for _, f := range s.running[name] {
		if extra := now.Sub(f.start) - f.credited; extra > 0 {
			s.inner[name] += extra
			f.credited += extra
		}
	}
	s.mu.Unlock()
}

func (s *StageTimes) beginInner(name string) *inflight {
	f := &inflight{start: time.Now()}
	s.mu.Lock()
	s.running[name] = append(s.running[name], f)
	s.mu.Unlock()
	return f
}

func (s *StageTimes) endInner(name string, f *inflight) {
	elapsed := time.Since(f.start)
	s.mu.Lock()
	if rest := elapsed - f.credited; rest > 0 {
		s.inner[name] += rest
	}
	list := s.running[name]
	for i, g := range list {
		if g == f {
			list[i] = list[len(list)-1]
			s.running[name] = list[:len(list)-1]
			break
		}
	}
	s.mu.Unlock()
}

// Add records time under a name outside the chain (the router's own write,
// the phases inside send_relay).
func (s *StageTimes) Add(name string, d time.Duration) {
	if s == nil || d <= 0 {
		return
	}
	s.mu.Lock()
	s.total[name] += d
	s.mu.Unlock()
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
			f := ctx.Stages.beginInner(name)
			err := next.HandleRelay(ctx)
			ctx.Stages.endInner(name, f)
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
