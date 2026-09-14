package relay

import (
	"testing"
	"time"
)

// Exclusive time is a stage's own wall time minus the time it spent inside
// the rest of the chain: an outer stage that sleeps 5 ms and then calls an
// inner stage that sleeps 10 ms owns 5 ms, not 15.
func TestStageTimes_ExclusivePerStage(t *testing.T) {
	outer := Middleware(func(next Handler) Handler {
		return HandlerFunc(func(ctx *Context) error {
			time.Sleep(5 * time.Millisecond)
			return next.HandleRelay(ctx)
		})
	})
	inner := Middleware(func(next Handler) Handler {
		return HandlerFunc(func(ctx *Context) error {
			time.Sleep(10 * time.Millisecond)
			return next.HandleRelay(ctx)
		})
	})
	terminal := HandlerFunc(func(*Context) error { return nil })
	chain := Chain(terminal, timed("outer", outer), timed("inner", inner))

	ctx := &Context{Stages: NewStageTimes()}
	if err := chain.HandleRelay(ctx); err != nil {
		t.Fatal(err)
	}
	got := ctx.Stages.Exclusive()
	if got["outer"] < 5*time.Millisecond || got["outer"] > 12*time.Millisecond {
		t.Errorf("outer exclusive = %s, want about 5ms", got["outer"])
	}
	if got["inner"] < 10*time.Millisecond || got["inner"] > 20*time.Millisecond {
		t.Errorf("inner exclusive = %s, want about 10ms", got["inner"])
	}

	// A stage that calls next twice (retry) sums both calls' inner time.
	twice := Middleware(func(next Handler) Handler {
		return HandlerFunc(func(ctx *Context) error {
			_ = next.HandleRelay(ctx)
			return next.HandleRelay(ctx)
		})
	})
	chain = Chain(terminal, timed("twice", twice), timed("inner", inner))
	ctx = &Context{Stages: NewStageTimes()}
	_ = chain.HandleRelay(ctx)
	got = ctx.Stages.Exclusive()
	if got["twice"] > 6*time.Millisecond {
		t.Errorf("twice exclusive = %s, want near zero: its time is all inner", got["twice"])
	}
	if got["inner"] < 20*time.Millisecond {
		t.Errorf("inner exclusive = %s, want both calls summed", got["inner"])
	}

	// No accumulator: nothing recorded, nothing panics.
	if err := chain.HandleRelay(&Context{}); err != nil {
		t.Fatal(err)
	}
	if (*StageTimes)(nil).Exclusive() != nil {
		t.Fatal("nil accumulator should read as nil")
	}
}
