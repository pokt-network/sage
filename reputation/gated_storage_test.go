package reputation

import (
	"context"
	"testing"
)

func TestLeaderOnlyStorage_DropsFollowerWrites(t *testing.T) {
	inner := NewMemoryStorage()
	leader := false
	s := NewLeaderOnlyStorage(inner, func() bool { return leader })

	if err := s.SetState(context.Background(), "k", State{Score: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := inner.GetState(context.Background(), "k"); err == nil {
		t.Fatal("a follower's write must not reach the store")
	}
	leader = true
	if err := s.SetState(context.Background(), "k", State{Score: 2}); err != nil {
		t.Fatal(err)
	}
	if st, err := inner.GetState(context.Background(), "k"); err != nil || st.Score != 2 {
		t.Fatalf("leader's write missing: %+v %v", st, err)
	}
}

// An operator's reset is about the fleet's view, not this replica's: on a
// follower it must still reach storage, where the leader and every other
// replica will read it, instead of being dropped with the follower's
// ordinary traffic writes.
func TestResetScore_WritesThroughOnAFollower(t *testing.T) {
	inner := NewMemoryStorage()
	gated := NewLeaderOnlyStorage(inner, func() bool { return false })
	svc := NewService(gated, NewTimeline(100), DefaultServiceConfig())
	svc.Start()

	ctx := context.Background()
	if err := svc.ResetScore(ctx, "eth", "ep1"); err != nil {
		t.Fatal(err)
	}
	svc.Stop() // drains the write queue
	states, err := inner.GetStates(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(states) == 0 {
		t.Fatal("the reset never reached storage: a follower dropped it, so the leader's next signal restores the old score")
	}
}
