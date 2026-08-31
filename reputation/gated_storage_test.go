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
