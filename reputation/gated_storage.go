package reputation

import "context"

// LeaderOnlyStorage writes through to inner only while isLeader reports
// true, and reads through always.
//
// The write-behind store is external state, not something the gateway reads
// back, and with several replicas each writing its own view of the same key
// it was last-writer-wins — nobody's view. Gating writes on the leader makes
// it one replica's coherent view: that replica's traffic plus the fleet's
// probes, which every replica applies. Dropped writes are not errors: the
// follower's in-memory state is untouched.
type LeaderOnlyStorage struct {
	inner    Storage
	isLeader func() bool
}

// NewLeaderOnlyStorage wraps inner. A nil isLeader always writes.
func NewLeaderOnlyStorage(inner Storage, isLeader func() bool) *LeaderOnlyStorage {
	return &LeaderOnlyStorage{inner: inner, isLeader: isLeader}
}

// GetState reads through.
func (s *LeaderOnlyStorage) GetState(ctx context.Context, key string) (State, error) {
	return s.inner.GetState(ctx, key)
}

// SetState writes through on the leader and drops the write elsewhere.
func (s *LeaderOnlyStorage) SetState(ctx context.Context, key string, st State) error {
	if s.isLeader != nil && !s.isLeader() {
		return nil
	}
	return s.inner.SetState(ctx, key, st)
}

// GetStates reads through.
func (s *LeaderOnlyStorage) GetStates(ctx context.Context, prefix string) (map[string]State, error) {
	return s.inner.GetStates(ctx, prefix)
}

// DeleteState deletes through on the leader and drops the delete elsewhere.
func (s *LeaderOnlyStorage) DeleteState(ctx context.Context, key string) error {
	if s.isLeader != nil && !s.isLeader() {
		return nil
	}
	return s.inner.DeleteState(ctx, key)
}

var _ Storage = (*LeaderOnlyStorage)(nil)
