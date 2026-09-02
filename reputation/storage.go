package reputation

import (
	"context"
	"time"
)

// Storage defines the persistence layer for reputation state.
//
// It is write-behind on the hot path: the in-memory cache is the whole read
// path, and a miss there answers InitialScore rather than consulting Storage.
// Storage is read exactly once, at startup, by Hydrate — which is what makes a
// restarted or rolled pod inherit the fleet's scores instead of re-learning
// them from probes. So a write here does survive a restart and is visible to
// the next pod, but only through that one read: nothing consults Storage again
// while the process runs, and a score that changes in the store mid-life
// reaches nobody.
type Storage interface {
	// GetState retrieves the state for the given key. Returns
	// ErrStateNotFound if the key does not exist.
	GetState(ctx context.Context, key string) (State, error)
	// SetState stores the state for the given key.
	SetState(ctx context.Context, key string, st State) error
	// GetStates retrieves all states whose keys begin with the given prefix.
	GetStates(ctx context.Context, prefix string) (map[string]State, error)
	// DeleteState removes the state for the given key.
	DeleteState(ctx context.Context, key string) error
}

// StaleDeleter is the optional half of Storage that bounds it. Storage is
// write-behind that nothing reads back, so without it the backing store holds
// one entry per key ever scored — at per-supplier granularity, every staked
// registration the network has ever put in a session. The service calls it
// from the write-behind goroutine on a fixed cadence with now-DefaultIdleTTL
// (or ServiceConfig.StateIdleTTL); a backend that cannot expire entries on its
// own implements it, one that can may ignore it.
type StaleDeleter interface {
	// DeleteStale removes every entry whose UpdatedAt is before olderThan —
	// including entries with no UpdatedAt at all, which predate the stamp
	// and are by definition older than any cutoff. Returns how many went.
	DeleteStale(ctx context.Context, olderThan time.Time) (int, error)
}
