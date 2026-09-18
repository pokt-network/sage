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

// BatchWriter is the optional half of Storage that writes many states in one
// round trip.
//
// Without it the write-behind is one round trip per write on one goroutine, so
// its ceiling is 1/RTT — about 2,650 writes a second against a Redis 0.38ms
// away. Mainnet asked for 3,119 on 2026-09-18 and lost 472 a second to a full
// queue for as long as the traffic lasted, which no queue size fixes: the
// deficit is a rate. A batch of K makes the ceiling K/RTT and moves the limit
// off the drain entirely.
//
// A backend that cannot batch simply does not implement it, and the service
// falls back to SetState per key.
type BatchWriter interface {
	// SetStates writes every state in one operation. The map is the caller's
	// and must not be retained. An error means none of them landed, which is
	// how the caller counts the loss.
	SetStates(ctx context.Context, states map[string]State) error
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
