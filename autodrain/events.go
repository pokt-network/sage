package autodrain

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/domain"
)

// maxEvents bounds the decision log, in memory and in Redis alike.
const maxEvents = 1000

// EventLog keeps the engine's decisions where a person, the ops agent or the
// admin UI can read them back: GET /admin/auto-drain/events.
type EventLog interface {
	Append(ctx context.Context, ev Event) error
	// Recent returns up to limit events, newest first; an empty svc means
	// every service.
	Recent(ctx context.Context, svc domain.ServiceID, limit int) ([]Event, error)
}

// MemoryLog is the EventLog without Redis: this process's last maxEvents.
type MemoryLog struct {
	mu     sync.Mutex
	events []Event
}

// Append records ev, dropping the oldest past maxEvents.
func (m *MemoryLog) Append(_ context.Context, ev Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ev)
	if len(m.events) > maxEvents {
		m.events = m.events[len(m.events)-maxEvents:]
	}
	return nil
}

// Recent returns newest first.
func (m *MemoryLog) Recent(_ context.Context, svc domain.ServiceID, limit int) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	for i := len(m.events) - 1; i >= 0 && len(out) < limit; i-- {
		if svc == "" || m.events[i].ServiceID == svc {
			out = append(out, m.events[i])
		}
	}
	return out, nil
}

// StreamKey is the Redis stream the decisions are appended to.
const StreamKey = "sage:auto_drain:events"

// RedisClient is the subset of redis.Cmdable the Redis log uses.
type RedisClient interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XRevRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd
}

var _ RedisClient = (*redis.Client)(nil)

// RedisLog appends decisions to a capped Redis stream, so they survive a
// restart or a leader change and read the same from every replica.
type RedisLog struct{ Client RedisClient }

// Append adds ev to the stream, trimmed to about maxEvents entries.
func (r RedisLog) Append(ctx context.Context, ev Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return r.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey,
		MaxLen: maxEvents,
		Approx: true,
		Values: map[string]any{"event": string(b)},
	}).Err()
}

// Recent reads newest first. A service filter scans the whole capped stream,
// which is maxEvents entries at most.
func (r RedisLog) Recent(ctx context.Context, svc domain.ServiceID, limit int) ([]Event, error) {
	msgs, err := r.Client.XRevRangeN(ctx, StreamKey, "+", "-", maxEvents).Result()
	if err != nil {
		return nil, err
	}
	var out []Event
	for _, m := range msgs {
		raw, _ := m.Values["event"].(string)
		var ev Event
		if json.Unmarshal([]byte(raw), &ev) != nil {
			continue
		}
		if svc == "" || ev.ServiceID == svc {
			out = append(out, ev)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}
