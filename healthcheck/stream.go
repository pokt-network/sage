package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// probeStreamKey is the Redis Stream every replica's probe results go
	// through. One stream, not one per service: a follower reads it once.
	probeStreamKey = "sage:probes"
	// probeStreamMaxLen bounds the stream (MAXLEN ~). On beta the fleet
	// produces ~30 results a minute per service; 10k is hours of history,
	// far more than the two-cycle replay a booting replica asks for.
	probeStreamMaxLen = 10000
	// probeStreamField is the one field each entry carries: the JSON result.
	probeStreamField = "result"
	// probeReadBlock is how long one XREAD waits for new entries before
	// returning to check the context.
	probeReadBlock = 5 * time.Second
	// probeReadCount caps one XREAD batch.
	probeReadCount = 256
)

// StreamRedisClient is the subset of redis.Cmdable the probe stream uses.
type StreamRedisClient interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd
	XRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd
}

var _ StreamRedisClient = (*redis.Client)(nil)

// RedisProbeStream publishes probe results to, and reads them from, a Redis
// Stream. It is both ProbeSink and ProbeSource: the leader publishes,
// everyone reads (the leader reads its own entries back too and applies
// them a second time? No — see Run: entries this instance wrote are
// skipped by instance id).
type RedisProbeStream struct {
	client     StreamRedisClient
	instanceID string
	// replay is how far back a fresh reader looks before blocking on new
	// entries, so a replica that just booted is not blind for a cycle.
	replay time.Duration
}

// NewRedisProbeStream returns a stream over client. replay is the window of
// recent history a new reader applies first; two health-check intervals is
// the sensible value.
func NewRedisProbeStream(client StreamRedisClient, instanceID string, replay time.Duration) *RedisProbeStream {
	return &RedisProbeStream{client: client, instanceID: instanceID, replay: replay}
}

// streamEntry is the on-wire envelope: the result plus who produced it, so
// a reader can skip its own.
type streamEntry struct {
	Producer string      `json:"producer"`
	Result   ProbeResult `json:"result"`
}

// Publish appends one result. XADD with MAXLEN ~: Redis trims in whole
// nodes, which is cheaper than an exact cap and bounded all the same.
func (s *RedisProbeStream) Publish(ctx context.Context, r ProbeResult) error {
	b, err := json.Marshal(streamEntry{Producer: s.instanceID, Result: r})
	if err != nil {
		return fmt.Errorf("encode probe result: %w", err)
	}
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: probeStreamKey,
		MaxLen: probeStreamMaxLen,
		Approx: true,
		Values: map[string]interface{}{probeStreamField: b},
	}).Err()
}

// Run replays the recent window, then blocks on new entries until ctx is
// done. Entries this instance published are skipped: it applied them when
// it probed. Malformed entries are skipped, never fatal — one bad entry must
// not cost the stream.
func (s *RedisProbeStream) Run(ctx context.Context, apply func(ProbeResult)) error {
	lastID := s.replayID()
	// Replay: everything from the window start to now.
	msgs, err := s.client.XRange(ctx, probeStreamKey, lastID, "+").Result()
	if err != nil {
		return fmt.Errorf("probe stream replay: %w", err)
	}
	for _, m := range msgs {
		s.applyMessage(m, apply)
		lastID = m.ID
	}
	if len(msgs) == 0 {
		// Nothing in the window: start from now, not from the window start,
		// or the same empty range is read again on the first XREAD.
		lastID = "$"
	}

	for ctx.Err() == nil {
		streams, err := s.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{probeStreamKey, lastID},
			Count:   probeReadCount,
			Block:   probeReadBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // Block timed out with nothing new.
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("probe stream read: %w", err)
		}
		for _, st := range streams {
			for _, m := range st.Messages {
				s.applyMessage(m, apply)
				lastID = m.ID
			}
		}
	}
	return ctx.Err()
}

// replayID is the stream id at the start of the replay window: ids are
// "<unix ms>-<seq>", so a timestamp is a valid lower bound.
func (s *RedisProbeStream) replayID() string {
	if s.replay <= 0 {
		return "$"
	}
	return strconv.FormatInt(time.Now().Add(-s.replay).UnixMilli(), 10) + "-0"
}

func (s *RedisProbeStream) applyMessage(m redis.XMessage, apply func(ProbeResult)) {
	raw, ok := m.Values[probeStreamField].(string)
	if !ok {
		return
	}
	var e streamEntry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return
	}
	if e.Producer == s.instanceID {
		return
	}
	apply(e.Result)
}

var (
	_ ProbeSink   = (*RedisProbeStream)(nil)
	_ ProbeSource = (*RedisProbeStream)(nil)
)
