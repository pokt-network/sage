package healthcheck

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	// stream names the Redis Stream results go to; NewRedisProbeStream
	// defaults it to probeStreamKey.
	stream string
	// resume is the id of the last entry Run applied, where the next Run
	// picks up. Run is never called concurrently (the feed restarts it in
	// one loop), so it needs no lock.
	resume     string
	client     StreamRedisClient
	instanceID string
	// replay is how far back a fresh reader looks before blocking on new
	// entries, so a replica that just booted is not blind for a cycle.
	replay time.Duration
}

// NewRedisProbeStream returns a stream over client. replay is the window of
// recent history a new reader applies first; two health-check intervals is
// the sensible value. An empty stream name means probeStreamKey, the literal
// every release before the Redis key prefix was configurable used.
func NewRedisProbeStream(client StreamRedisClient, instanceID string, replay time.Duration, stream string) *RedisProbeStream {
	return &RedisProbeStream{client: client, instanceID: instanceID, replay: replay, stream: cmp.Or(stream, probeStreamKey)}
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
		Stream: s.stream,
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
	// Replay: everything from the window start to now, or, on a restart,
	// everything after the last entry applied. The feed restarts Run after
	// any read error, and a full replay each time applied every result in
	// the window once per restart: a flapping connection multiplied every
	// signal in it.
	//
	// The resume never reaches back past the window: after an outage longer
	// than it, everything since the last entry applied is older than any
	// result worth applying. And it moves before an entry is applied, so an
	// entry whose apply panics is not the first one read again on every
	// restart, stalling the feed for good.
	lastID := s.replayID()
	if s.resume != "" && !streamIDBefore(s.resume, lastID) {
		lastID = "(" + s.resume
	}
	msgs, err := s.client.XRange(ctx, s.stream, lastID, "+").Result()
	if err != nil {
		return fmt.Errorf("probe stream replay: %w", err)
	}
	for _, m := range msgs {
		lastID, s.resume = m.ID, m.ID
		s.applyMessage(m, apply)
	}
	if len(msgs) == 0 {
		// Nothing new: read on from the last entry applied, or from now,
		// not from the window start, or the same empty range is read again
		// on the first XREAD.
		lastID = cmp.Or(s.resume, "$")
	}

	for ctx.Err() == nil {
		streams, err := s.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{s.stream, lastID},
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
				lastID, s.resume = m.ID, m.ID
				s.applyMessage(m, apply)
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

// streamIDBefore reports whether stream id a ("<ms>-<seq>") sorts before b.
// An id that does not parse (b is "$" when there is no window) is never
// before anything.
func streamIDBefore(a, b string) bool {
	am, as, okA := parseStreamID(a)
	bm, bs, okB := parseStreamID(b)
	return okA && okB && (am < bm || (am == bm && as < bs))
}

func parseStreamID(id string) (ms, seq uint64, ok bool) {
	m, sq, found := strings.Cut(id, "-")
	if !found {
		return 0, 0, false
	}
	ms, err1 := strconv.ParseUint(m, 10, 64)
	seq, err2 := strconv.ParseUint(sq, 10, 64)
	return ms, seq, err1 == nil && err2 == nil
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
