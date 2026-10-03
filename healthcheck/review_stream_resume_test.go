package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pokt-network/sage/domain"
)

func reviewEntry(t *testing.T, at time.Time, seq int, svc domain.ServiceID) redis.XMessage {
	t.Helper()
	raw, err := json.Marshal(streamEntry{Producer: "leader-1", Result: ProbeResult{ServiceID: svc, Check: "block_number"}})
	if err != nil {
		t.Fatal(err)
	}
	return redis.XMessage{ID: fmt.Sprintf("%d-%d", at.UnixMilli(), seq), Values: map[string]any{probeStreamField: string(raw)}}
}

// runFeedOnce is one turn of Executor.runFeed: Run under a recover (runFeed
// wraps it in safego.Run), bounded by a short context.
func runFeedOnce(s *RedisProbeStream, apply func(ProbeResult)) {
	defer func() { _ = recover() }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx, apply)
}

// Run's contract (stream.go): "one bad entry must not cost the stream". An
// entry whose apply panics is recovered by runFeed and Run restarts; resume
// was not advanced past it (it is assigned after applyMessage), so every
// restart XRANGEs "(resume" and hits the same entry first. Before resume, the
// restart read from the replay window, and the entry aged out of it.
func TestReview_StreamPoisonEntryDoesNotStallFeedForever(t *testing.T) {
	now := time.Now()
	f := &audit2Stream{entries: []redis.XMessage{
		reviewEntry(t, now, 0, "a"),
		reviewEntry(t, now, 1, "poison"),
	}}
	s := NewRedisProbeStream(f, "follower-1", 100*time.Millisecond, "")
	var applied []domain.ServiceID
	apply := func(r ProbeResult) {
		if r.ServiceID == "poison" {
			panic("apply bug")
		}
		applied = append(applied, r.ServiceID)
	}

	runFeedOnce(s, apply)
	if !slices.Contains(applied, "a") {
		t.Fatalf("precondition: first Run applied %v, want a", applied)
	}

	// Well past the replay window, a fresh result arrives.
	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	f.entries = append(f.entries, reviewEntry(t, time.Now(), 0, "b"))
	f.mu.Unlock()

	for range 5 { // runFeed restarts once a second, forever
		runFeedOnce(s, apply)
	}
	if !slices.Contains(applied, "b") {
		t.Fatalf("a fresh result was never applied across 5 feed restarts (applied %v): every restart replays the poison entry first", applied)
	}
}

// Once resume is set, Run ignores the replay window: after a disconnect
// longer than the window, XRANGE "(resume" returns every entry since (up to
// the stream's MAXLEN) and the own-cluster feed applies them, scoring
// suppliers on results older than the window a booting replica is limited to.
// The peer feed got an age gate (peerAgeLimit) for exactly this; this one
// did not.
func TestReview_StreamResumeHonoursReplayWindow(t *testing.T) {
	now := time.Now()
	f := &audit2Stream{entries: []redis.XMessage{reviewEntry(t, now, 0, "fresh")}, xreadErrs: 1}
	window := 100 * time.Millisecond
	s := NewRedisProbeStream(f, "follower-1", window, "")
	var applied []domain.ServiceID
	apply := func(r ProbeResult) { applied = append(applied, r.ServiceID) }

	// The connection drops on the first XREAD.
	if err := s.Run(context.Background(), apply); err == nil {
		t.Fatal("precondition: the first Run must end on the XRead error")
	}
	// Published just after the drop; the outage lasts three windows.
	f.mu.Lock()
	f.entries = append(f.entries, reviewEntry(t, now, 1, "during-outage"))
	f.mu.Unlock()
	time.Sleep(3 * window)

	runFeedOnce(s, apply)
	if slices.Contains(applied, "during-outage") {
		t.Fatalf("a result %v old was applied on restart; the replay window is %v (applied %v)",
			time.Since(now).Round(time.Millisecond), window, applied)
	}
}
