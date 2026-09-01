package reputation

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeline_RecordAndGet(t *testing.T) {
	tl := NewTimeline(5)
	key := "eth:ep1"

	for i := range 3 {
		tl.Record(key, TimelineEvent{
			Timestamp: time.Now(),
			Event:     "signal",
			Detail:    "test",
			Score:     float64(100 - i*10),
		})
	}

	events := tl.Get(key)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Score != 100 {
		t.Errorf("first event score = %f, want 100", events[0].Score)
	}
}

func TestTimeline_RingBufferOverflow(t *testing.T) {
	maxLen := 3
	tl := NewTimeline(maxLen)
	key := "eth:ep1"

	for i := range 5 {
		tl.Record(key, TimelineEvent{
			Timestamp: time.Now(),
			Event:     "signal",
			Score:     float64(i),
		})
	}

	events := tl.Get(key)
	if len(events) != maxLen {
		t.Fatalf("expected %d events, got %d", maxLen, len(events))
	}
	// Oldest events (0, 1) should have been evicted.
	if events[0].Score != 2 {
		t.Errorf("oldest event score = %f, want 2", events[0].Score)
	}
	if events[2].Score != 4 {
		t.Errorf("newest event score = %f, want 4", events[2].Score)
	}
}

func TestTimeline_GetEmpty(t *testing.T) {
	tl := NewTimeline(10)
	events := tl.Get("nonexistent")
	if events != nil {
		t.Errorf("expected nil for empty key, got %v", events)
	}
}

func TestTimeline_GetAll(t *testing.T) {
	tl := NewTimeline(10)

	tl.Record("eth:ep1", TimelineEvent{Event: "signal", Score: 90})
	tl.Record("eth:ep2", TimelineEvent{Event: "signal", Score: 80})
	tl.Record("poly:ep1", TimelineEvent{Event: "signal", Score: 70})

	ethEvents := tl.GetAll("eth:")
	if len(ethEvents) != 2 {
		t.Fatalf("expected 2 eth events, got %d", len(ethEvents))
	}

	allEvents := tl.GetAll("")
	if len(allEvents) != 3 {
		t.Fatalf("expected 3 total events, got %d", len(allEvents))
	}
}

func TestTimeline_DefaultMaxLen(t *testing.T) {
	tl := NewTimeline(0)
	if tl.maxLen != 100 {
		t.Errorf("expected default maxLen=100, got %d", tl.maxLen)
	}
}

func TestTimeline_GetReturnsCopy(t *testing.T) {
	tl := NewTimeline(10)
	key := "eth:ep1"
	tl.Record(key, TimelineEvent{Event: "signal", Score: 90})

	events := tl.Get(key)
	events[0].Score = 999 // Mutate the returned copy.

	original := tl.Get(key)
	if original[0].Score != 90 {
		t.Error("Get should return a copy, but original was mutated")
	}
}

// A key whose last event is older than the idle TTL is dropped the next time
// its shard admits a new key. This is the bound that keeps a rotating key set
// (per-supplier granularity: a fresh supplier address every session) from
// growing for the life of the process.
func TestTimeline_EvictsIdleKeys(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tl := NewTimelineWithConfig(TimelineConfig{MaxLen: 5, IdleTTL: time.Hour, MaxKeys: 100_000})
	tl.now = func() time.Time { return now }

	tl.Record("eth:old", TimelineEvent{Timestamp: now, Event: "signal"})
	now = now.Add(30 * time.Minute)
	tl.Record("eth:fresh", TimelineEvent{Timestamp: now, Event: "signal"})

	// Past the TTL for "old", not for "fresh"; a new key triggers the sweep.
	now = now.Add(45 * time.Minute)
	tl.Record("eth:newer", TimelineEvent{Timestamp: now, Event: "signal"})

	if got := tl.Get("eth:old"); got != nil {
		t.Errorf("idle key survived the sweep: %v", got)
	}
	if got := tl.Get("eth:fresh"); len(got) != 1 {
		t.Errorf("live key was evicted: got %d events", len(got))
	}
	if got := tl.Len(); got != 2 {
		t.Errorf("Len = %d, want 2", got)
	}
}

// Recording on a key refreshes its idle clock: a key that keeps receiving
// events is never idle, however old its first event is.
func TestTimeline_ActivityKeepsKeyAlive(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tl := NewTimelineWithConfig(TimelineConfig{MaxLen: 5, IdleTTL: time.Hour, MaxKeys: 100_000})
	tl.now = func() time.Time { return now }

	for i := 0; i < 5; i++ {
		tl.Record("eth:busy", TimelineEvent{Timestamp: now, Event: "signal"})
		now = now.Add(40 * time.Minute)
	}
	tl.Record("eth:other", TimelineEvent{Timestamp: now, Event: "signal"})

	if got := tl.Get("eth:busy"); len(got) != 5 {
		t.Errorf("busy key was evicted: got %d events", len(got))
	}
}

// When the idle sweep is not enough, the hard cap drops the keys with the
// oldest last event until the timeline is back under MaxKeys. Memory is then
// bounded by MaxKeys × MaxLen regardless of how fast keys rotate.
func TestTimeline_HardCapEvictsOldest(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	const maxKeys = 64
	tl := NewTimelineWithConfig(TimelineConfig{MaxLen: 5, IdleTTL: 24 * time.Hour, MaxKeys: maxKeys})
	tl.now = func() time.Time { return now }

	for i := 0; i < 4*maxKeys; i++ {
		now = now.Add(time.Second)
		tl.Record(fmt.Sprintf("eth:ep%d", i), TimelineEvent{Timestamp: now, Event: "signal"})
	}

	if got := tl.Len(); got > maxKeys {
		t.Fatalf("Len = %d, want <= %d", got, maxKeys)
	}
	// The newest key always survives; the very first is long gone.
	if got := tl.Get(fmt.Sprintf("eth:ep%d", 4*maxKeys-1)); len(got) != 1 {
		t.Errorf("newest key missing")
	}
	if got := tl.Get("eth:ep0"); got != nil {
		t.Errorf("oldest key survived the cap: %v", got)
	}
}

// Zero-value config falls back to the defaults, and NewTimeline(maxLen) keeps
// its old meaning with the default key bounds applied.
func TestTimeline_DefaultBounds(t *testing.T) {
	tl := NewTimeline(0)
	if tl.maxLen != 100 || tl.idleTTL != DefaultTimelineIdleTTL || tl.maxKeys != DefaultTimelineMaxKeys {
		t.Errorf("defaults not applied: maxLen=%d idleTTL=%v maxKeys=%d", tl.maxLen, tl.idleTTL, tl.maxKeys)
	}
}
