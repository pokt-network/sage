package reputation

import (
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
