package reputation

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// TimelineEvent represents a single event in an endpoint's reputation history.
//
// Signal events store structured fields (SignalType, Reason, OldScore, Score);
// Detail is rendered from them on the read path (admin API) so the relay hot
// path never pays for fmt formatting.
type TimelineEvent struct {
	Timestamp  time.Time
	Event      string  // "signal", "cooldown_start", "cooldown_end", "circuit_break", "recovery"
	SignalType string  // e.g. "critical_error"; empty for non-signal events
	Reason     string  // e.g. "http_5xx"
	OldScore   float64 // score before the event
	Score      float64 // score after the event
	Detail     string  // human-readable; pre-set by callers or rendered on read
}

// rendered returns a copy with Detail filled in from the structured fields.
func (e TimelineEvent) rendered() TimelineEvent {
	if e.Detail == "" && e.SignalType != "" {
		e.Detail = fmt.Sprintf("%s: %s (score: %.1f -> %.1f)", e.SignalType, e.Reason, e.OldScore, e.Score)
	}
	return e
}

// timelineShards stripes the per-endpoint event log so concurrent relays
// recording signals for different endpoints don't serialize on one mutex.
const timelineShards = 32

type timelineShard struct {
	mu     sync.RWMutex
	events map[string][]TimelineEvent // key = serviceID:endpoint
}

// Timeline is a ring-buffer-backed per-endpoint event log for debugging.
type Timeline struct {
	shards [timelineShards]timelineShard
	maxLen int
}

// NewTimeline creates a timeline with the given maximum events per endpoint.
func NewTimeline(maxLen int) *Timeline {
	if maxLen <= 0 {
		maxLen = 100
	}
	t := &Timeline{maxLen: maxLen}
	for i := range t.shards {
		t.shards[i].events = make(map[string][]TimelineEvent)
	}
	return t
}

func (t *Timeline) shard(key string) *timelineShard {
	return &t.shards[fnv32a(key)%timelineShards]
}

// Record appends an event to the timeline for the given key.
// When the buffer is full, the oldest event is evicted.
func (t *Timeline) Record(key string, event TimelineEvent) {
	s := t.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := s.events[key]
	if len(buf) >= t.maxLen {
		// Shift left by one to make room (ring buffer eviction).
		copy(buf, buf[1:])
		buf = buf[:len(buf)-1]
	}
	s.events[key] = append(buf, event)
}

// Get returns all events for the given key, oldest first, with Detail rendered.
func (t *Timeline) Get(key string) []TimelineEvent {
	s := t.shard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	buf := s.events[key]
	if len(buf) == 0 {
		return nil
	}
	out := make([]TimelineEvent, len(buf))
	for i, e := range buf {
		out[i] = e.rendered()
	}
	return out
}

// GetAll returns all events whose key starts with the given prefix, oldest
// first per endpoint, with Detail rendered.
func (t *Timeline) GetAll(prefix string) []TimelineEvent {
	var out []TimelineEvent
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		for k, buf := range s.events {
			if strings.HasPrefix(k, prefix) {
				for _, e := range buf {
					out = append(out, e.rendered())
				}
			}
		}
		s.mu.RUnlock()
	}
	return out
}

// fnv32a is an inline FNV-1a hash for shard selection (avoids hash/fnv allocs).
func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
