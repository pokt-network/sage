package reputation

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

// Key bounds. The per-key ring (maxLen) bounds one key; these bound the number
// of keys, which is the dimension that actually grew: at per-supplier or
// per-endpoint granularity the key carries a supplier address, a staked
// registration that rotates every session, so the set of keys ever seen grows
// with the network for as long as the process lives. On the mainnet canary
// (2026-09-01, per-supplier, ~50 services) that was ~6k new keys an hour at
// ~6.5KB each — ~100MB/h, flat, until the 1Gi limit at 14.7h.
//
// Nothing in the scoring path reads the timeline; it is the admin API's
// debugging log. So the cost of forgetting a key is a shorter history for an
// endpoint nobody has relayed to lately, not a scoring change.
const (
	// DefaultTimelineIdleTTL is how long a key is kept after its last event.
	// A key in the current session receives probe events at least every
	// health-check interval, so a key idle this long has left the session.
	// Shared with the storage sweep — see DefaultIdleTTL.
	DefaultTimelineIdleTTL = DefaultIdleTTL
	// DefaultTimelineMaxKeys is the hard ceiling on distinct keys across all
	// shards, applied when the idle sweep alone is not enough. At the default
	// ring of 100 events (~104B each, capacity rounds up to 128) that is
	// ~13KB per key worst case, ~210MB for a full timeline.
	DefaultTimelineMaxKeys = 16_384
	// timelineSweepInterval is the least time between two idle sweeps, so a
	// burst of records does not scan every key on every insert.
	timelineSweepInterval = time.Minute
)

// TimelineConfig sets the per-key ring length and the key bounds. Zero values
// take the defaults.
type TimelineConfig struct {
	// MaxLen is the number of events kept per key.
	MaxLen int
	// IdleTTL drops a key whose last event is older than this.
	IdleTTL time.Duration
	// MaxKeys is the ceiling on distinct keys across the whole timeline.
	MaxKeys int
}

type timelineShard struct {
	mu     sync.RWMutex
	events map[string][]TimelineEvent // key = serviceID:endpoint
	// lastSeen is the wall-clock time of the last Record per key. Kept apart
	// from the event's own Timestamp, which is when the signal happened and
	// is supplied by the caller.
	lastSeen map[string]time.Time
}

// Timeline is a ring-buffer-backed per-endpoint event log for debugging.
type Timeline struct {
	shards  [timelineShards]timelineShard
	maxLen  int
	idleTTL time.Duration
	maxKeys int
	// lastSweep is the Unix time of the last idle sweep, across all shards.
	lastSweep atomic.Int64
	// now is the clock, replaceable in tests.
	now func() time.Time
}

// NewTimeline creates a timeline with the given maximum events per endpoint
// and the default key bounds.
func NewTimeline(maxLen int) *Timeline {
	return NewTimelineWithConfig(TimelineConfig{MaxLen: maxLen})
}

// NewTimelineWithConfig creates a timeline with explicit bounds.
func NewTimelineWithConfig(cfg TimelineConfig) *Timeline {
	if cfg.MaxLen <= 0 {
		cfg.MaxLen = 100
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = DefaultTimelineIdleTTL
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = DefaultTimelineMaxKeys
	}
	t := &Timeline{
		maxLen:  cfg.MaxLen,
		idleTTL: cfg.IdleTTL,
		maxKeys: cfg.MaxKeys,
		now:     time.Now,
	}
	for i := range t.shards {
		t.shards[i].events = make(map[string][]TimelineEvent)
		t.shards[i].lastSeen = make(map[string]time.Time)
	}
	return t
}

func (t *Timeline) shard(key string) *timelineShard {
	return &t.shards[fnv32a(key)%timelineShards]
}

// Record appends an event to the timeline for the given key.
// When the buffer is full, the oldest event is evicted.
func (t *Timeline) Record(key string, event TimelineEvent) {
	now := t.now()
	t.maybeSweep(now)

	s := t.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	buf, known := s.events[key]
	if !known {
		// Only a new key can grow the shard, so only a new key pays for the
		// cap check.
		t.capLocked(s)
	}
	if len(buf) >= t.maxLen {
		// Shift left by one to make room (ring buffer eviction).
		copy(buf, buf[1:])
		buf = buf[:len(buf)-1]
	}
	s.events[key] = append(buf, event)
	s.lastSeen[key] = now
}

// maybeSweep drops every key idle past idleTTL, at most once per
// timelineSweepInterval. The sweep covers all shards, not just the one being
// written: a key parks in whichever shard its hash lands in, and a shard that
// stops receiving new keys would otherwise keep its idle ones for ever. One
// scan of every key per minute is cheap; the CAS makes sure only one writer
// pays it.
func (t *Timeline) maybeSweep(now time.Time) {
	last := t.lastSweep.Load()
	if now.Unix()-last < int64(timelineSweepInterval/time.Second) {
		return
	}
	if !t.lastSweep.CompareAndSwap(last, now.Unix()) {
		return
	}
	cutoff := now.Add(-t.idleTTL)
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		for k, seen := range s.lastSeen {
			if seen.Before(cutoff) {
				delete(s.events, k)
				delete(s.lastSeen, k)
			}
		}
		s.mu.Unlock()
	}
}

// capLocked enforces the hard key ceiling on one shard, applied per shard —
// maxKeys/timelineShards — so no cross-shard lock is needed; the hash spreads
// keys evenly enough that the total stays within maxKeys. When the shard is
// full, the least recently recorded keys go until it is not.
//
// Must be called with the shard locked, before inserting a new key.
func (t *Timeline) capLocked(s *timelineShard) {
	perShard := t.maxKeys / timelineShards
	if perShard < 1 {
		perShard = 1
	}
	// +1: the caller is about to insert one more key.
	excess := len(s.lastSeen) + 1 - perShard
	if excess <= 0 {
		return
	}
	type keyAge struct {
		key  string
		seen time.Time
	}
	ages := make([]keyAge, 0, len(s.lastSeen))
	for k, seen := range s.lastSeen {
		ages = append(ages, keyAge{k, seen})
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i].seen.Before(ages[j].seen) })
	for _, a := range ages[:excess] {
		delete(s.events, a.key)
		delete(s.lastSeen, a.key)
	}
}

// Len returns the number of distinct keys currently held. Exported as
// sage_reputation_timeline_keys; a value climbing toward MaxKeys is the
// signature of a rotating key set.
func (t *Timeline) Len() int {
	n := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		n += len(s.events)
		s.mu.RUnlock()
	}
	return n
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
