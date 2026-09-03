package healthcheck

import (
	"math"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/reputation"
)

// DefaultMinTrafficSignals is the traffic a backend must have carried in one
// window before a probe against it is considered redundant.
//
// The number is derived, not chosen. A probe is the only observation source
// that bypasses sampling (observe.Queue.Submit exempts SourceHealthCheck
// alone), so a client relay reaches the block-consensus and QoS state at the
// configured sample_rate while a probe reaches it every time. Replacing a
// probe with traffic is therefore only free when that traffic produces at
// least as many observations as the probe would have: one.
//
// At the default 10% sample rate, and the 2-2.7 reputation signals per relay
// measured on the mainnet canary on 2026-09-02, this works out at 10 relays'
// worth — one sampled observation, the same one the probe would have made.
// MinTrafficSignalsFor does the arithmetic against the pipeline's real rate,
// because a deployment that samples at 1% needs ten times the traffic to say
// the same thing.
const DefaultMinTrafficSignals = 20

// TrafficSkipConfig configures traffic-informed probing.
type TrafficSkipConfig struct {
	// MinSignals is how many client-traffic signals a backend must have
	// recorded within one cycle for its probe to be skipped. Zero means
	// MinTrafficSignalsFor(sampleRate). There is no "any traffic" setting on
	// purpose: see DefaultMinTrafficSignals.
	MinSignals uint64
	// SampleRate is the observation pipeline's sample_rate, used to derive
	// MinSignals when it is unset. Zero or negative means the pipeline samples
	// everything.
	SampleRate float64
}

// signalsPerRelay is how many reputation signals one client relay records, as
// measured on the mainnet canary on 2026-09-02 (2-2.7, taken at the low end).
// Taking it low makes the derived threshold higher, which errs towards
// probing: assuming few signals per relay means assuming a given signal count
// represents more relays than it does would be the dangerous direction, so we
// assume it represents fewer.
const signalsPerRelay = 2

// MinTrafficSignalsFor derives the traffic threshold from an observation
// sample rate: enough client relays that the sampler forwards at least one
// observation, expressed in the signal units TrafficCounter reports.
//
// A deployment sampling at 1% needs ten times the traffic of one sampling at
// 10% before its traffic says as much as a probe, and that is exactly what
// this multiplies out. An unsampled pipeline still needs one relay's worth.
func MinTrafficSignalsFor(sampleRate float64) uint64 {
	if sampleRate <= 0 || sampleRate >= 1 {
		return signalsPerRelay
	}
	relays := math.Ceil(1 / sampleRate)
	return uint64(relays) * signalsPerRelay
}

// trafficSkipper decides which probes client traffic has already paid for.
//
// It holds one reading per (service, backend, RPC type) — a cumulative signal
// count and when it was taken — and skips a check whose backend has gained
// MinSignals or more over a window at least as long as the check's own
// interval. Readings are carried forward until that window elapses, then
// refreshed.
//
// The window is measured in TIME, not in cycles, and that is the whole design.
// The first version diffed against "last cycle" and recorded a reading only
// when a check was due, which coupled the baseline to the probe schedule: any
// check whose interval was longer than the executor's tick had its reading
// dropped on every cycle in between, so it never had a baseline and could
// never skip. The tick is the shortest interval across ALL services, so one
// fast check anywhere silently disabled skipping everywhere. It shipped that
// way and the canary found it on 2026-09-03 — flag on, thousands of traffic
// signals per key per interval, zero skips.
//
// State is one small struct per backend actually probed, rebuilt each cycle
// from the backends seen, so a backend that leaves the session takes its entry
// with it the way lastRun does. This is not another map that only grows.
//
// Two facts make the diff safe rather than clever. A key the counter does not
// know yet is not a key with no traffic, so a first sighting never skips. And
// a count that went backwards means the key was evicted and re-created between
// readings, which is a reset rather than negative traffic, so it does not skip
// either.
type trafficSkipper struct {
	counter    reputation.TrafficCounter
	minSignals uint64

	prev map[trafficKey]trafficReading
	next map[trafficKey]trafficReading

	// lastDecision records what the most recent cycle observed, for the log
	// line that says why nothing is being skipped. Only runOnce's goroutine
	// touches it.
	lastDecision skipDecision
}

// trafficReading is a cumulative signal count and when it was taken. The
// timestamp is what lets the window be a duration rather than a cycle count.
type trafficReading struct {
	signals uint64
	at      time.Time
}

// skipDecision summarises one cycle for diagnostics: how many checks were
// considered, how many were skipped, and the largest traffic delta seen
// against the threshold that delta had to clear.
type skipDecision struct {
	considered int
	skipped    int
	maxDelta   uint64
	waiting    int // readings whose window has not elapsed yet
}

// trafficKey identifies what a traffic reading is about: one backend of one
// service, reached over one RPC type. The RPC type is part of it because it is
// part of the reputation key — a supplier's REST traffic says nothing about
// its WebSocket backend, so it must not excuse a WebSocket probe.
type trafficKey struct {
	service domain.ServiceID
	backend string
	rpcType domain.RPCType
}

func newTrafficSkipper(counter reputation.TrafficCounter, cfg TrafficSkipConfig) *trafficSkipper {
	minSignals := cfg.MinSignals
	if minSignals == 0 {
		minSignals = MinTrafficSignalsFor(cfg.SampleRate)
	}
	return &trafficSkipper{
		counter:    counter,
		minSignals: minSignals,
		prev:       make(map[trafficKey]trafficReading),
		next:       make(map[trafficKey]trafficReading),
	}
}

// beginCycle starts a fresh set of readings. Called once per cycle from
// runOnce's goroutine, which is the only goroutine that touches the maps.
func (t *trafficSkipper) beginCycle() {
	t.next = make(map[trafficKey]trafficReading, len(t.prev))
	t.lastDecision = skipDecision{}
}

// endCycle promotes this cycle's readings to be the next cycle's baseline.
func (t *trafficSkipper) endCycle() {
	t.prev = t.next
}

// skip reports whether client traffic has graded this backend enough, over a
// window at least as long as interval, that a probe would only confirm what
// the score middleware already recorded.
//
// It carries a reading forward until its window elapses rather than replacing
// it every cycle: the question is "how much traffic since a probe's worth of
// time ago", and refreshing the baseline on a cycle shorter than that would
// keep resetting the clock and never accumulate a window.
func (t *trafficSkipper) skip(
	serviceID domain.ServiceID,
	ep domain.EndpointAddr,
	backend string,
	rpcType domain.RPCType,
	interval time.Duration,
	now time.Time,
) bool {
	t.lastDecision.considered++

	signals, known := t.counter.TrafficSignals(serviceID, ep, rpcType)
	if !known {
		return false
	}
	key := trafficKey{service: serviceID, backend: backend, rpcType: rpcType}

	before, seen := t.prev[key]
	if !seen {
		t.next[key] = trafficReading{signals: signals, at: now}
		return false
	}
	if now.Sub(before.at) < interval {
		// Window still open: keep the baseline so it can accumulate.
		t.next[key] = before
		t.lastDecision.waiting++
		return false
	}

	// Window elapsed: this reading becomes the next baseline either way.
	t.next[key] = trafficReading{signals: signals, at: now}
	if signals < before.signals {
		return false
	}
	delta := signals - before.signals
	if delta > t.lastDecision.maxDelta {
		t.lastDecision.maxDelta = delta
	}
	if delta < t.minSignals {
		return false
	}
	t.lastDecision.skipped++
	return true
}
