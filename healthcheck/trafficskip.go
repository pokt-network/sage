package healthcheck

import (
	"math"

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
// It holds the previous cycle's cumulative signal counts and skips a check
// whose backend gained MinSignals or more since then. The state is one uint64
// per (service, backend, RPC type) actually probed, and it is rebuilt from
// scratch every cycle rather than updated in place — a backend that leaves the
// session takes its entry with it, the way lastRun works, so this cannot
// become another map that only ever grows.
//
// Two facts make the diff safe rather than clever. A key the counter does not
// know yet is not a key with no traffic, so a first sighting never skips. And
// a count that went backwards means the key was evicted and re-created between
// cycles, which is a reset rather than negative traffic, so it does not skip
// either.
type trafficSkipper struct {
	counter    reputation.TrafficCounter
	minSignals uint64

	prev map[trafficKey]uint64
	next map[trafficKey]uint64
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
		prev:       make(map[trafficKey]uint64),
		next:       make(map[trafficKey]uint64),
	}
}

// beginCycle starts a fresh reading. Called once per cycle from runOnce's
// goroutine, which is the only goroutine that touches the maps.
func (t *trafficSkipper) beginCycle() {
	t.next = make(map[trafficKey]uint64, len(t.prev))
}

// endCycle promotes this cycle's readings to be the next cycle's baseline.
func (t *trafficSkipper) endCycle() {
	t.prev = t.next
}

// skip reports whether client traffic has graded this backend enough since the
// last cycle that a probe would only confirm what the score middleware has
// already recorded. It records the reading either way: a probe that runs this
// cycle still needs a baseline for the next one.
func (t *trafficSkipper) skip(serviceID domain.ServiceID, ep domain.EndpointAddr, backend string, rpcType domain.RPCType) bool {
	signals, known := t.counter.TrafficSignals(serviceID, ep, rpcType)
	if !known {
		return false
	}
	key := trafficKey{service: serviceID, backend: backend, rpcType: rpcType}
	t.next[key] = signals

	before, seen := t.prev[key]
	if !seen || signals < before {
		return false
	}
	return signals-before >= t.minSignals
}
