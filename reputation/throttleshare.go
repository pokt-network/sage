package reputation

import (
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party that throttles — answers HTTP 429, or its node's own rate limit —
// is charged for it on every key it has in the service except WebSocket ones
// (featureflag.FlagThrottleShare), the way dupshare charges repeats.
//
// A throttled relay is graded minor, which the chronic rate does not weigh and
// the next success erases: on mainnet (2026-10-06) keys sending back 3-20% of
// their first attempts throttled held 90-100, and the largest throttler a
// fifth of its attempts on one chain. A throttle is honest, and retry rescues
// the request, but it says the party lacks the capacity its share of the
// traffic assumes; so the evidence is kept per (service, party) as decayed
// counts of first attempts and the throttled ones among them, and priced
// relative to the service's cleanest party: a chain whose public limits
// throttle everyone charges no one.
//
// Only the supplier's backend throttling counts. The relay miner's own
// refusals (upstream_429: its validation queue or its store saturated, with
// Retry-After: 1) are its admission control for about a second, and the
// session cap (over_serviced) is the protocol working; neither is scored as
// throttling here.
const (
	// throttleShareHalfLife ages the counts by the clock, as dupShareHalfLife.
	throttleShareHalfLife = 30 * time.Minute
	// throttleShareMinAttempts is the decayed evidence a party needs to be
	// measured, or to set the service's baseline.
	throttleShareMinAttempts = 500
	// throttleSharePenalty is the most the term takes off a score.
	throttleSharePenalty = -40.0
	// DefaultThrottleShareFloor is the excess over the cleanest party charged
	// nothing, and DefaultThrottleShareFull the excess charged the whole
	// penalty: the bases of the reputation.throttle_share_* knobs. A steady
	// 10% excess costs about 25 points, below tier 1.
	DefaultThrottleShareFloor = 0.02
	DefaultThrottleShareFull  = 0.15
	// reasonHTTP429 and reasonRateLimited are the heuristic's reasons for a
	// backend's HTTP 429 and a node's own rate-limit answer
	// (heuristic.Analyze, heuristic.ReasonRateLimited).
	reasonHTTP429     = "http_429"
	reasonRateLimited = "rate_limited"
)

// throttleRule prices throttling.
var throttleRule = shareRule{minEvidence: throttleShareMinAttempts, most: throttleSharePenalty, hold: throttleShareHalfLife,
	floor: DefaultThrottleShareFloor, full: DefaultThrottleShareFull}

// SetThrottleShare turns on the throttle-share penalty per service behind
// gate; limits gives a service's floor and full excess. Both are read on each
// refresh. Call at wire time.
func (s *serviceImpl) SetThrottleShare(gate func(domain.ServiceID) bool, limits func(domain.ServiceID) (floor, full float64)) {
	s.throttleGate.Store(&gate)
	s.throttleLimits.Store(&limits)
}

// PartyThrottleShares reports every party measured for throttling, for the
// metrics collector.
func (s *serviceImpl) PartyThrottleShares() []PartyShare {
	if v := s.chronic.Load(); v != nil {
		return v.throttle
	}
	return nil
}

// recordThrottle counts one graded attempt toward its party's throttle share:
// first client attempts only, the fair sample (retries and hedges carry what
// another host failed), and no WebSocket, whose attempts are connections.
func (s *serviceImpl) recordThrottle(serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType, signal Signal, now time.Time) {
	if signal.Type == "" || signal.Probe || signal.Leftover || rpcType == domain.RPCTypeWebSocket {
		return
	}
	party := endpoint.Party()
	if party == "" {
		return
	}
	var throttled float64
	if signal.Reason == reasonHTTP429 || signal.Reason == reasonRateLimited {
		throttled = 1
	}
	s.throttles.recordN(opID{svc: serviceID, op: party}, 1, throttled, now)
}

// throttlePenalty is what a key's party's throttling costs it in a service: 0
// for a WebSocket key.
func (v *chronicView) throttlePenalty(svc domain.ServiceID, key string) float64 {
	if v == nil || len(v.throttlePen) == 0 || rpcOfKey(key) == string(domain.RPCTypeWebSocket) {
		return 0
	}
	party, ok := v.keyParty[key]
	if !ok {
		party = partyOfKey(key)
	}
	return v.throttlePen[opID{svc: svc, op: party}]
}
