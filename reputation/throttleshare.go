package reputation

import (
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party that throttles relays — an HTTP 429 from its backend or its relay
// miner, or a node's own rate-limit answer — is charged for the share of its
// first attempts it throttled, on every key it has in the service except
// WebSocket ones (featureflag.FlagThrottleShare), priced like dupshare:
// relative to the service's cleanest party. A throttled relay is graded minor,
// which the chronic rate does not weigh, so without this a throttling party
// keeps its rank.
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
	// penalty: the bases of the reputation.throttle_share_* knobs.
	DefaultThrottleShareFloor = 0.02
	DefaultThrottleShareFull  = 0.15
	// reasonHTTP429, reasonUpstream429 and reasonRateLimited are the
	// heuristic's reasons for an HTTP 429 in the relay's answer, one from the
	// relay miner itself, and a node's own rate-limit answer.
	reasonHTTP429     = "http_429"
	reasonUpstream429 = "upstream_429"
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
	switch signal.Reason {
	case reasonHTTP429, reasonUpstream429, reasonRateLimited:
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
