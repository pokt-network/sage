package reputation

import (
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party that answers the chain head stale is charged for it on all of its
// traffic, not only on the head calls that show it.
//
// stale_response grades each stale answer where it happens, as a major hit on
// one key. That works where head calls are a large part of a party's traffic
// and fails where they are not: on mainnet (2026-09-30) one owner's cache
// answered 70-80% of its head calls stale on tron, bera and arb-one, but head
// calls were 1-2% of its traffic there, spread over 12-57 keys a pod, and each
// key healed between hits. A cache that serves an old head serves an old
// "latest" to eth_call and eth_getLogs too; only the head calls say so.
//
// So the evidence is kept per (service, party) — domain.EndpointAddr.Party,
// the key the height anchor votes by — as decayed counts of head answers and
// stale ones, and the share is charged, relative to the service's cleanest
// party, to every key of the party (featureflag.FlagStaleShare). Relative,
// because a chain whose head the projection runs ahead of reads stale for
// everyone: on scroll every party sat near 20%.
const (
	// staleShareHalfLife ages the counts by the clock, so a party that gets
	// little traffic fades and is measured afresh rather than frozen.
	staleShareHalfLife = 30 * time.Minute
	// staleShareMinAnswers is the decayed evidence a party needs to be
	// measured, or to set the service's baseline. Counts are per pod, so a
	// demoted party's evidence falls under this fast; see staleShareHold.
	staleShareMinAnswers = 50
	// staleShareFloor is the excess over the cleanest party charged nothing:
	// honest parties' propagation tails reached 11 points on 2026-09-30, the
	// cache's lowest 22.
	staleShareFloor = 0.15
	// staleShareFull is the excess charged the whole staleSharePenalty.
	staleShareFull = 0.45
	// staleSharePenalty is the most the term takes off a score.
	staleSharePenalty = -40.0
	// staleShareHold is how long a priced party keeps its penalty once too
	// few answers remain to measure it. The penalty itself starves the
	// evidence: on mainnet (2026-09-30) the owner a stale_response demotion
	// had already moved off poly, fantom and zksync-era fell under the
	// minimum on every pod, and was not measured at all. Fresh evidence
	// replaces a held penalty at once, in either direction.
	staleShareHold = staleShareHalfLife
)

// RecordHeadAnswer counts one answer that named the chain head, and whether it
// was stale (qos.HeadLagReader), against the party that served it. Counted
// whatever the flag says, so the share is there to read before it is charged.
func (s *serviceImpl) RecordHeadAnswer(serviceID domain.ServiceID, party string, stale bool) {
	if party == "" {
		return
	}
	failure := 0.0
	if stale {
		failure = 1
	}
	s.heads.record(opID{svc: serviceID, op: party}, failure, time.Now())
}

// SetStaleShare turns on the stale-share penalty, per service, behind gate.
// Call at wire time; the gate is read on each baseline refresh.
func (s *serviceImpl) SetStaleShare(gate func(domain.ServiceID) bool) {
	s.staleGate.Store(&gate)
}

// PartyStale is one party's stale head share in a service and the penalty it
// is charged, as of the last refresh. Penalty is 0 where the flag is off.
type PartyStale struct {
	ServiceID domain.ServiceID
	Party     string
	Share     float64
	Penalty   float64
	// Excess is Share over the service's cleanest measured party, 0 where
	// the service has one measured party. Computed whatever the flag says:
	// the trust evidence reads it (trust.go).
	Excess float64
	// pricedAt is when the penalty was last measured rather than held.
	pricedAt time.Time
}

// PartyStaleShares reports every measured party, for the metrics collector.
func (s *serviceImpl) PartyStaleShares() []PartyStale {
	if v := s.chronic.Load(); v != nil {
		return v.stale
	}
	return nil
}

// partyStale measures every party with enough evidence and prices it against
// its service's cleanest. A service with one measured party charges nothing:
// with no second party, a chain-wide lag and a party's cache look the same.
//
// A party priced in prev that is no longer measured keeps its penalty for
// staleShareHold from when it was last priced (flag permitting).
func partyStale(stats map[opID]OperatorStat, on func(domain.ServiceID) bool, prev []PartyStale, now time.Time) []PartyStale {
	bySvc := map[domain.ServiceID][]PartyStale{}
	for id, st := range stats {
		if st.Attempts < staleShareMinAnswers {
			continue
		}
		bySvc[id.svc] = append(bySvc[id.svc], PartyStale{ServiceID: id.svc, Party: id.op, Share: st.Failures / st.Attempts})
	}
	var out []PartyStale
	measured := map[opID]bool{}
	for svc, parties := range bySvc {
		best := 1.0
		for _, p := range parties {
			best = min(best, p.Share)
		}
		for _, p := range parties {
			if len(parties) >= 2 {
				p.Excess = p.Share - best
			}
			if len(parties) >= 2 && on != nil && on(svc) {
				p.Penalty = stalePenalty(p.Share - best)
				p.pricedAt = now
			}
			measured[opID{svc: svc, op: p.Party}] = true
			out = append(out, p)
		}
	}
	for _, p := range prev {
		if p.Penalty < 0 && !measured[opID{svc: p.ServiceID, op: p.Party}] &&
			on != nil && on(p.ServiceID) && now.Sub(p.pricedAt) < staleShareHold {
			out = append(out, p)
		}
	}
	return out
}

// stalePenalty prices a party's excess stale share: nothing up to
// staleShareFloor, linear to staleSharePenalty at staleShareFull.
func stalePenalty(excess float64) float64 {
	if excess <= staleShareFloor {
		return 0
	}
	return staleSharePenalty * min(1, (excess-staleShareFloor)/(staleShareFull-staleShareFloor))
}
