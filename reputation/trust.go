package reputation

import (
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party caught faking answers on several services is distrusted on all of
// them (featureflag.FlagTrustPenalty): every key it has, every service and RPC
// type, WebSocket included, ranks lower for a day after the evidence was last
// seen.
//
// stale_share and refused_recent charge where the evidence is: the service
// and, for refusals, the host. A party that fakes on the chains we measure
// serves the ones we do not (cosmos chains, WebSocket feeds, state at latest
// that no head call shows) from the same infrastructure. On mainnet
// (2026-10-01) one owner's cache was stale on 26 services and its hosts
// refused recent blocks as "pruned" on three chains, while it carried 90% of
// sei and a third of several cosmos chains where nothing measured it.
//
// The evidence is breadth, because that is what tells a cache or a policy
// from a lagging machine. An honest node behind the head is one node on one
// chain; it moves one service's stale share. On the same day honest parties
// reached three services at most (a lagging fleet, a node on two chains),
// the owner 24; eight leaves a wide margin on both sides.
const (
	// trustStaleServices is how many services a party's stale share must
	// exceed the cleanest party's by staleShareFloor on at once.
	trustStaleServices = 8
	// trustRefusalServices is how many services must each have seen at least
	// trustRefusalMin of the party's refused_recent verdicts in the window.
	trustRefusalServices = 2
	trustRefusalMin      = 10
	// refusalHalfLife ages the refusal counts: "in the last hour", roughly.
	refusalHalfLife = time.Hour
	// trustHold is how long a party stays distrusted after the evidence was
	// last seen.
	trustHold = 24 * time.Hour
	// trustPenalty is what distrust takes off a score. It is charged as the
	// larger of it and the party's stale-share penalty, never both: where
	// stale_share already measures the party it decides, and this reaches
	// the services and faces it does not. One tier (100 to 70), not out of
	// rotation.
	trustPenalty = -30.0
)

// PartyTrust is one party's trust evidence and penalty, as of the last
// refresh. Penalty is the penalty in force; whether a service charges it is
// that service's flag.
type PartyTrust struct {
	Party string
	// StaleServices and RefusalServices are how many services carry each
	// kind of evidence now.
	StaleServices   int
	RefusalServices int
	Penalty         float64
	// until is when the penalty lapses without fresh evidence.
	until time.Time
}

// SetTrustPenalty turns on charging the trust penalty, per service, behind
// gate. The evidence is computed either way. Call at wire time.
func (s *serviceImpl) SetTrustPenalty(gate func(domain.ServiceID) bool) {
	s.trustGate.Store(&gate)
}

// PartyTrusts reports every party with trust evidence or a penalty in force,
// for the metrics collector.
func (s *serviceImpl) PartyTrusts() []PartyTrust {
	if v := s.chronic.Load(); v != nil {
		return v.trust
	}
	return nil
}

// partyTrust tallies the evidence per party and carries a penalty set in prev
// until it lapses.
func partyTrust(stale []PartyStale, refusals map[opID]OperatorStat, prev []PartyTrust, now time.Time) []PartyTrust {
	by := map[string]*PartyTrust{}
	get := func(party string) *PartyTrust {
		t := by[party]
		if t == nil {
			t = &PartyTrust{Party: party}
			by[party] = t
		}
		return t
	}
	for _, p := range stale {
		if p.Excess > staleShareFloor {
			get(p.Party).StaleServices++
		}
	}
	for id, st := range refusals {
		if st.Attempts >= trustRefusalMin {
			get(id.op).RefusalServices++
		}
	}
	for _, p := range prev {
		if p.Penalty < 0 && now.Before(p.until) {
			get(p.Party).until = p.until
		}
	}
	out := make([]PartyTrust, 0, len(by))
	for _, t := range by {
		if t.StaleServices >= trustStaleServices || t.RefusalServices >= trustRefusalServices {
			t.until = now.Add(trustHold)
		}
		if now.Before(t.until) {
			t.Penalty = trustPenalty
		}
		out = append(out, *t)
	}
	return out
}
