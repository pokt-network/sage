package reputation

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
)

// An instance with little traffic of its own never gathers the evidence a
// party penalty needs (staleShareMinAnswers per party, trustStaleServices
// vetted services), so a party another instance has caught serving stale or
// refused answers is charged nothing there. Following a peer's priced parties
// (active_health_checks.peer_parties) borrows that verdict as a floor: each
// (service, party) and each party is charged the harsher of this instance's
// own penalty and the peer's, on the peer's clocks, and only where this
// instance's own flags price it.

// peerPartiesTimeout bounds the peer read inside a refresh.
const peerPartiesTimeout = 2 * time.Second

// SetPeerParties makes every refresh read a peer instance's priced parties
// through read and apply them as a floor (mergePeerParties). Call at wire
// time; nil turns it off.
func (s *serviceImpl) SetPeerParties(read func(context.Context) (PartyPenalties, error)) {
	s.peerParties.Store(&read)
}

// readPeerParties returns the peer's priced parties, none when unset or
// unreadable: a peer that cannot be read lends nothing, and what it lent
// before runs out on its own clocks.
func (s *serviceImpl) readPeerParties() PartyPenalties {
	rp := s.peerParties.Load()
	if rp == nil || *rp == nil {
		return PartyPenalties{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerPartiesTimeout)
	defer cancel()
	p, err := (*rp)(ctx)
	if err != nil {
		return PartyPenalties{}
	}
	return p
}

// mergePeerParties lowers stale and trust to the peer's penalties where the
// peer's are harsher. A stale-share penalty is borrowed only for a service
// this instance serves (served) with stale_share on (staleOn), and only while
// the peer's own hold runs (staleShareHold from its priced_at); a trust
// penalty only until the peer's own end. Trust is charged per service through
// the trust gate as any other, so nothing here reads that flag.
//
// A borrowed stale penalty enters held: never trust evidence here (partyTrust
// skips held entries), and carried by the next refresh's hold from the peer's
// priced_at, never from now. Run it after partyTrust, so a borrowed penalty
// cannot count toward this instance's own trust verdict either way.
func mergePeerParties(stale []PartyStale, trust []PartyTrust, peer PartyPenalties, served, staleOn func(domain.ServiceID) bool, now time.Time) ([]PartyStale, []PartyTrust) {
	for _, ps := range peer.Stale {
		pricedAt := time.Unix(ps.PricedAt, 0)
		if ps.Penalty >= 0 || now.Sub(pricedAt) >= staleShareHold || !served(ps.Service) || staleOn == nil || !staleOn(ps.Service) {
			continue
		}
		i := indexStale(stale, ps.Service, ps.Party)
		switch {
		case i < 0:
			stale = append(stale, PartyStale{ServiceID: ps.Service, Party: ps.Party, Penalty: ps.Penalty, pricedAt: pricedAt, held: true, peer: true})
		case ps.Penalty < stale[i].Penalty:
			// The local measurement stays (its share and excess are this
			// instance's evidence); the charge and its clock are the peer's.
			stale[i].Penalty, stale[i].pricedAt, stale[i].peer = ps.Penalty, pricedAt, true
		}
	}
	for _, pt := range peer.Trust {
		until := time.Unix(pt.Until, 0)
		if !now.Before(until) {
			continue
		}
		i := indexTrust(trust, pt.Party)
		switch {
		case i < 0:
			trust = append(trust, PartyTrust{Party: pt.Party, Penalty: trustPenalty, until: until, peer: true})
		case trust[i].Penalty > trustPenalty:
			// Evidence here, but not enough: the peer's verdict and end. A
			// party distrusted here already keeps its own end; once that
			// lapses, the next refresh borrows the peer's if it still runs.
			trust[i].Penalty, trust[i].until, trust[i].peer = trustPenalty, until, true
		}
	}
	return stale, trust
}

func indexStale(stale []PartyStale, svc domain.ServiceID, party string) int {
	for i, p := range stale {
		if p.ServiceID == svc && p.Party == party {
			return i
		}
	}
	return -1
}

func indexTrust(trust []PartyTrust, party string) int {
	for i, t := range trust {
		if t.Party == party {
			return i
		}
	}
	return -1
}
