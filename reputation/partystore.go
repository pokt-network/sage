package reputation

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A pod's party penalties (stale share, trust) rest on its own evidence, which
// a new pod does not have: after a roll or a KEDA scale-up the penalized party
// was charged nothing on the new pods until each had gathered its own (minutes
// for stale share, longer for trust). The leader writes what it prices; a new
// pod adopts that at boot as the view before its first, under the holds that
// already exist (staleShareHold, the trust hold's own end), and its own
// evidence takes over from there. Storage bridges the gap and decides nothing.

// PartyPenalties is the priced parties as the leader last saw them.
type PartyPenalties struct {
	Stale []StoredStale `json:"stale,omitempty"`
	Trust []StoredTrust `json:"trust,omitempty"`
}

// StoredStale is one priced stale-share party: the penalty and when it was
// last measured, so the hold runs from then, not from the adoption.
type StoredStale struct {
	Service  domain.ServiceID `json:"service"`
	Party    string           `json:"party"`
	Penalty  float64          `json:"penalty"`
	PricedAt int64            `json:"priced_at"`
}

// StoredTrust is one distrusted party and when its penalty lapses.
type StoredTrust struct {
	Party string `json:"party"`
	Until int64  `json:"until"`
}

// PartyPenaltyStore is the optional half of Storage that keeps the leader's
// priced parties. A storage without it starts every pod from nothing, as
// before.
type PartyPenaltyStore interface {
	GetPartyPenalties(ctx context.Context) (PartyPenalties, error)
	SetPartyPenalties(ctx context.Context, p PartyPenalties) error
}

// storePartyPenalties writes the view's priced parties. Called on every
// refresh; LeaderOnlyStorage keeps a follower's copy out.
func (s *serviceImpl) storePartyPenalties(v *chronicView) {
	store, ok := s.storage.(PartyPenaltyStore)
	if !ok {
		return
	}
	var p PartyPenalties
	for _, st := range v.stale {
		if st.Penalty < 0 {
			p.Stale = append(p.Stale, StoredStale{Service: st.ServiceID, Party: st.Party, Penalty: st.Penalty, PricedAt: st.pricedAt.Unix()})
		}
	}
	for _, t := range v.trust {
		if t.Penalty < 0 {
			p.Trust = append(p.Trust, StoredTrust{Party: t.Party, Until: t.until.Unix()})
		}
	}
	_ = store.SetPartyPenalties(context.Background(), p)
}

// adoptPartyPenalties seeds the view with the stored priced parties as the
// one before this pod's first, then refreshes, so the holds carry them: a
// stale-share penalty until staleShareHold after it was last priced, a trust
// penalty until its stored end. Anything already past those is dropped by
// the same rules. It reports how many parties were adopted.
func (s *serviceImpl) adoptPartyPenalties(ctx context.Context) int {
	store, ok := s.storage.(PartyPenaltyStore)
	if !ok {
		return 0
	}
	p, err := store.GetPartyPenalties(ctx)
	if err != nil || len(p.Stale)+len(p.Trust) == 0 {
		return 0
	}
	seed := &chronicView{}
	for _, st := range p.Stale {
		seed.stale = append(seed.stale, PartyStale{ServiceID: st.Service, Party: st.Party, Penalty: st.Penalty, pricedAt: time.Unix(st.PricedAt, 0), held: true})
	}
	for _, t := range p.Trust {
		seed.trust = append(seed.trust, PartyTrust{Party: t.Party, Penalty: trustPenalty, until: time.Unix(t.Until, 0)})
	}
	// Only if this pod has not priced anything yet: its own view, if it has
	// one, is newer than the leader's last write.
	if cur := s.chronic.Load(); cur == nil || len(cur.stale)+len(cur.trust) == 0 {
		s.chronic.Store(seed)
	}
	s.refreshBaselines()
	return len(p.Stale) + len(p.Trust)
}
