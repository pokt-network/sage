package reputation

import (
	"context"
	"errors"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A policy penalty is one the gateway's operators set on a party by hand, for
// conduct SAGE cannot measure. A party reselling a public RPC answers fresh
// and correct: stale share and trust never see it, and both stop charging a
// party once its answers are clean (mainnet, 2026-10-05: an owner proven to
// resell a public provider's feed fixed its stale cache overnight and its
// trust hold lapsed a day later). It is kept apart from trust, which says
// "measured faking", and carries the reason it was set for.
//
// It is charged like the other party penalties (featureflag.FlagPolicyPenalty):
// on every key of the party in a service where the flag is on, websocket keys
// where party_penalties_websocket is, as the largest of the party's penalties
// rather than their sum, and never below the selection floor, so it ranks a
// party down without taking it out of rotation. A party is an owner address
// over every domain dedicated to it, so a new brand of the same owner is
// charged as soon as its domain is known to be that owner's.

// PolicyPenalty is one party's policy penalty.
type PolicyPenalty struct {
	// Party is an owner address ("pokt1…") or an operator domain, as the
	// party penalties name parties (domain.PartyOfOperator).
	Party string `json:"party"`
	// Penalty is what it takes off every key's score: negative.
	Penalty float64 `json:"penalty"`
	// Reason is why it was set, for whoever reads it next.
	Reason string    `json:"reason"`
	SetAt  time.Time `json:"set_at"`
	// Until is when it lapses; zero means it stands until deleted.
	Until time.Time `json:"until,omitzero"`
}

// Active reports whether p is in force at now.
func (p PolicyPenalty) Active(now time.Time) bool {
	return p.Until.IsZero() || now.Before(p.Until)
}

// PolicyPenaltyStore is the optional half of Storage that keeps the policy
// penalties, one per party, shared by every pod. Every pod writes: an admin
// call lands on whichever one the request reaches.
type PolicyPenaltyStore interface {
	PutPolicyPenalty(ctx context.Context, p PolicyPenalty) error
	PolicyPenalties(ctx context.Context) ([]PolicyPenalty, error)
	// DeletePolicyPenalty reports whether the party had one.
	DeletePolicyPenalty(ctx context.Context, party string) (bool, error)
}

// PolicyPenaltyAdmin is the optional half of Service the admin API sets policy
// penalties through.
type PolicyPenaltyAdmin interface {
	SetPolicyPenalty(ctx context.Context, p PolicyPenalty) error
	DeletePolicyPenalty(ctx context.Context, party string) (bool, error)
	ListPolicyPenalties(ctx context.Context) ([]PolicyPenalty, error)
}

var _ PolicyPenaltyAdmin = (*serviceImpl)(nil)

// ErrNoPolicyStore is returned when the storage cannot hold policy penalties.
var ErrNoPolicyStore = errors.New("reputation storage cannot hold policy penalties")

// policyTimeout bounds the store read on each refresh.
const policyTimeout = 2 * time.Second

// SetPolicyPenaltyGate turns on charging policy penalties, per service, behind
// gate. Call at wire time.
func (s *serviceImpl) SetPolicyPenaltyGate(gate func(domain.ServiceID) bool) {
	s.policyGate.Store(&gate)
}

// SetPolicyPenalty stores p, replacing the party's previous one. It is charged
// from the next refresh, within 30 seconds, on every pod sharing the store.
func (s *serviceImpl) SetPolicyPenalty(ctx context.Context, p PolicyPenalty) error {
	store, ok := s.storage.(PolicyPenaltyStore)
	if !ok {
		return ErrNoPolicyStore
	}
	return store.PutPolicyPenalty(ctx, p)
}

// DeletePolicyPenalty removes the party's policy penalty.
func (s *serviceImpl) DeletePolicyPenalty(ctx context.Context, party string) (bool, error) {
	store, ok := s.storage.(PolicyPenaltyStore)
	if !ok {
		return false, ErrNoPolicyStore
	}
	return store.DeletePolicyPenalty(ctx, party)
}

// ListPolicyPenalties returns the policy penalties in force.
func (s *serviceImpl) ListPolicyPenalties(ctx context.Context) ([]PolicyPenalty, error) {
	store, ok := s.storage.(PolicyPenaltyStore)
	if !ok {
		return nil, ErrNoPolicyStore
	}
	all, err := store.PolicyPenalties(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := all[:0]
	for _, p := range all {
		if p.Active(now) {
			out = append(out, p)
		}
	}
	return out, nil
}

// PartyPolicies reports the policy penalties the last refresh charged, for the
// metrics collector.
func (s *serviceImpl) PartyPolicies() []PolicyPenalty {
	if v := s.chronic.Load(); v != nil {
		return v.policy
	}
	return nil
}

// policyPenalties reads the policy penalties in force for a refresh. A lapsed
// one is left in the store, ignored, until it is deleted or set again: deleting
// it here could race an admin setting the party anew. A failed read keeps the
// previous refresh's, so a Redis blip does not lift a penalty for 30 seconds.
func (s *serviceImpl) policyPenalties(prev []PolicyPenalty, now time.Time) []PolicyPenalty {
	store, ok := s.storage.(PolicyPenaltyStore)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), policyTimeout)
	defer cancel()
	all, err := store.PolicyPenalties(ctx)
	if err != nil {
		return prev
	}
	var out []PolicyPenalty
	for _, p := range all {
		if p.Active(now) {
			out = append(out, p)
		}
	}
	return out
}
