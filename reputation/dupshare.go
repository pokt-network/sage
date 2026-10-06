package reputation

import (
	"context"
	"time"

	"github.com/pokt-network/sage/domain"
)

// A party that repeats its WebSocket notifications is charged for it on every
// one of its WebSocket keys in the service (featureflag.FlagWSDuplicateShare),
// the way stale_share charges a stale head.
//
// The per-minute ws_duplicate_notifications signal is charged to one key, and
// a repeating party's keys are one URL per node, a fresh set every session:
// on mainnet bsc (2026-10-05) parties repeating 25-28% of their pending
// hashes held WebSocket keys at 95-100 while charged dozens of times an hour,
// each charge erased by the next success. So the evidence is kept per
// (service, party) as decayed counts of notifications and repeats, reported by
// each connection once a minute, and priced relative to the service's cleanest
// party: a chain whose whole mempool re-announces repeats for everyone.
const (
	// dupShareHalfLife ages the counts by the clock, as staleShareHalfLife.
	dupShareHalfLife = 30 * time.Minute
	// dupShareMinNotes is the decayed evidence a party needs to be measured,
	// or to set the service's baseline.
	dupShareMinNotes = 500
	// dupSharePenalty is the most the term takes off a score.
	dupSharePenalty = -40.0
	// dupShareHold keeps a priced party's penalty once too little evidence
	// remains to measure it: the penalty moves its connections away, which
	// starves the evidence (staleShareHold).
	dupShareHold = dupShareHalfLife
	// DefaultDuplicateShareFloor is the excess over the cleanest party charged
	// nothing, and DefaultDuplicateShareFull the excess charged the whole
	// penalty: the bases of the websocket.duplicate_share_* knobs.
	DefaultDuplicateShareFloor = 0.02
	DefaultDuplicateShareFull  = 0.20
)

// RecordNotifications counts notifications a party pushed on one WebSocket
// connection in a service, dups of them repeats. Counted whatever the flag
// says, so the share is there to read before it is charged.
func (s *serviceImpl) RecordNotifications(serviceID domain.ServiceID, party string, notes, dups int) {
	if party == "" || notes <= 0 {
		return
	}
	s.dups.recordN(opID{svc: serviceID, op: party}, float64(notes), float64(dups), time.Now())
}

// A WebSocket connection stays on the pod that opened it, so one pod's counts
// say nothing about a party whose connections sit on other pods: priced per
// pod, the penalty landed only where the repeater's connections happened to
// be, and a pod whose connections had moved away stopped charging and picked
// the party again (mainnet gnosis, 2026-10-05). So every pod writes its counts
// to the shared store on each refresh and prices every party on the fleet's
// sum: its own counts plus every other pod's last write, aged to now.
const (
	// dupShareFleetMaxAge is how old another pod's write may be and still
	// count: a pod gone (rolled, scaled in) stops writing, and its counts are
	// dropped and deleted past this.
	dupShareFleetMaxAge = 10 * time.Minute
	// dupShareFleetTimeout bounds the shared store's round trips on a refresh.
	dupShareFleetTimeout = 2 * time.Second
)

// NotificationCounts is one pod's per-party notification counts, aged to At.
type NotificationCounts struct {
	At     int64                `json:"at"`
	Counts []PartyNotifications `json:"counts"`
}

// PartyNotifications is one party's notifications and repeats in a service.
type PartyNotifications struct {
	Service domain.ServiceID `json:"s"`
	Party   string           `json:"p"`
	Notes   float64          `json:"n"`
	Dups    float64          `json:"d"`
}

// NotificationCountStore is the optional half of Storage that shares each
// pod's notification counts with the fleet. Every pod writes, the leader or
// not.
type NotificationCountStore interface {
	PutNotificationCounts(ctx context.Context, pod string, c NotificationCounts) error
	NotificationCounts(ctx context.Context) (map[string]NotificationCounts, error)
	DeleteNotificationCounts(ctx context.Context, pods ...string) error
}

// SetInstanceID names this pod in the shared notification counts; without one
// the repeat share is priced on this pod's own counts. Call at wire time.
func (s *serviceImpl) SetInstanceID(id string) { s.instanceID = id }

// fleetDuplicates is the evidence the repeat share is priced on: this pod's
// counts plus every other pod's last write within dupShareFleetMaxAge, each
// aged to now. This pod's counts are written first. Without a shared store, or
// when it fails, this pod's own.
func (s *serviceImpl) fleetDuplicates(now time.Time) map[opID]OperatorStat {
	local := s.dups.snapshot(now)
	store, ok := s.storage.(NotificationCountStore)
	if !ok || s.instanceID == "" {
		return local
	}
	ctx, cancel := context.WithTimeout(context.Background(), dupShareFleetTimeout)
	defer cancel()
	mine := NotificationCounts{At: now.Unix()}
	for id, st := range local {
		mine.Counts = append(mine.Counts, PartyNotifications{Service: id.svc, Party: id.op, Notes: st.Attempts, Dups: st.Failures})
	}
	_ = store.PutNotificationCounts(ctx, s.instanceID, mine)
	all, err := store.NotificationCounts(ctx)
	if err != nil {
		return local
	}
	out := make(map[opID]OperatorStat, len(local))
	for id, st := range local {
		out[id] = st
	}
	var gone []string
	for pod, c := range all {
		if pod == s.instanceID {
			continue
		}
		at := time.Unix(c.At, 0)
		if now.Sub(at) > dupShareFleetMaxAge {
			gone = append(gone, pod)
			continue
		}
		for _, pc := range c.Counts {
			aged := OperatorStat{Attempts: pc.Notes, Failures: pc.Dups, UpdatedAt: c.At}.decayTo(now, dupShareHalfLife)
			id := opID{svc: pc.Service, op: pc.Party}
			st := out[id]
			st.Attempts += aged.Attempts
			st.Failures += aged.Failures
			out[id] = st
		}
	}
	if len(gone) > 0 {
		_ = store.DeleteNotificationCounts(ctx, gone...)
	}
	return out
}

// SetDuplicateShare turns on the repeat-share penalty per service behind gate;
// limits gives a service's floor and full excess. Both are read on each
// baseline refresh. Call at wire time.
func (s *serviceImpl) SetDuplicateShare(gate func(domain.ServiceID) bool, limits func(domain.ServiceID) (floor, full float64)) {
	s.dupGate.Store(&gate)
	s.dupLimits.Store(&limits)
}

// PartyShare is one party's share of a kind of bad answer in a service (its
// WebSocket repeats, its throttled relays) and the penalty it is charged, as
// of the last refresh. Penalty is 0 where the kind's flag is off.
type PartyShare struct {
	ServiceID domain.ServiceID
	Party     string
	Share     float64
	Penalty   float64
	// pricedAt is when the penalty was last measured rather than held.
	pricedAt time.Time
}

// PartyDuplicateShares reports every party measured for WebSocket repeats,
// for the metrics collector.
func (s *serviceImpl) PartyDuplicateShares() []PartyShare {
	if v := s.chronic.Load(); v != nil {
		return v.dup
	}
	return nil
}

// shareRule is how one kind of party share is priced.
type shareRule struct {
	// minEvidence is the decayed count a party needs to be measured, or to
	// set the service's baseline.
	minEvidence float64
	// most is the whole penalty, charged from the full excess on.
	most float64
	// hold keeps a priced party's penalty once too little evidence remains
	// to measure it: the penalty moves its traffic away, which starves the
	// evidence (staleShareHold).
	hold time.Duration
	// floor and full are the excess charged nothing and the excess charged
	// most, where the rule's limits give none.
	floor, full float64
}

// dupRule prices WebSocket repeats.
var dupRule = shareRule{minEvidence: dupShareMinNotes, most: dupSharePenalty, hold: dupShareHold,
	floor: DefaultDuplicateShareFloor, full: DefaultDuplicateShareFull}

// priceShares measures every party with enough evidence (Attempts the
// denominator, Failures the bad ones) and prices it against its service's
// cleanest. A service with one measured party charges nothing. A party priced
// in prev that is no longer measured keeps its penalty for the rule's hold
// from when it was last priced (flag permitting).
func priceShares(stats map[opID]OperatorStat, rule shareRule, on func(domain.ServiceID) bool, limits func(domain.ServiceID) (float64, float64), prev []PartyShare, now time.Time) []PartyShare {
	bySvc := map[domain.ServiceID][]PartyShare{}
	for id, st := range stats {
		if st.Attempts < rule.minEvidence {
			continue
		}
		bySvc[id.svc] = append(bySvc[id.svc], PartyShare{ServiceID: id.svc, Party: id.op, Share: st.Failures / st.Attempts})
	}
	var out []PartyShare
	measured := map[opID]bool{}
	for svc, parties := range bySvc {
		best := 1.0
		for _, p := range parties {
			best = min(best, p.Share)
		}
		charged := len(parties) >= 2 && on != nil && on(svc)
		floor, full := rule.floor, rule.full
		if charged && limits != nil {
			floor, full = limits(svc)
		}
		for _, p := range parties {
			if charged {
				p.Penalty = excessPenalty(p.Share-best, floor, full, rule.most)
				p.pricedAt = now
			}
			measured[opID{svc: svc, op: p.Party}] = true
			out = append(out, p)
		}
	}
	for _, p := range prev {
		if p.Penalty < 0 && !measured[opID{svc: p.ServiceID, op: p.Party}] &&
			on != nil && on(p.ServiceID) && now.Sub(p.pricedAt) < rule.hold {
			out = append(out, p)
		}
	}
	return out
}

// excessPenalty prices an excess share: nothing up to floor, linear to most
// at full.
func excessPenalty(excess, floor, full, most float64) float64 {
	if excess <= floor {
		return 0
	}
	if full <= floor {
		return most
	}
	return most * min(1, (excess-floor)/(full-floor))
}

// duplicatePenalty is what a WebSocket key's party's repeats cost it in a
// service: 0 for any other key.
func (v *chronicView) duplicatePenalty(svc domain.ServiceID, key string) float64 {
	if v == nil || len(v.dupPen) == 0 || rpcOfKey(key) != string(domain.RPCTypeWebSocket) {
		return 0
	}
	party, ok := v.keyParty[key]
	if !ok {
		party = partyOfKey(key)
	}
	return v.dupPen[opID{svc: svc, op: party}]
}
