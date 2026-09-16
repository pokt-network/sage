package reputation

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// An operator's failure rate cannot be measured from its keys, because its
// keys do not live long enough to measure anything.
//
// Shannon draws a fresh set of endpoints for an application every session —
// 20 blocks, roughly 20 minutes. An operator with a wide stake is represented
// by a different handful of hosts each time: on mainnet sei (2026-09-16) one
// operator had ~275 endpoints staked and 7 in any given session, and two
// sessions nine hours apart shared exactly one host. Each per-URL key
// therefore collects a few hundred attempts and then its host leaves the
// draw; by the time that host returns, its stored state is past the idle TTL
// and starts again from zero. An operator with a narrow stake, whose few
// hosts recur, accumulates mature keys instead.
//
// Anything derived from those keys inherits the asymmetry. The per-key chronic
// rate did (it read a wide-stake operator as near-perfect), and so did the
// attempt-weighted correction that replaced it: dividing by an attempt count
// that resets every session leaves the estimate swinging on noise. The same
// operator read 0.096 one morning and 0.034 that evening with nothing about
// its behaviour having changed — and since the pool baseline is the best rate
// in the pool, that swing was enough to invert which operator got charged.
//
// So the measurement is kept where the identity is stable. An operator
// (eTLD+1) does not rotate, is not redrawn per session, and survives a pod
// roll. These counters are attached to (service, operator, rpc_type), decay by
// TIME rather than by attempts — so a quiet operator fades instead of freezing
// — and are persisted, so they outlive both the session that produced them and
// the process that observed them.

// One rule differs from the per-key rate on purpose: a floored key still
// contributes here. The per-key term stops counting once a key's additive
// score reaches zero, so that probes against a dead host cannot pin it at the
// cap for weeks, a term that decays by attempts having no other way back.
// These counters decay by time, so they need no such protection — and the
// traffic a floored key receives is the measurement, not noise: the
// pool-collapse fallback serves the least-bad member of a pool where every key
// is floored, and ignoring those attempts would make the operator the fallback
// feeds look better the worse it became.

// DefaultOperatorHalfLife is how long an operator's evidence takes to lose
// half its weight. Six hours spans many sessions, so a rate reflects the
// operator across the draws it has been given rather than the luck of the
// current one, and still forgets a bad day by the next.
const DefaultOperatorHalfLife = 6 * time.Hour

// minOperatorAttempts is the decayed evidence an operator needs before its
// rate is used at all. Below it the ratio is noise.
const minOperatorAttempts = 200

// OperatorStat is one operator's decayed evidence in one pool.
//
// Attempts and Failures are weighted counts, not integers: they decay
// continuously, and a failure contributes what FailureWeight says (a major
// error is half a failure), the same weighting the per-key rate used.
type OperatorStat struct {
	Attempts  float64 `json:"attempts"`
	Failures  float64 `json:"failures"`
	UpdatedAt int64   `json:"updated_at"`
}

// decayTo returns the stat aged forward to now. Decay is applied on read and
// before every update, so the stored pair is always "as of UpdatedAt".
func (s OperatorStat) decayTo(now time.Time, halfLife time.Duration) OperatorStat {
	if s.UpdatedAt <= 0 || halfLife <= 0 {
		s.UpdatedAt = now.Unix()
		return s
	}
	elapsed := now.Sub(time.Unix(s.UpdatedAt, 0))
	if elapsed <= 0 {
		return s
	}
	factor := math.Exp(-math.Ln2 * elapsed.Seconds() / halfLife.Seconds())
	s.Attempts *= factor
	s.Failures *= factor
	s.UpdatedAt = now.Unix()
	return s
}

// Rate is failures per attempt, or 0 when there is too little evidence to say.
func (s OperatorStat) Rate() float64 {
	if s.Attempts < minOperatorAttempts {
		return 0
	}
	return min(s.Failures/s.Attempts, 1)
}

// OperatorStatStore is the optional half of Storage that persists operator
// evidence. A backend that does not implement it keeps the counters in memory
// only, which costs the fleet's shared view and a pod's history across a
// restart — the same degradation the rest of the service accepts without
// Redis.
type OperatorStatStore interface {
	// GetOperatorStats returns every stored stat, keyed by OperatorField.
	GetOperatorStats(ctx context.Context) (map[string]OperatorStat, error)
	// SetOperatorStat writes one, keyed by OperatorField.
	SetOperatorStat(ctx context.Context, field string, st OperatorStat) error
}

// OperatorField is the storage key for one (service, operator, rpc_type).
// The separator is "|" because an operator is a registrable domain and a
// service ID is a short name; neither contains one.
func OperatorField(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) string {
	return string(serviceID) + "|" + operator + "|" + string(rpcType)
}

// parseOperatorField reverses OperatorField.
func parseOperatorField(field string) (opID, bool) {
	parts := strings.Split(field, "|")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return opID{}, false
	}
	return opID{svc: domain.ServiceID(parts[0]), op: parts[1], rpc: parts[2]}, true
}

// opTracker holds the live operator counters. One mutex covers the map: it is
// taken per recorded signal, like the shard lock beside it, and the map is
// small — operators per service, not keys per service.
//
// ponytail: one mutex over every operator counter; shard by service if a
// profile ever shows it.
type opTracker struct {
	mu       sync.RWMutex
	halfLife time.Duration
	stats    map[opID]OperatorStat
	dirty    map[opID]struct{}
}

func newOpTracker(halfLife time.Duration) *opTracker {
	if halfLife <= 0 {
		halfLife = DefaultOperatorHalfLife
	}
	return &opTracker{
		halfLife: halfLife,
		stats:    make(map[opID]OperatorStat),
		dirty:    make(map[opID]struct{}),
	}
}

// record adds one attempt, weighted by how much of a failure it was.
func (t *opTracker) record(id opID, failure float64, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.stats[id].decayTo(now, t.halfLife)
	st.Attempts++
	st.Failures += failure
	t.stats[id] = st
	t.dirty[id] = struct{}{}
}

// get returns one operator's stat, decayed to now.
func (t *opTracker) get(id opID, now time.Time) (OperatorStat, bool) {
	t.mu.RLock()
	st, ok := t.stats[id]
	t.mu.RUnlock()
	if !ok {
		return OperatorStat{}, false
	}
	return st.decayTo(now, t.halfLife), true
}

// snapshot returns every stat decayed to now. Off the relay path: the
// baseline refresh and the admin listing read it.
func (t *opTracker) snapshot(now time.Time) map[opID]OperatorStat {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[opID]OperatorStat, len(t.stats))
	for id, st := range t.stats {
		out[id] = st.decayTo(now, t.halfLife)
	}
	return out
}

// takeDirty returns the stats changed since the last call and clears the mark.
func (t *opTracker) takeDirty(now time.Time) map[opID]OperatorStat {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.dirty) == 0 {
		return nil
	}
	out := make(map[opID]OperatorStat, len(t.dirty))
	for id := range t.dirty {
		out[id] = t.stats[id].decayTo(now, t.halfLife)
	}
	t.dirty = make(map[opID]struct{})
	return out
}

// merge adopts stored stats, keeping whichever side has seen more. A pod that
// has been running holds evidence storage does not; a pod that just started
// holds none, and adopts the fleet's.
func (t *opTracker) merge(stored map[string]OperatorStat, now time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for field, st := range stored {
		id, ok := parseOperatorField(field)
		if !ok {
			continue
		}
		aged := st.decayTo(now, t.halfLife)
		if cur, exists := t.stats[id]; exists && cur.decayTo(now, t.halfLife).Attempts >= aged.Attempts {
			continue
		}
		t.stats[id] = aged
		n++
	}
	return n
}

// len reports how many operator counters are held, for the gauge.
func (t *opTracker) len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.stats)
}
