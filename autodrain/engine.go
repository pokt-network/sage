// Package autodrain drains an operator that the pool-collapse fallback keeps
// feeding while it answers nothing, when the pool has another operator that
// reputation vouches for.
//
// A score is per key and ranks keys; at 0 it has nothing lower to say. What it
// cannot represent is the relation across operators: among keys that are all
// below the floor, this operator answers none of the requests the collapse
// guard sends it, and a different operator exists that the pool vouches for.
// The engine measures that and holds one power, time-bounded exclusion through
// drain.Store — the same power a manual drain holds. It records no reputation
// signal. docs/auto-drain.md is the design; the numbers below are its §3–§5.
package autodrain

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/reputation"
)

// ReasonPrefix marks a drain the engine set. The engine never sets, extends
// or releases a drain whose reason lacks it.
const ReasonPrefix = "auto:"

const (
	windowSlots = 10 // one-minute slots: a 10-minute sliding window
	tickEvery   = time.Minute

	// Trigger, all required (docs/auto-drain.md §3).
	minCollapse = 20   // collapse picks for the pool in the window
	minShare    = 0.30 // of them landing on this operator
	minAttempts = 50   // traffic attempts on this operator in the window
	maxSuccess  = 0.02 // its success rate at or below this

	drainFor      = 2 * time.Hour
	maxLivePool   = 1 // live auto drains per (service, RPC type)
	maxLiveFleet  = 5
	serviceGap    = 30 * time.Minute // between new auto drains on one service
	maxPerHour    = 3                // new auto drains per hour, fleet-wide
	suppressFor   = 6 * time.Hour    // after a person releases an auto drain
	repeatEventAt = 10 * time.Minute // same key and outcome recorded at most this often
)

// Outcomes, a closed set used as a metric label and in events.
const (
	OutcomeDrained     = "drained"
	OutcomeShadow      = "shadow"
	OutcomeSuppressed  = "suppressed"
	OutcomeRateLimited = "rate_limited"
	OutcomeCapped      = "capped"
	OutcomeNoVouched   = "no_vouched_alternative"
	OutcomeManual      = "manual_drain"
)

// Event is one decision and the evidence behind it.
type Event struct {
	At            time.Time        `json:"at"`
	ServiceID     domain.ServiceID `json:"service_id"`
	RPCType       domain.RPCType   `json:"rpc_type"`
	Operator      string           `json:"operator"`
	Outcome       string           `json:"outcome"`
	CollapsePicks int              `json:"collapse_picks"`
	Share         float64          `json:"share"`
	Attempts      int              `json:"attempts"`
	SuccessRate   float64          `json:"success_rate"`
	// Alternative is the other operator reputation vouches for, when found.
	Alternative string     `json:"vouched_alternative,omitempty"`
	Until       *time.Time `json:"until,omitempty"`
}

// EndpointProvider lists a service's current endpoints for one RPC type.
// protocol.EndpointProvider satisfies it; drained endpoints are already gone.
type EndpointProvider interface {
	AvailableEndpoints(ctx context.Context, serviceID domain.ServiceID, rpcType domain.RPCType) (domain.EndpointAddrList, error)
}

// Voucher answers whether reputation vouches for an endpoint.
type Voucher interface {
	Vouched(ctx context.Context, serviceID domain.ServiceID, endpoint domain.EndpointAddr, rpcType domain.RPCType) bool
}

// Recorder counts decisions. metrics.Recorder satisfies it; nil disables.
type Recorder interface {
	RecordAutoDrain(serviceID domain.ServiceID, rpcType, outcome string)
}

// Deps is what the engine reads and writes.
type Deps struct {
	Drains    drain.Store
	Endpoints EndpointProvider
	Vouch     Voucher
	Flags     featureflag.FlagStore
	Events    EventLog
	Recorder  Recorder
	// IsLeader gates evaluation and counting: only one instance acts, and
	// drains fan out through the store. Nil means always leader.
	IsLeader func() bool
	// MaxDrain caps a drain's length like the admin route's ceiling. Zero
	// means no cap below drainFor.
	MaxDrain time.Duration
	Logger   *slog.Logger
	// Now is the clock; nil means time.Now. Tests set it.
	Now func() time.Time
}

type poolKey struct {
	svc domain.ServiceID
	rpc domain.RPCType
}

type opCount struct{ picks, attempts, successes int }

type slot struct {
	minute   int64
	collapse int
	ops      map[string]*opCount
}

type pool struct{ slots [windowSlots]slot }

type emitted struct {
	outcome string
	at      time.Time
}

// Engine is the auto-drain engine. Feed it through OnCollapse and OnSignal
// (the reputation hooks) and run it with Start.
type Engine struct {
	d        Deps
	counting atomic.Bool

	// ponytail: one mutex over every counter, taken per reputation signal;
	// shard by pool if it ever shows in a profile.
	mu    sync.Mutex
	pools map[poolKey]*pool

	// Evaluation state, touched only from Evaluate.
	live       map[drain.Key]time.Time // auto drains this instance set, until
	suppressed map[drain.Key]time.Time
	lastBySvc  map[domain.ServiceID]time.Time
	recent     []time.Time
	lastEmit   map[drain.Key]emitted
}

// New builds an engine.
func New(d Deps) *Engine {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.IsLeader == nil {
		d.IsLeader = func() bool { return true }
	}
	e := &Engine{
		d:          d,
		pools:      make(map[poolKey]*pool),
		live:       make(map[drain.Key]time.Time),
		suppressed: make(map[drain.Key]time.Time),
		lastBySvc:  make(map[domain.ServiceID]time.Time),
		lastEmit:   make(map[drain.Key]emitted),
	}
	e.counting.Store(d.IsLeader())
	return e
}

// Start runs Evaluate once a minute until ctx is done.
func (e *Engine) Start(ctx context.Context) {
	safego.GoCtx(ctx, e.d.Logger, "autodrain", func(ctx context.Context) {
		t := time.NewTicker(tickEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				safego.Run(e.d.Logger, "autodrain.tick", func() { e.Evaluate(ctx) })
			}
		}
	})
}

// OnCollapse is the reputation collapse hook.
func (e *Engine) OnCollapse(svc domain.ServiceID, rpc domain.RPCType, served domain.EndpointAddrList) {
	if !e.counting.Load() || len(served) == 0 {
		return
	}
	now := e.d.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.slot(poolKey{svc, rpc}, now)
	s.collapse++
	var seen [4]string // served is one endpoint from Select, a few ties from TopTierCandidates
	n := 0
	for _, ep := range served {
		op := ep.Operator()
		dup := false
		for _, o := range seen[:n] {
			dup = dup || o == op
		}
		if dup {
			continue
		}
		if n < len(seen) {
			seen[n] = op
			n++
		}
		s.op(op).picks++
	}
}

// OnSignal is the reputation signal hook. Probes are not traffic: a drained
// endpoint is not probed either, so only relays count.
func (e *Engine) OnSignal(svc domain.ServiceID, rpc domain.RPCType, ep domain.EndpointAddr, st reputation.SignalType, probe bool) {
	if probe || !e.counting.Load() {
		return
	}
	now := e.d.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.slot(poolKey{svc, rpc}, now).op(ep.Operator())
	c.attempts++
	if st == reputation.SignalSuccess {
		c.successes++
	}
}

func (e *Engine) slot(k poolKey, now time.Time) *slot {
	p := e.pools[k]
	if p == nil {
		p = &pool{}
		e.pools[k] = p
	}
	m := now.Unix() / 60
	s := &p.slots[m%windowSlots]
	if s.minute != m {
		*s = slot{minute: m}
	}
	return s
}

func (s *slot) op(name string) *opCount {
	if s.ops == nil {
		s.ops = make(map[string]*opCount)
	}
	c := s.ops[name]
	if c == nil {
		c = &opCount{}
		s.ops[name] = c
	}
	return c
}

type candidate struct {
	key   drain.Key
	event Event
}

// window sums every pool's live slots and returns the operators that meet the
// trigger's traffic conditions (§3, conditions 1–3).
func (e *Engine) window(now time.Time) []candidate {
	e.mu.Lock()
	defer e.mu.Unlock()
	oldest := now.Unix()/60 - windowSlots + 1
	var out []candidate
	for k, p := range e.pools {
		collapse := 0
		ops := map[string]opCount{}
		for i := range p.slots {
			s := &p.slots[i]
			if s.minute < oldest {
				continue
			}
			collapse += s.collapse
			for name, c := range s.ops {
				t := ops[name]
				t.picks += c.picks
				t.attempts += c.attempts
				t.successes += c.successes
				ops[name] = t
			}
		}
		if collapse < minCollapse {
			continue
		}
		for name, c := range ops {
			share := float64(c.picks) / float64(collapse)
			if share < minShare || c.attempts < minAttempts {
				continue
			}
			rate := float64(c.successes) / float64(c.attempts)
			if rate > maxSuccess {
				continue
			}
			out = append(out, candidate{
				key: drain.Key{ServiceID: k.svc, Operator: name, RPCType: k.rpc},
				event: Event{
					ServiceID: k.svc, RPCType: k.rpc, Operator: name,
					CollapsePicks: collapse, Share: share, Attempts: c.attempts, SuccessRate: rate,
				},
			})
		}
	}
	return out
}

// Evaluate runs one decision pass. Exported for tests; Start calls it.
func (e *Engine) Evaluate(ctx context.Context) {
	leader := e.d.IsLeader()
	e.counting.Store(leader)
	if !leader {
		return
	}
	now := e.d.Now()
	e.reconcile(ctx, now)
	for _, c := range e.window(now) {
		svc := c.key.ServiceID
		act := e.d.Flags.IsEnabled(ctx, featureflag.FlagAutoDrain, svc)
		shadow := e.d.Flags.IsEnabled(ctx, featureflag.FlagAutoDrainShadow, svc)
		if !act && !shadow {
			continue
		}
		ev := c.event
		ev.At = now
		ev.Outcome = e.decide(ctx, c.key, now, act && !shadow, &ev)
		if ev.Outcome != "" {
			e.emit(ctx, c.key, ev)
		}
	}
}

// reconcile forgets expired auto drains and notices the ones a person
// released early (suppression) or took over (a reason without the prefix).
func (e *Engine) reconcile(ctx context.Context, now time.Time) {
	for k, until := range e.live {
		if !now.Before(until) {
			delete(e.live, k)
			continue
		}
		entry, ok := e.find(ctx, k)
		switch {
		case !ok:
			e.suppressed[k] = now.Add(suppressFor)
			delete(e.live, k)
		case !strings.HasPrefix(entry.Reason, ReasonPrefix):
			delete(e.live, k)
		}
	}
	for k, until := range e.suppressed {
		if !now.Before(until) {
			delete(e.suppressed, k)
		}
	}
	cut := now.Add(-time.Hour)
	kept := e.recent[:0]
	for _, t := range e.recent {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	e.recent = kept
}

// find returns the live drain covering k: the scoped entry, or an unscoped
// one for the operator.
func (e *Engine) find(ctx context.Context, k drain.Key) (drain.Entry, bool) {
	for _, en := range e.d.Drains.Active(ctx, k.ServiceID) {
		if strings.EqualFold(en.Operator, k.Operator) && (en.RPCType == k.RPCType || en.RPCType == "") {
			return en, true
		}
	}
	return drain.Entry{}, false
}

// decide returns the outcome for one candidate, or "" when there is nothing
// to say (the engine's own drain is already live).
func (e *Engine) decide(ctx context.Context, k drain.Key, now time.Time, act bool, ev *Event) string {
	if entry, ok := e.find(ctx, k); ok {
		if strings.HasPrefix(entry.Reason, ReasonPrefix) {
			return ""
		}
		return OutcomeManual
	}
	eps, _ := e.d.Endpoints.AvailableEndpoints(ctx, k.ServiceID, k.RPCType)
	for _, ep := range eps {
		if op := ep.Operator(); !strings.EqualFold(op, k.Operator) && e.d.Vouch.Vouched(ctx, k.ServiceID, ep, k.RPCType) {
			ev.Alternative = op
			break
		}
	}
	if ev.Alternative == "" {
		return OutcomeNoVouched
	}
	if until, ok := e.suppressed[k]; ok && now.Before(until) {
		return OutcomeSuppressed
	}
	if e.liveAuto(ctx, k.ServiceID, k.RPCType) >= maxLivePool || e.liveAutoFleet(ctx) >= maxLiveFleet {
		return OutcomeCapped
	}
	if last, ok := e.lastBySvc[k.ServiceID]; ok && now.Sub(last) < serviceGap || len(e.recent) >= maxPerHour {
		return OutcomeRateLimited
	}
	if !act {
		return OutcomeShadow
	}
	d := drainFor
	if e.d.MaxDrain > 0 && e.d.MaxDrain < d {
		d = e.d.MaxDrain
	}
	until := now.Add(d)
	reason := fmt.Sprintf("%s %d collapse picks (%.0f%% on this operator), success %.1f%% over %d attempts; vouched alternative %s",
		ReasonPrefix, ev.CollapsePicks, 100*ev.Share, 100*ev.SuccessRate, ev.Attempts, ev.Alternative)
	if err := e.d.Drains.Set(ctx, drain.Entry{Key: k, Until: until, Reason: reason}); err != nil {
		e.d.Logger.Warn("autodrain: setting drain failed", "service_id", k.ServiceID, "operator", k.Operator, "rpc_type", k.RPCType, "error", err)
		return ""
	}
	e.live[k] = until
	e.lastBySvc[k.ServiceID] = now
	e.recent = append(e.recent, now)
	ev.Until = &until
	return OutcomeDrained
}

func (e *Engine) liveAuto(ctx context.Context, svc domain.ServiceID, rpc domain.RPCType) int {
	n := 0
	for _, en := range e.d.Drains.Active(ctx, svc) {
		if en.RPCType == rpc && strings.HasPrefix(en.Reason, ReasonPrefix) {
			n++
		}
	}
	return n
}

func (e *Engine) liveAutoFleet(ctx context.Context) int {
	e.mu.Lock()
	svcs := map[domain.ServiceID]bool{}
	for k := range e.pools {
		svcs[k.svc] = true
	}
	e.mu.Unlock()
	n := 0
	for svc := range svcs {
		for _, en := range e.d.Drains.Active(ctx, svc) {
			if strings.HasPrefix(en.Reason, ReasonPrefix) {
				n++
			}
		}
	}
	return n
}

// emit records a decision once per change of outcome, and repeats an
// unchanged one at most every repeatEventAt: a shadow decision holds for as
// long as the condition does, and one event a minute says nothing new.
func (e *Engine) emit(ctx context.Context, k drain.Key, ev Event) {
	last, ok := e.lastEmit[k]
	if ok && last.outcome == ev.Outcome && ev.At.Sub(last.at) < repeatEventAt && ev.Outcome != OutcomeDrained {
		return
	}
	e.lastEmit[k] = emitted{outcome: ev.Outcome, at: ev.At}
	if e.d.Recorder != nil {
		e.d.Recorder.RecordAutoDrain(ev.ServiceID, string(ev.RPCType), ev.Outcome)
	}
	if e.d.Events != nil {
		if err := e.d.Events.Append(ctx, ev); err != nil {
			e.d.Logger.Debug("autodrain: event not recorded", "error", err)
		}
	}
	level := slog.LevelWarn
	if ev.Outcome == OutcomeShadow {
		level = slog.LevelInfo
	}
	e.d.Logger.Log(ctx, level, "autodrain: decision",
		"outcome", ev.Outcome, "service_id", ev.ServiceID, "rpc_type", ev.RPCType, "operator", ev.Operator,
		"collapse_picks", ev.CollapsePicks, "share", ev.Share, "attempts", ev.Attempts,
		"success_rate", ev.SuccessRate, "vouched_alternative", ev.Alternative)
}
