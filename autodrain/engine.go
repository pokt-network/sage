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
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/relay"
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

	// The client gate. Collapse evidence says the fallback keeps feeding an
	// operator that answers nothing; it does not say a caller noticed, because
	// retry usually rescues the request on another operator. Of 44 shadow
	// proposals over 2026-09-15/16 not one sat on a service whose clients were
	// failing above 2.2%, and each was on a service SAGE was already serving
	// better than PATH; every drain a person actually made sat above 5%. A
	// proposal below the bar is recorded and never acted on.
	minClientFailure  = 0.05
	minClientRequests = 50

	// The severity path past the client gate. Retry and hedge keep an
	// operator's failures off the client-facing error rate, not off the
	// client: each failed attempt is a paid relay and a slice of the caller's
	// deadline, and enough of them still end in a 504. On mainnet base
	// (2026-09-26) one owner's two operators answered 51% of their attempts
	// while clients saw 4.8% failures; the gate stood the engine down on
	// about fifteen services while base served ~6 client 504s a second, all of
	// them that owner's. An operator failing this badly on this much evidence
	// is an incident whatever the error rate shows, so the gate yields to it.
	// Every other guard — a vouched alternative, the caps, the rate limits,
	// suppression — still applies.
	severeMaxSuccess  = 0.60
	severeMinAttempts = 200

	// opRateTrigger is the second way in, for the case collapse share cannot
	// see: an operator whose corrected chronic rate is this high over
	// minAttempts attempts, however its picks are spread. An operator holding
	// ~90 keys of one service dilutes its per-key rate below every threshold
	// and never concentrates the fallback — mainnet sei, drained by hand three
	// times in two days while the engine proposed nothing (reputation/operator.go).
	opRateTrigger = 0.03

	// The third way in, for the operator scoring counts as perfect: one whose
	// first attempts are answered with a chain error (a JSON-RPC error the
	// heuristic passes through as the chain's answer, scored a success) far
	// more often than the pool's other operators' are. A chain error depends
	// on the request, and every operator in a pool draws from the same
	// requests, so a share this far above its peers is the operator, not the
	// chain. On mainnet solana (2026-10-04) one operator's middleware answered
	// 100% of its first attempts with -32603 against 4% for its peers, scored
	// 100 throughout, and about 42% of the service's client requests got the
	// error for at least two days; on hyperliquid another answered 27% with
	// -32603 against 0% for fourteen hours, about 18% of client requests.
	// Over 60 hours of mainnet first attempts every pool where all operators
	// answer many chain errors alike (akash at 45-75%) stayed below the ratio,
	// and every outlier that reached the client bar below was one of those
	// incidents or a smaller one of the same kind.
	chainGap   = 0.20 // its chain-answer share at least this far above its peers'
	chainRatio = 3.0  // and at least this many times theirs

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
	// OutcomeBelowClient is a candidate the client gate stopped: the operator
	// meets the trigger, the callers of that service are not failing.
	OutcomeBelowClient = "below_client_failure"
	// OutcomeNoClientEvidence is the other half of the gate: too few
	// client-facing answers in the window to say anything either way. It is a
	// separate outcome because "nobody is failing" and "nobody asked" are
	// different facts, and a chain at 0.1 requests/second hits the second one
	// while 98% of its callers fail (mainnet poly-zkevm, 2026-09-16).
	OutcomeNoClientEvidence = "no_client_evidence"
)

// What put a candidate in front of the decision, recorded on the event.
const (
	TriggerCollapse     = "collapse"
	TriggerOperatorRate = "operator_rate"
	TriggerChainAnswers = "chain_answers"
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
	// Trigger is which condition raised this candidate (collapse share or the
	// operator's chronic rate).
	Trigger string `json:"trigger,omitempty"`
	// OperatorRate is the operator's corrected chronic failure rate in the pool.
	OperatorRate float64 `json:"operator_rate,omitempty"`
	// ClientFailure is the share of the service's client-facing answers that
	// failed over the same window, and ClientRequests how many there were: the
	// evidence the gate reads.
	ClientFailure  float64 `json:"client_failure"`
	ClientRequests int     `json:"client_requests"`
	// Severe marks an operator failing badly enough (severeMaxSuccess over
	// severeMinAttempts) that the client gate did not apply to it.
	Severe bool `json:"severe,omitempty"`
	// FirstAttempts is how many first and probation attempts the operator
	// answered in the window, the sample the chain-answer shares are read on.
	FirstAttempts int `json:"first_attempts,omitempty"`
	// ChainShare is the share of them answered with a chain error, and
	// PeerChainShare the same share over the pool's other operators.
	ChainShare     float64 `json:"chain_share,omitempty"`
	PeerChainShare float64 `json:"peer_chain_share,omitempty"`
	// AnswerHarm is the chain errors the operator answered beyond its peers'
	// share, per client request of the service in the window: errors callers
	// received inside a 200, which ClientFailure cannot see. Set only when the
	// share is an outlier (chainGap, chainRatio). It overstates a little: a
	// chain error the heuristic retries is not delivered.
	AnswerHarm float64 `json:"answer_harm,omitempty"`
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

// OperatorRates reports an operator's chronic failure rate, from counters kept
// per operator in a pool. reputation's service satisfies it; nil leaves the engine on
// collapse evidence alone.
type OperatorRates interface {
	OperatorRate(serviceID domain.ServiceID, rpcType domain.RPCType, operator string) (reputation.OperatorRateView, bool)
}

// Deps is what the engine reads and writes.
type Deps struct {
	Drains    drain.Store
	Endpoints EndpointProvider
	Vouch     Voucher
	Flags     featureflag.FlagStore
	Events    EventLog
	Recorder  Recorder
	// Rates is the per-operator chronic rate, the second trigger's input. Nil
	// leaves the engine on collapse evidence alone.
	Rates OperatorRates
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

// opCount is one operator's minute. picks, attempts and successes come from
// reputation (the collapse and signal hooks); firsts, chain come from the
// attempt hook, first and probation attempts only.
type opCount struct{ picks, attempts, successes, firsts, chain int }

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
	mu      sync.Mutex
	pools   map[poolKey]*pool
	clients map[domain.ServiceID]*clientWindow

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
		clients:    make(map[domain.ServiceID]*clientWindow),
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

// OnAttempt is the metrics recorder's attempt hook. It counts only first and
// probation attempts: the fair sample of how an operator answers. A retry or a
// hedge arm reaches whoever is left with whatever budget is left, carrying the
// requests other operators already failed. A client-attributed attempt is
// nobody's and is not counted.
func (e *Engine) OnAttempt(svc domain.ServiceID, rpc domain.RPCType, ep domain.EndpointAddr, attribution, kind string) {
	if !e.counting.Load() || (kind != relay.AttemptFirst && kind != relay.AttemptProbation) ||
		attribution == heuristic.AttrClient.String() {
		return
	}
	now := e.d.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.slot(poolKey{svc, rpc}, now).op(ep.Operator())
	c.firsts++
	if attribution == heuristic.AttrBlockchain.String() {
		c.chain++
	}
}

// chainOutlier reports whether an operator's chain-answer share stands out
// from its peers' (chainGap, chainRatio), each side over minAttempts first
// attempts, and returns both shares.
func chainOutlier(c opCount, poolFirsts, poolChain int) (share, peers float64, ok bool) {
	peerFirsts := poolFirsts - c.firsts
	if c.firsts < minAttempts || peerFirsts < minAttempts {
		return 0, 0, false
	}
	share = float64(c.chain) / float64(c.firsts)
	peers = float64(poolChain-c.chain) / float64(peerFirsts)
	return share, peers, share-peers >= chainGap && share >= chainRatio*peers
}

// clientSlot is one minute of client-facing answers for a service.
type clientSlot struct {
	minute        int64
	total, failed int
}

type clientWindow struct{ slots [windowSlots]clientSlot }

// OnClientResult counts one client-facing answer. This is the gate's evidence
// and it is deliberately not the relay counters: retry and hedge mean an
// operator can answer nothing while every caller of that service is served.
func (e *Engine) OnClientResult(svc domain.ServiceID, status int) {
	if svc == "" || !e.counting.Load() {
		return
	}
	now := e.d.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.clients[svc]
	if w == nil {
		w = &clientWindow{}
		e.clients[svc] = w
	}
	m := now.Unix() / 60
	s := &w.slots[m%windowSlots]
	if s.minute != m {
		*s = clientSlot{minute: m}
	}
	s.total++
	if clientFailed(status) {
		s.failed++
	}
}

// clientFailed is the client-facing failure bucket: every 5xx, plus the two
// statuses SAGE returns for an upstream that timed out or rate-limited it. A
// JSON-RPC error inside a 200 is the chain answering, not a failure.
func clientFailed(status int) bool {
	return status >= 500 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests
}

// clientShare sums a service's live client slots. Caller holds e.mu.
func (e *Engine) clientShare(svc domain.ServiceID, oldest int64) (share float64, requests int) {
	w := e.clients[svc]
	if w == nil {
		return 0, 0
	}
	failed := 0
	for i := range w.slots {
		s := &w.slots[i]
		if s.minute < oldest {
			continue
		}
		requests += s.total
		failed += s.failed
	}
	if requests == 0 {
		return 0, 0
	}
	return float64(failed) / float64(requests), requests
}

// opRate is the operator's corrected chronic rate in the pool, 0 when unknown.
func (e *Engine) opRate(k poolKey, operator string) float64 {
	if e.d.Rates == nil {
		return 0
	}
	r, ok := e.d.Rates.OperatorRate(k.svc, k.rpc, operator)
	if !ok {
		return 0
	}
	return r.Rate
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
		collapse, poolFirsts, poolChain := 0, 0, 0
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
				t.firsts += c.firsts
				t.chain += c.chain
				ops[name] = t
				poolFirsts += c.firsts
				poolChain += c.chain
			}
		}
		cshare, creq := e.clientShare(k.svc, oldest)
		for name, c := range ops {
			share, success, opRate := 0.0, 0.0, 0.0
			if collapse > 0 {
				share = float64(c.picks) / float64(collapse)
			}
			if c.attempts > 0 {
				success = float64(c.successes) / float64(c.attempts)
			}
			if c.attempts >= minAttempts {
				opRate = e.opRate(k, name)
			}
			chainShare, peerChain, outlier := chainOutlier(c, poolFirsts, poolChain)
			harm := 0.0
			if outlier && creq > 0 {
				harm = (float64(c.chain) - peerChain*float64(c.firsts)) / float64(creq)
			}
			trigger := ""
			switch {
			case c.attempts >= minAttempts && collapse >= minCollapse && share >= minShare && success <= maxSuccess:
				trigger = TriggerCollapse
			case outlier:
				trigger = TriggerChainAnswers
			case c.attempts >= minAttempts && opRate >= opRateTrigger:
				trigger = TriggerOperatorRate
			default:
				continue
			}
			out = append(out, candidate{
				key: drain.Key{ServiceID: k.svc, Operator: name, RPCType: k.rpc},
				event: Event{
					ServiceID: k.svc, RPCType: k.rpc, Operator: name,
					CollapsePicks: collapse, Share: share, Attempts: c.attempts, SuccessRate: success,
					Trigger: trigger, OperatorRate: opRate,
					ClientFailure: cshare, ClientRequests: creq,
					FirstAttempts: c.firsts, ChainShare: chainShare, PeerChainShare: peerChain, AnswerHarm: harm,
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
	// The client gate: an operator answering nothing while every caller of the
	// service is served is a routing inefficiency, not an incident, and a drain
	// buys nothing a retry is not already buying.
	if ev.ClientRequests < minClientRequests {
		return OutcomeNoClientEvidence
	}
	ev.Severe = ev.Attempts >= severeMinAttempts && ev.SuccessRate <= severeMaxSuccess
	// Chain errors an outlier delivered inside a 200 hurt callers as much as a
	// 5xx does, and the client-facing status never shows them.
	if ev.ClientFailure < minClientFailure && ev.AnswerHarm < minClientFailure && !ev.Severe {
		return OutcomeBelowClient
	}
	eps, _ := e.d.Endpoints.AvailableEndpoints(ctx, k.ServiceID, k.RPCType)
	// The alternative must be a different provider, not another brand of the
	// same owner: an operator's affiliates are its own endpoints' operator
	// and owners.
	var drained domain.Affiliates
	for _, ep := range eps {
		if strings.EqualFold(ep.Operator(), k.Operator) {
			drained.Add(ep)
		}
	}
	for _, ep := range eps {
		if op := ep.Operator(); !strings.EqualFold(op, k.Operator) && !drained.Contains(ep) && e.d.Vouch.Vouched(ctx, k.ServiceID, ep, k.RPCType) {
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
	if ev.Trigger == TriggerChainAnswers {
		reason = fmt.Sprintf("%s %.0f%% of %d first attempts answered with a chain error against %.0f%% for the other operators, %.1f%% of client requests; vouched alternative %s",
			ReasonPrefix, 100*ev.ChainShare, ev.FirstAttempts, 100*ev.PeerChainShare, 100*ev.AnswerHarm, ev.Alternative)
	}
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
	switch ev.Outcome {
	case OutcomeShadow, OutcomeBelowClient, OutcomeNoClientEvidence:
		level = slog.LevelInfo
	}
	e.d.Logger.Log(ctx, level, "autodrain: decision",
		"outcome", ev.Outcome, "service_id", ev.ServiceID, "rpc_type", ev.RPCType, "operator", ev.Operator,
		"trigger", ev.Trigger, "collapse_picks", ev.CollapsePicks, "share", ev.Share, "attempts", ev.Attempts,
		"success_rate", ev.SuccessRate, "operator_rate", ev.OperatorRate,
		"client_failure", ev.ClientFailure, "client_requests", ev.ClientRequests,
		"chain_share", ev.ChainShare, "peer_chain_share", ev.PeerChainShare, "answer_harm", ev.AnswerHarm,
		"vouched_alternative", ev.Alternative)
}
