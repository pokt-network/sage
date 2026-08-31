# Reputation scoring — design

Status: **decided 2026-08-27, implemented**. §1–§2 record what the code did at
the 2026-08-21 audit, with the places the implementation has since moved marked
inline; §3–§6 the model; §7 the decisions and the data they rest on. The
implementation contract is
`docs/design/specs/2026-08-27-scoring-v2-design.md`.

`ARCHITECTURE.md` describes reputation as the thing that learns from relays and
steers selection away from bad endpoints. That is the intent. This document
records what the implementation currently does instead, the principles the
replacement should hold to, and the model proposed for it.

## 1. What scores today

Four things produce `reputation.Signal`s:

| Producer | Where | Cadence |
|---|---|---|
| Relay outcomes | `relay/middleware/score.go` | once per **relay attempt** (with `scoring_v2` on; with it off, `observe.go` instead, once per client request) |
| Health-check probes | `healthcheck/executor.go` | once per probe, fanned out to every sibling on the same backend URL |
| WebSocket lifecycle | `protocol/shannon/ws_relayer.go` | on connect failure, on close |
| Configured checks | `healthcheck/configured.go` (`SignalFor`) | overrides the grade of a named check |

A signal carries a type, a latency and a free-text reason. Only the type has
any effect: `reputation/signals.go` maps it to a score delta (`+5` success,
`-3` minor, `-10` major, `-25` critical, `-50` fatal), the service clamps the
running score to `[0, 100]`, and `reputation.TieredSelector` buckets endpoints
by that score. The reason string is never matched on — which is worth stating
explicitly, because PATH's classifier compared reason strings with exact
equality and a `(method=…)` suffix silently disabled six of its branches.

Relay severity comes from `heuristic.Analyze`, which the `heuristic`
middleware runs inside `send_relay` and stores on `ctx.HeuristicResult`. Two
middlewares read that field: `circuit_break`, which reports per-attempt outcomes
to the breaker, and `observe`, which converts it into the signal. (A third,
`score`, reads it now — that is the change §2 argues for.)

The chain positions are what make the difference between those two:

```
… cache → batch → singleflight → observe → cross_validate → retry → hedge
    → supplier_affinity → circuit_break → select_endpoint → score → heuristic
    → send_relay
```

`circuit_break` sits **inside** `retry`/`hedge`, so it sees every attempt.
`observe` sits **outside** both, so it sees one outcome per client request — and
it sits **inside** `batch`, so it sees one per payload. That position is the
cause of §2.1, §2.3 and §2.4; `score` is the middleware added to fix it, and it sits
inside `retry`/`hedge` beside `circuit_break` for exactly the reason
`circuit_break` does.

## 2. What is wrong

Each of these was reproduced with a probe chain rather than derived from
reading, because three of them are invisible in the code and only appear in the
composition.

**2.1 Retry losers are never scored.** Endpoint A returns an empty body
(critical), the request is retried, endpoint B answers correctly. The only
signal recorded is `success` for B. A loses nothing. An endpoint that fails a
third of the time looks perfect as long as retries rescue the request, and the
requests it fails are exactly the ones that generate no evidence against it.

**2.2 A stale heuristic result charges the wrong endpoint.** `retry` clears
`ctx.Response` and `ctx.Err` between attempts but not `ctx.HeuristicResult`. If
attempt 1 on A returns an empty body and attempt 2 on B dies at the transport
(so the `heuristic` middleware returns before setting a fresh result), the one
signal recorded is `critical_error{reason: empty_response}` **against B**. B is
docked 25 points for A's misbehaviour, and A is again docked nothing.

**2.3 Hedging erases severity.** `hedge.mergeContext` copies `Endpoint`,
`Response`, `Err`, `Degraded`, `Cached` and `Coalesced` from the winning arm —
not `HeuristicResult`. The outer context therefore reaches `observe` with a nil
result, which falls back to "was there an error / was the status 2xx". A
fabricated response (both `result` and `error` present, severity fatal, `-50`)
scores as `minor_error`, `-3`.

**2.4 Batch multiplies the weight of one request.** Because `observe` is inside
`batch`, a 50-payload request writes 50 signals. One bad moment on a backend
costs an endpoint `50 × -25`; one good moment earns `50 × +5`. Whoever sends
batches decides how fast scores move, which is a property of the caller, not of
the endpoint.

**2.5 Four of nine signal types have no producer.** `slow_response`,
`very_slow_response`, `stale_block` and `recovery_success` are constructed
nowhere outside tests. Every signal carries a `Latency` that nothing reads, so
**latency does not affect reputation at all** — a correct endpoint at 4 seconds
ranks with a correct endpoint at 40 milliseconds.

The compound effect of 2.1–2.4 is that health-check probes are the only
consistent grader of an endpoint, and user traffic contributes mostly noise —
occasionally attributed to the wrong endpoint. That is the inverse of PATH's
2026-08-20 incident, where probe-derived signals fed a bench that user traffic
never justified: the same coupling read from the other end.

## 3. Principles

These are the rules the replacement should be checkable against. They exist
mostly as reactions to specific failures, in PATH or here.

1. **One attempt is one scoring event.** Not one request, not one payload. The
   endpoint that produced a response is the unit of evidence.
2. **Attribution follows the response, never the request.** A signal must be
   derived from state produced by the attempt it is charged to. Defect 2.2 is
   what happens when that is left to field lifetimes.
3. **Exclusion and penalty are separate powers, and the same fact must not
   trigger both.** Block-height staleness already excludes an endpoint at
   selection (`qos.BlockHeightFilter`); also scoring it would penalise an
   endpoint for a condition that already cost it the traffic. This is the rule
   PATH broke by accumulating a strike system, a critical-rate detector, an
   invalid-rate detector and a circuit breaker that all fired on overlapping
   events with independent state.
4. **Volume must not decide weight.** A batch of 50 is one client's request. A
   supplier that happens to serve batch-heavy traffic must not accumulate score
   fifty times faster in either direction.
5. **Probes and traffic must stay distinguishable.** Not because probes should
   be ignored — they are how a benched endpoint recovers when it receives no
   traffic — but because "is this endpoint graded by anything real?" has to be
   answerable. PATH could not answer it, and ten services tripped a detector
   sized for one.
6. **A signal type with no producer does not exist.** Either wire it or delete
   it; a constructor nobody calls reads as a capability the system has.

## 4. Proposed model

### 4.1 Move signal recording to the attempt boundary

Split today's `observe` middleware in two:

- `observe` keeps the async observation submission, which is genuinely
  per-request (it feeds block heights and archival inference off the hot path,
  sampled) and stays where it is.
- A new `score` middleware records reputation signals. It registers next to
  `circuit_break` — inside `retry`, `hedge` and `batch`, outside
  `select_endpoint` — which is the position that already gives `circuit_break`
  a per-attempt view with `ctx.Endpoint` populated.

`ValidateChainOrder` gains a `mustPrecede` rule (`retry`/`hedge` must precede
`score`) so the position is enforced rather than conventional. An unregistered
name gets no ordering protection; see `docs/middleware.md`.

This alone fixes 2.1 and 2.3: each attempt scores its own endpoint from its own
result, and nothing depends on what survives a merge.

### 4.2 Give `HeuristicResult` a lifetime

`score` consumes the result at the attempt boundary and clears it. `retry`
clearing it alongside `Response`/`Err` is the belt-and-braces version and should
also happen, but the load-bearing change is that no code path can read a result
produced by a different attempt. Fixes 2.2.

### 4.3 Collapse a batch to one signal per endpoint

Batch sub-relays run on shallow clones, so a pointer field on `relay.Context` is
shared across the whole request tree — which is normally a hazard (see the
`Clone()` warning in `CLAUDE.md`) and here is exactly the mechanism needed. The
`score` middleware writes into a shared per-request aggregator, guarded by a
mutex, and one signal per `(endpoint, rpc_type)` is emitted when the parent
request completes, carrying the **worst** severity observed for that endpoint.

Worst-of, over **supplier-attributed** outcomes only (decided, §7.1). A single
fabricated response inside a batch is still a fabricated response, and averaging
it away is how a bad endpoint hides in bulk traffic. The attribution filter is
what the data forced: on beta 66% of mixed batches contain an item error, and
every one of them was `-32601` — the client asked for a method the chain does
not have. Worst-of over raw outcomes would have scored two thirds of all batch
traffic as a supplier fault.

### 4.4 Decide what latency does

Decided: option 3, delete the two signal types, and give latency a reporting
role only (§7.2). The three options as they were weighed:

1. **Score it relative to the service, not absolutely.** Emit `slow_response` /
   `very_slow_response` when an attempt exceeds a multiple of the service's
   rolling median latency. No magic millisecond constant, adapts to chains that
   are simply slow, and the multiple is one number to defend.
2. **Score it against a configured threshold** per service (`slow_after`,
   `very_slow_after`), zero meaning off. Simpler, but every service needs a
   number chosen by hand and wrong numbers are invisible.
3. **Delete the two signal types.** Honest, and leaves latency to selection
   (which does not use it either) or to hedging (which papers over it per
   request without ever demoting the cause).

`stale_block` should be **deleted** under principle 3 — staleness already
excludes at selection. `recovery_success` should be deleted or given a
definition; today it is a synonym for `success` with the same `+5`.

### 4.5 Recalibrate the weights, and write down the ratio

Decided (§7.3): the additive constants stay, the ratio is stated, and a second
term — a rate term — is added to the score for the violators the additive term
provably cannot see.

`+5` success against `-25` critical means five good relays erase one critical.
That ratio was never stated as an intent; it fell out of two constants. Per-attempt
scoring changes the effective rate at which both accumulate, so the numbers have
to be re-chosen anyway. The design question to answer explicitly: **how many
consecutive good relays should it take to undo one protocol violation?** Pick
that, then derive the constants.

Related, and currently unanswerable: an endpoint that emits a structural
violation on 0.2% of its traffic holds a perfect score forever, because additive
scoring is dominated by volume. PATH's answer was a second detector with its own
EWMA, threshold, escalation counter and cooldown. **Do not copy that** — see §5.
If SAGE wants to catch a low-rate violator, it belongs in the existing score as
a term, not as a parallel mechanism with its own state.

### 4.6 Mark probe-derived signals

Add a boolean to `Signal` set by the health-check executor and by the WS
lifecycle producers. Nothing needs to gate on it on day one; what it buys is the
ability to answer "what is grading this endpoint?" and to keep a future
mechanism from being driven entirely by synthetic traffic — which is the loop
PATH measured on canary at 19.3× the control's pool-collapse rate.

### 4.7 Build it on PATH's config surface, not a new one

PATH's scoring configuration already exists in `config/service.go`, parsed,
documented, and read by nothing: `signal_impacts` (the score delta per signal
type), `latency_profiles` (fast/slow/severe thresholds with a bonus and two
penalties, per chain), `tiered_selection` (tier thresholds plus probation),
`min_threshold`, `recovery_timeout`. They are reported at startup as inert (see
`docs/path-compat.md`), which is honest, but the better answer for most of them
is to make them true.

Doing the rework through those keys rather than inventing SAGE names has three
payoffs, and no cost that is not also a benefit:

- An operator's PATH tuning starts doing what it says. Today moving a config
  across silently drops it.
- Two of the open questions below stop being ours to answer in the abstract:
  the score deltas (§4.5) and the latency thresholds (§4.4) become configured
  values with a documented default, and the default is what we have to defend
  rather than the only possible answer.
- It forces the divergence to be where it belongs. PATH's *keys* with SAGE's
  *machine* is exactly the compatibility line in `docs/path-compat.md`: the
  operator surface matches, the mechanism does not.

The cost is that a key SAGE honours must mean what PATH means by it. Where it
cannot — `recovery_timeout` names a cooldown SAGE does not have, and should not
grow one to satisfy a key — the key stays inert and stays reported. That is the
choice this design is making explicit: **implement the surface, refuse the
mechanism**, and say so out loud in the one case where the surface implies a
mechanism we are declining.

## 5. What this deliberately does not add

No strike system, no cooldown, no second rate detector, no bench. PATH reached
five overlapping mechanisms that could each remove an endpoint from rotation,
with separate counters, separate timestamps and one shared `CooldownUntil` that
made a first offence inherit a stale escalation. Every one of them was a
reasonable local fix.

The rule that keeps that from recurring: **a new mechanism must measure
something the existing score cannot represent, and must state which power it
holds** — excluding, penalising, or reporting. If it can be expressed as a term
in the score, it is a term in the score.

## 6. Rollout

Behind a feature flag (`scoring_v2`), per-service overridable, because that is
how every other behavioural change here ships and because the comparison matters
more than the switch. Both paths can run at once — the new `score` middleware
recording, the old `observe` path recording — with the delta exported per
service, until the numbers are boring.

Revert-check every change, and make sure each test discriminates: for scoring in
particular, "no signal recorded" and "the right signal recorded" are easy to
confuse when the assertion only counts calls.

## 7. Decisions, and the data behind them

Answered 2026-08-27 from two sources, because neither alone covers both sides
of the question:

- **Beta TestNet, 12 minutes of mixed traffic through SAGE** (20,304 relay
  attempts, 156 probes): singles, batches of 5/10/20 with 15% unsupported
  methods mixed in, client timeouts of 150/250/400 ms, Cosmos REST, CometBFT
  and JSON-RPC. Beta has exactly one live backend
  (`rm.beta.infra.pocket.network`; the other two registered hosts do not
  resolve), so it yields a real healthy-endpoint latency distribution and
  probe-versus-traffic agreement, and nothing about bad-but-alive endpoints.
- **PATH's mainnet evidence** (`EVIDENCE_EMPTY_RESPONSES_2026-08-19.md`) for the
  violator side: per-domain empty-response rates over one day — spacebelt
  0.216%, rpcgate 0.065%, nodefleet 0.00003%, kleomedes ~0%. Every one of the
  bad responses landed on a 3.0 s deadline (p50 3021 ms, min 2820, max 3080).
- **Simulation** of the scoring rules against those rates, where the question
  is arithmetic rather than measurement.

### 7.1 Batch: worst-of over supplier-attributed outcomes

Beta: 1,238 batches, 812 (66%) with at least one item error, 1,459 item errors,
**all** `-32601`. Not one supplier-attributed partial failure was observed. So
worst-of and majority differ on nothing in the data, and the decision is by
principle: worst-of, because the case that separates them — one fabricated item
in twenty — is exactly the case a bad endpoint hides behind. The data's
contribution is the filter: the collapse runs over outcomes with
`Attribution == Supplier`; client- and blockchain-attributed items are ignored;
a batch with only those and at least one success scores one `success`; a batch
with only client-attributed items scores nothing.

### 7.2 Latency: no penalising power, reporting only

Two findings, one arithmetic and one empirical.

Arithmetic: under a score clamped at 100, a `-1` slow signal at any plausible
rate is invisible — the next `+5` restores it. For latency to move a score it
would have to fire often (then it is the median, not a tail) or cost as much as
an error (then a slow-but-correct endpoint is treated as a broken one). PATH's
`latency_profiles` model — multipliers on the success reward — has the same
property under the same clamp.

Empirical (beta, 16,533 healthy `pnf-anvil` attempts, median 126 ms): the
fraction of correct responses above k× the service median is 6.2% at 2×, 3.4%
at 3×, 1.1% at 5×, 0.01% at 8×. The tail of a healthy endpoint is wide and
smooth; there is no multiple that separates "slow endpoint" from "normal tail".
PATH's `fast` profile thresholds (`penalty_threshold: 1000ms`) would have fired
on 0.012% of them — a rule that never fires and whose absence is invisible.

What latency **is** in the data is a fault signature: every mainnet empty
response sat at the RelayMiner's 3 s deadline. That is evidence for the
heuristic (a body-less 200 at the transport deadline is a timeout wearing a
success envelope), not for the score.

Decision: delete `slow_response`, `very_slow_response`, `stale_block` and
`recovery_success` and their constructors (principle 6). Keep `Signal.Latency`
and maintain a per-key latency EWMA from **traffic** attempts, exported through
the admin reputation listing. Probes are excluded from it: their connections are
cold, and on beta the probe tail was 2.4× the traffic tail at p90 (500 ms vs
206 ms for `eth_blockNumber`) with the same median. `latency_profiles` and
`latency_profile` stay parsed, inert and reported at startup, and this section
is the reason.

### 7.3 The ratio, and the term the ratio cannot buy

The additive term with `+5 / -25` has a break-even failure rate of
`s / (s + c) = 16.7%`: an endpoint failing less often than one relay in six sits
at 100 forever; one failing more often drifts to 0. Simulated over 20,000
attempts, an endpoint at 15% critical is below tier 1 62% of the time, at 10%
31%, at 5% 9.5%, at 2% 2.6%, at 0.2% 0.2%. Raising the ratio does not reach the
mainnet violators: at `k = 20` (`-100` per critical) spacebelt at 0.216% is
below tier 1 2.3% of the time, and one transient error costs a perfect host 20
relays of demotion. No ratio catches a 0.2% violator without punishing a 0.001%
one.

So the ratio is **kept at 5 and stated**: the additive term is an outage
detector — it exists to remove an endpoint that has *started* failing within a
handful of attempts (a dead host at `-25` leaves tier 1 on the first critical
and hits 0 on the fourth) — and it is not asked to see anything under a 1-in-6
failure rate. `signal_impacts` becomes live so an operator can move it.

The chronic violator gets a term of its own inside the same score (§5 rule:
measures something the score cannot represent, and its power is the score's —
demotion by tier, nothing else). Per `(endpoint, rpc_type)`, an EWMA of
supplier-attributed failure per attempt (`1` for critical/fatal, `0.5` for
major, `0` otherwise; client-attributed outcomes are not attempts), half-life
20,000 attempts, mapped to a penalty that is 0 up to 0.02% and logarithmic
above it: `-40` at 1%, `-70` at 10% and beyond. Effective score is
`clamp(additive + rate_penalty)`.

Simulated at steady state (100k attempts, 10 runs): nodefleet 0; rpcgate `-11`
(tier 1, 89); spacebelt `-23` (tier 2, 77); 1% `-40` (tier 2, 60); 5% `-61`;
20% `-70` (probation). A burst of 3 criticals on a clean endpoint moves the
rate term by 0.010% — penalty 0; a burst of 6 costs `-0.4` and clears in ~1,100
clean attempts; 20 in a row cost `-13`. Detection of a spacebelt-rate violator
takes 12k–22k attempts, which at that supplier's mainnet volume (2M relays/day)
is under fifteen minutes and at 1 relay/s is six hours: the term is for chronic
behaviour and is allowed to be slow. (§7.7: in the running gateway the intake
is not the supplier's volume but the probe cycle, and the wall time is closer
to an hour.) Where the two terms disagree the additive
one is the fast path and the rate term the memory, and neither has state the
other needs.

What it does not do: a 0.065% endpoint keeps tier 1. That is the price of not
penalising a 6-error burst, and it is the trade the half-life sets. A shorter
half-life reacts faster and treats bursts as chronic; 20k is where a 6-burst
stays under the onset.

**An outage does not feed the rate.** A signal recorded against a key whose
additive term is *already* 0 updates everything except `Rate`: the attempt that
floors the key still counts, and every one after it does not. The rule follows
from principle 3 — the additive term has already removed the endpoint from
selection, and charging the same fact to the chronic term as well is the second
power the principle forbids. The arithmetic that makes it more than a
principle: a dead host answers nothing but health checks, two checks every 30
seconds (the cadence when this was measured; the chain-id check moved to every
5 minutes on 2026-08-31, which lowers the figure, not the conclusion) is 5,760
criticals a day, and 5,760 attempts at the 20k half-life take
the rate to 0.18 — the `-70` cap. Getting back to tier 1 from the cap needs the
rate to fall by a factor of ~130, seven half-lives, 140,000 attempts; at
probe-only volume that is **24 days** of the host being perfectly healthy and
still capped, because a benched endpoint receives nothing but probes. The
additive term brings the same host back in four successes, and the two terms
would then be arguing about a fact only one of them observed.

The cost is real and is accepted: a host that alternates outages with clean
stretches shows a lower chronic rate than its true failure fraction, because
the attempts it spends floored are not counted against it. What that host does
not escape is the additive term, which removes it for the duration of every
outage — the case the chronic term exists for is the endpoint that fails 0.2%
of the time and is *never* floored, and its arithmetic is untouched (a 1-in-500
violator never reaches 0, so every one of its attempts feeds the rate).

### 7.4 Probe-only endpoints: selectable at full score

Beta: probes and traffic agreed on every host — the live one passed every probe
(156/156) and served 20,304 attempts without one supplier-attributed failure;
the two dead hosts failed every probe and were never selected. Median latency
agreed within 15% (108 vs 127 ms). The data gives no case where a probe-graded
endpoint misled selection.

The cost of trusting a probe is bounded by the per-attempt model: the first bad
relay scores the endpoint itself (`-25`, out of tier 1) and Retry moves on, so a
wrong probe costs one client one retry. The cost of capping is unbounded in the
other direction: a capped endpoint gets traffic only through probation's 10%,
which on a tier-1-heavy pool is how a new supplier never earns a score — the
single-healthy-host caveat again, from the other side. No cap. `Signal.Probe`
is set by the health-check executor so the admin listing can say which keys
have never seen traffic, and so that a future mechanism can be checked against
principle 5. One probe is one attempt *per key*, whatever the registration
count: the executor hands the whole sibling set to `RecordSignalOnce`, which
records once per distinct reputation key — so at per-URL a backend fronted by
ten stakes is graded once, and at per-endpoint each of the ten is graded once
(principle 4; a probe's weight must not be set by the stake table).

### 7.5 Client cancellation: no signal, not a latency signal either

Beta, 627 requests with client timeouts of 150–400 ms against an endpoint whose
median is 126 ms: 72 cancellations, 58 of them at 150 ms. The attempts SAGE
saw cancelled had a p50 of 151 ms — the client's deadline, not the supplier's
latency. Scoring a cancel penalises the endpoint for the caller's impatience,
and the fastest endpoint takes the most traffic and therefore the most
cancels. The censored observation ("the supplier had not answered by *t*") is
the client's *t*, which says nothing about the supplier that a `slow` signal
could carry. Stays as implemented in `observe.go`: `AttrClient` on an error is
nobody's signal. Cancelled attempts are not attempts for the rate term either.

### 7.6 PATH's config surface, key by key

Honoured: `signal_impacts.{success,minor_error,major_error,critical_error,fatal_error}`,
`tiered_selection.{tier1_threshold,tier2_threshold}`,
`tiered_selection.probation.{threshold,traffic_percent}`, `min_threshold`.
Inert and reported, with the reason: `signal_impacts.{recovery_success,slow_response,very_slow_response,stale_block}`
(types deleted, §7.2), `latency_profiles` / `latency_profile` (§7.2),
`tiered_selection.enabled` and `probation.enabled` (always on),
`probation.recovery_multiplier` (no such mechanism), `recovery_timeout` (a
cooldown SAGE does not have, §4.7). The rate term's three numbers are SAGE keys
under `reputation_config` because PATH has no equivalent:
`chronic_half_life_attempts` (0 = 20000, negative = term off),
`chronic_onset_rate` (0 = 0.0002), `chronic_full_rate` (0 = 0.01), and, from
§7.7, `tiered_selection.tier2_traffic_percent` (0 = 5, negative = off).

### 7.7 Mock soak, 2026-08-29: the term demotes, and where it stops

The rate term had only ever run in the simulation above, because beta has one
live backend. `bench/rate-term.sh` runs the full chain against the mock
backend with three endpoints injecting empty-body 200s (critical,
supplier-attributed) at the mainnet rates, defaults everywhere, retry and
health checks on, cache/singleflight/hedge off. 75 minutes, 59k relays/s,
every client response a 200.

| endpoint | injected | attempts | rate (EWMA) | penalty | score at full additive |
|---|---|---|---|---|---|
| spacebelt | 0.216% | 28,817 | 0.1415% | -20.0 | 79.997 — tier 2 |
| rpcgate | 0.065% | 132,125 | 0.081% | -14.3 | 85.7 — tier 1 |
| nodefleet | 0.00003% | 266,601,952 | 0 | 0 | 100 |

The three verdicts the design was calibrated for hold. Two things the
simulation did not show, both consequences of the tiers rather than of the
term:

**The intake of a violator is set by the probe cycle, not by its volume.** One
critical is `-25`, which leaves tier 1, and tier 2 receives no traffic while
tier 1 is populated — the snapshots show exactly 4 attempts a minute on a
tier-2 key, the two probes every 30 s at the time (the chain-id one now runs
every 5 minutes, so the intake is nearer 2 a minute). So an endpoint returns to tier 1 only
through probes: four of them from 75, five once the rate penalty is past
`-15`. A 0.216% endpoint therefore takes in one burst of ~460 relays per
cycle, however much the pool is offered — 560/min early, 300/min once the
penalty lengthened the recovery — and the first demotion by the rate term
alone came at 28.7k attempts and 53 minutes. The "fifteen minutes at mainnet
volume" above assumed the attempts keep flowing; they do not. The same
mechanism gave rpcgate 0.05% of the pool's relays in a 3-endpoint pool at
59k/s: it burns its ~1,500 clean attempts in a second and then waits a minute
for probes. At mainnet rates the bench is a smaller fraction of the cycle (at
23 relays/s the burst is 67 s against a 60 s bench), but for any endpoint the
additive term, not the rate term, is what meters traffic to it — a `-25` with
probe-only recovery is a duty cycle, and the rate term's job is only to make
the parked state stick.

**The steady state is the boundary, not the table.** Once the penalty reaches
`-20` a full additive scores 79.997, one probe under tier 1, and the key stops
receiving anything but probes. Probes are clean attempts, so they decay the
rate — by 0.28% (relative) per 80 attempts, which at four a minute is a
three-minute crawl back across the line, a ~460-relay visit to tier 1, one
critical, and a re-park that the next clean burst has to undo. spacebelt sat
at `rate 0.1415%, penalty -20.003, score 79.997` for the final 20 minutes. The
`-23 / -40 / -70` steady states in §7.3 are what the term reports for a key
whose attempts keep coming — a violator in a pool whose tier 1 has collapsed —
and are otherwise never displayed: `GET /admin/reputation/{service}` will show
every chronic violator at `-20` and the additive at 100, and that is the term
working. An operator reading the rate off the admin listing should know it is
a floor on the true rate once the endpoint is parked.

Not changed by this: the ratio, the half-life, the curve. What it put on the
table was a selection question, not a scoring one — whether tier 2 should
carry a trickle so that a parked key is measured by traffic rather than by
probes.

**Decided 2026-08-29: tier 2 carries 5% of first tries**
(`tiered_selection.tier2_traffic_percent`, `reputation.SelectorConfig.Tier2Pct`).
When tier 1 wins the cascade, that share of relays tries a uniform tier-2 pick
first with the tier-1 pick behind it for Retry — probation's mechanism, one
tier up. Tier 3 is left out: 10–50 is two or three recent criticals, which is
the outage detector doing its job, and a probe-cycle bench is the right price
there. The alternative considered — a cheaper critical on a key scoring 95 or
more — protects a good host from one blip but costs the outage detector its
first attempt, and does nothing for the parked-at-the-boundary state; it stays
on the table if the duty cycle is still visible after this. The cost to
clients is bounded by what tier 2 contains: at the mainnet rates that is a
0.1–0.3% first-try failure on 5% of relays, one extra retry per ~30k.

**Found while wiring it: probation's share never reached the HTTP path.**
`TieredSelector.Select` prepended a probation endpoint on 10% of relays as
designed, and `SelectBest` returned the *last* element of that list, with a
comment saying so on purpose ("we want the healthy pick"). Both lines date
from the initial commit. The prepend had a unit test, the reader ignored it,
and nothing exercised the two together. So `probation.traffic_percent` was
honoured by config and did nothing — the "same key, different semantics" case
`docs/path-compat.md` names as the one an operator cannot detect — and §7.4's
"traffic only through probation's 10%" reasoned from a share that did not
exist (its conclusion stands for the reason it gives). WebSocket never had a
share either: `TopTierCandidates` cascades T1 → T2 → T3 → probation.
`SelectBest` now returns the first element, the endpoint to *try*; Retry is
the fallback and always was. `TestService_SelectBest_ReturnsTheFirstTryPick`
pins it for both shares. What changes in production: 10% of first tries go to
a probation endpoint when one exists. The worst case is a dead host lifted to
10 by two probes — one failed first attempt, `-25`, out again.

**Soak with the trickle and the fix, same setup, 59 minutes** (stopped once
stable; 19.8k relays/s, every client response a 200):

| endpoint | injected | attempts | share | rate (EWMA) | penalty | score at full additive |
|---|---|---|---|---|---|---|
| spacebelt | 0.216% | 339,682 | 0.26% | 0.233% | -25.1 | 74.9 — tier 2 |
| rpcgate | 0.065% | 37,318,360 | 28.3% | 0.092% | -15.6 | 84.4 — tier 1 |
| nodefleet | 0.00003% | 94,092,442 | 71.4% | 0 | 0 | 100 |

Against the first soak: rpcgate went from 0.05% of relays to 28% — a tier-1
share, its one-blip benches now ending in relays instead of a probe cycle —
and spacebelt reached the table's steady state (`-23` predicted, `-24` to `-25`
measured, EWMA reading 0.21–0.24% against 0.216% injected) in three minutes
rather than parking at `-20.003` forever. The admin listing's rate is a
measurement again. spacebelt spends most of its time below 50 (additive 75
after each critical, `-25` on top), where neither share reaches it and probes
lift it back to tier 2 — so it is demoted *and* kept measured, at 0.23% of
relays. rpcgate's EWMA wandered 0.06–0.09% over the hour (`-11` to `-16`), which
at 37M attempts is the term's own noise at a 20k half-life, never near the
tier line. The duty cycle for good hosts is gone as a first-order effect; the
cheaper-critical-at-95 idea stays parked.
