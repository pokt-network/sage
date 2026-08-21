# Reputation scoring — design

Status: **proposal**. Nothing here is implemented yet. It exists because the
scoring path was audited on 2026-08-21 and what it actually does is not what the
code reads like it does.

`ARCHITECTURE.md` describes reputation as the thing that learns from relays and
steers selection away from bad endpoints. That is the intent. This document
records what the implementation currently does instead, the principles the
replacement should hold to, and the model proposed for it.

## 1. What scores today

Four things produce `reputation.Signal`s:

| Producer | Where | Cadence |
|---|---|---|
| Relay outcomes | `relay/middleware/observe.go` | once per **client request** |
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
to the breaker, and `observe`, which converts it into the signal.

The chain positions are what make the difference between those two:

```
… cache → batch → singleflight → observe → cross_validate → retry → hedge
    → supplier_affinity → circuit_break → select_endpoint → heuristic → send_relay
```

`circuit_break` sits **inside** `retry`/`hedge`, so it sees every attempt.
`observe` sits **outside** both, so it sees one outcome per client request — and
it sits **inside** `batch`, so it sees one per payload.

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

Open: worst-of versus majority. Worst-of is proposed because a single fabricated
response inside a batch is still a fabricated response, and averaging it away is
how a bad endpoint hides in bulk traffic.

### 4.4 Decide what latency does

Three options, in order of preference:

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

## 7. Open questions

- Worst-of or majority for a batch (§4.3)?
- Which latency option, if any (§4.4)?
- The erase ratio, stated as a number (§4.5)?
- Should a probe-only-graded endpoint be selectable at full score, or capped
  until real traffic has confirmed it (§4.6)? This is the one place where
  probe/traffic separation would change behaviour rather than only reporting.
