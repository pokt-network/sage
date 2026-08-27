# Scoring v2 — implementation spec

Status: **approved for implementation pending Otto's review**. The model and the
decisions behind it live in `docs/scoring.md` (§3 principles, §4 model, §7
decisions with data). This document is the contract an implementer builds
from; it does not re-argue the decisions.

## 1. Goal

Reputation scores every relay **attempt** against the endpoint that produced
it, from the result that attempt produced; a batch counts once per endpoint;
latency reports and never penalises; chronic low-rate violators are visible to
the score; the PATH scoring keys that can mean what they say become live.

## 2. Components

### 2.1 `score` middleware — `relay/middleware/score.go`

Registered as `relay.MWScore = "score"`. Default chain position:

```
… circuit_break → method_blocks → select_endpoint → score → heuristic → send_relay
```

`mustPrecede` rules added to `relay/chain_order.go`:
`select_endpoint` → `score` ("score reads ctx.Endpoint"), `score` →
`heuristic` ("score consumes the attempt's HeuristicResult"), `retry` →
`score` and `hedge` → `score` ("one attempt is one scoring event"), `batch` →
`score` ("batch collapses per-attempt outcomes").

Behaviour, after `next.HandleRelay(ctx)` returns, when `ctx.Endpoint != ""`:

1. Build an `attemptOutcome{endpoint, rpcType, class, latency, reason}` from
   `ctx.HeuristicResult` and the returned error — the same table `observe.go`
   uses today (`buildSignal`), moved here unchanged in its grading. `class`
   is one of `success | minor | major | critical | fatal | none`, where `none`
   is a client-attributed error (nobody's signal) and a blockchain-attributed
   error is `success` for the purposes of the score (the endpoint answered).
2. If `ctx.ScoreSink != nil`, `sink.Add(outcome)`; else record it now via
   `reputation.Service.RecordAttempt`.
3. Set `ctx.HeuristicResult = nil` — the result has been consumed; no outer
   middleware may read it for scoring. (Retry's own reset per attempt stays.
   `hedge.mergeContext` keeps copying it for Observe's `X-Degraded` / logging
   use; Observe no longer scores from it.)

Gated by `featureflag.FlagScoringV2 = "scoring_v2"` (default **on**). When off,
the middleware passes through and Observe records exactly as today.

### 2.2 `relay.Context.ScoreSink`

New pointer field, `*middleware.ScoreSink` (type lives in `relay/` to avoid the
import cycle: `relay.ScoreSink` with `Add(AttemptOutcome)` and
`Flush(func(endpoint, rpcType, outcome))`). Set by **exactly one**
middleware: `batch`, on the parent context before it clones for sub-relays.
Nil on every non-batch request. Documented in the `Context` field comment as
"shared across the request tree on purpose — that is the mechanism".

Batch, after all sub-relays return: `sink.Flush` emits one outcome per
`(endpoint, rpcType)`: the worst class among outcomes with
`class ∈ {minor, major, critical, fatal}` (supplier-attributed); if none, and at
least one `success`, `success`; if only `none`, nothing. Emitted outcomes go
to `RecordAttempt`. Latency of a collapsed batch outcome is the **max** item
latency for that endpoint (reporting only).

### 2.3 `reputation.Service` changes

```go
// AttemptOutcome is what one attempt says about one endpoint.
type Outcome struct {
    Class     OutcomeClass   // success | minor | major | critical | fatal
    Latency   time.Duration  // reporting only; ignored when Probe
    Reason    string
    Probe     bool           // set by the health-check executor only
}

RecordAttempt(ctx, serviceID, endpoint, rpcType, Outcome) error
```

`RecordSignal(Signal)` stays as a thin adapter (`Signal.Type` → class) for the
WebSocket lifecycle and health-check producers, which keep their call sites;
`Signal` gains `Probe bool`. Signal types `recovery_success`, `slow_response`,
`very_slow_response`, `stale_block` and their constructors are **deleted**.
`DefaultImpact` remains for the five surviving types.

Per-key state (memory cache and storage):

| field | meaning | persisted |
|---|---|---|
| `score` | additive term, clamped `[0, MaxScore]` | yes (today's field) |
| `rate` | EWMA of failure weight per attempt | yes |
| `attempts` | attempts counted into `rate` (uint64) | yes |
| `traffic_attempts` | attempts with `Probe == false` | yes |
| `latency_ms` | EWMA of traffic latency, α = 0.05 | memory only |

Failure weight per attempt: `critical|fatal → 1`, `major → 0.5`, else `0`.
EWMA update: `rate += λ·(x − rate)`, `λ = ln2 / halfLife`. `none` outcomes never
reach `RecordAttempt` (the middleware drops them), so they are not attempts.

Effective score, the only value the selector, `Vouched`, `GetScore`,
`GetScores` and the admin listing return:

```
penalty(rate) = 0                                        rate ≤ onset
              = −40 · log10(rate/onset) / log10(full/onset)   onset < rate ≤ full
              = max(−70, −40 − 30 · log10(rate/full))         rate > full
effective     = clamp(score + penalty(rate), 0, MaxScore)
```

Defaults: `halfLife = 20000`, `onset = 0.0002`, `full = 0.01`. Config under
`reputation_config` (global, per-service override): `chronic_half_life_attempts`
(int; 0 = default, negative = rate term off), `chronic_onset_rate`,
`chronic_full_rate` (float64; 0 = default). Doc comments carry §7.3's
numbers. `ResetScore` clears all fields. `pruneUninformative` keeps a key
whose `rate > 0` even when `score == InitialScore`.

Storage: `Storage` gains `GetState/SetState(ctx, key, State)` and
`GetStates(prefix)`; Redis stores `State` as a JSON string in the existing
hash field (a legacy float value parses as `{score: v}`), so no migration.
`MemoryStorage` mirrors. The async write-behind path is unchanged in shape.

### 2.4 Observe — `relay/middleware/observe.go`

Under `scoring_v2` on: no signal recording. `buildSignal`/`penaltySignal` move
to `score.go` as the outcome table. Under the flag off: unchanged. The
docstring's step 1 is rewritten to say which path is live.

### 2.5 Health-check executor — `healthcheck/executor.go`

Sets `Probe: true` on every signal it records. No other change: probe outcomes
feed `score` and `rate` exactly like traffic.

### 2.6 Config surface — `config/service.go`, `cmd/sagegw/wire.go`

Live keys, threaded through `reputation.ServiceConfig` / `SelectorConfig` at
wire time with per-service override where PATH allows it:
`signal_impacts.{success,minor_error,major_error,critical_error,fatal_error}`
(zero = default), `tiered_selection.{tier1_threshold,tier2_threshold}`,
`tiered_selection.probation.{threshold,traffic_percent}`, `min_threshold`
(confirm what already reads it; wire the rest). Their doc comments lose the
"parsed and not implemented" text; `docs/path-compat.md` moves them to the
honoured list. Keys that stay inert keep the text and gain the §7 pointer.

`config.Validate`: `tier2 < tier1 ≤ MaxScore`, `probation.threshold < tier2`,
`0 ≤ traffic_percent ≤ 100`, `0 < onset < full < 1` when set.

### 2.7 Admin — `router/admin.go`

`GET /admin/reputation/{svc}` rows gain `additive`, `rate`, `penalty`,
`attempts`, `traffic_attempts`, `probe_only` (`traffic_attempts == 0`),
`latency_ms`. `score` stays the effective value. `docs/admin-api.md`
regenerates. UI Reputation table shows `penalty` and `probe_only`.

### 2.8 Metrics — `metrics/`

`sage_reputation_chronic_penalty` gauge histogram is **not** added (cardinality
per endpoint). One counter: `sage_reputation_attempts_total{service_id,rpc_type,class,probe}`.
`docs/metrics.md` regenerates.

## 3. Data flow, one request

```
client → … → batch (sets ScoreSink if >1 payload) → sub-relay clones
  → observe (no scoring) → retry → hedge → circuit_break → method_blocks
  → select_endpoint (ctx.Endpoint) → score ──▶ heuristic → send_relay
                                       │
                          outcome ◀────┘ (per attempt, from this attempt's result)
                          sink.Add | RecordAttempt
batch: sink.Flush → worst-of per (endpoint, rpc) → RecordAttempt
```

Retry losers: scored by their own attempt. Hedge arms: each scores its own
attempt on its own clone (`ScoreSink` pointer is shared and mutex-guarded;
without a batch it is nil and each arm records directly). Cancelled attempts:
`AttrClient` → class `none` → dropped before `RecordAttempt`.

## 4. Error handling

- `RecordAttempt` errors are logged at debug and dropped; never block a relay
  (today's contract).
- A `ScoreSink` on a context whose batch panics: `batch` flushes in a `defer`
  after `safego.Call` returns, so a contained panic still scores the attempts
  that completed.
- Rate term off (`chronic_half_life_attempts < 0`): `rate` stays 0 and is not
  written; effective = additive.
- Legacy storage values (bare float) parse as `{score}` with `rate = 0`.
- Flag flipped at runtime: the middleware and Observe check the flag per
  request; a request in flight is scored by whichever path saw the flag on its
  way out — at most one signal per attempt either way because Observe and
  `score` are never both active for the same request (Observe reads the flag
  at entry and stores the verdict on the context: `ctx.scoringV2 bool`, set by
  Observe, read by `score`).

## 5. Tests

Every test here must fail against a revert of the change it covers, and must
assert the *right* signal, not merely a count (`feedback_revert_check_discriminating_tests`).

- **Composition** (`relay/middleware/score_chain_test.go`): a fake chain
  `batch → observe → retry → hedge → circuit_break → select_endpoint → score →
  heuristic → fakeSend` with a scripted endpoint sequence. Cases: retry loser
  scored critical on A and success on B; stale result cannot reach B (B's
  transport error grades as its own transport class); hedge loser scored;
  both hedge arms fail → both scored; batch of 20 with one fabricated item →
  one `fatal` for that endpoint; batch with only `-32601` items → nothing;
  batch with `-32601` + successes → one `success`; cancelled attempt → nothing.
- **Rate term** (`reputation/rate_test.go`): deterministic sequences reproduce
  §7.3 numbers within tolerance: 3-burst → penalty 0; 20-burst → −12.7 ± 0.5;
  steady 0.2% over 100k → −23 ± 3; 20% → −70; term off → 0; onset/full from
  config change the curve; `ResetScore` clears; legacy float parses.
- **Effective score everywhere**: selector tiers, `Vouched`, admin listing
  use effective; a key at `score = 100, penalty = −40` lands in tier 2.
- **Config**: PATH example config's `signal_impacts` values are the deltas in
  effect; `tiered_selection` thresholds reach the selector; validation
  rejects `tier2 ≥ tier1`; inert keys still reported.
- **Probe flag**: executor signals carry `Probe`; latency EWMA ignores them;
  `probe_only` true until first traffic attempt.
- **Flag off**: Observe records as before, `score` records nothing (the
  revert-check for the whole feature).
- `make docs` golden tests pass after regeneration.

## 6. Rollout

Ships on `main` behind `scoring_v2` default on. Beta check before merge: the
12-minute load script from the 2026-08-27 collection, then
`GET /admin/reputation/pnf-anvil` shows the live host with `attempts ≈ traffic
attempts + probes`, `penalty 0`, `probe_only false`, and the dead hosts at
`score 0, probe_only true`; timeline shows one signal per attempt, none for
`-32601` or cancelled relays.

## 7. Out of scope

Dual recording with exported delta (§6 of `docs/scoring.md`) — replaced by the
flag and the beta check above. A per-endpoint latency gauge. Any use of
`latency_ms` in selection. WebSocket frame scoring changes.
