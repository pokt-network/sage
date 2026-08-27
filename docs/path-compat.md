# PATH compatibility — what it means, and where it stops

SAGE is a restructured fork of PATH. It is meant to be droppable into PATH's
place, and it is also meant not to inherit the bugs that came out of PATH's
structure. Those two goals only conflict if "compatible" is treated as one
thing. It is three, and they have different rules.

## The three layers

**1. What a client sees — must match.**
Request and response shapes, headers, status codes, the `Target-Service-Id`
routing, batch semantics, WebSocket framing. A client must not be able to tell
which gateway it is talking to. Divergence here is a bug, not a decision.

**2. What an operator configures and observes — must match, or be loud.**
Config keys, admin routes, metric names. SAGE loads a PATH config unmodified,
which means tolerating keys for features it does not have. Tolerating is not the
same as pretending, so every key SAGE does not act on is reported at startup:

- `Config.Ignored` — the key has no Go field at all. Caught by a second decode
  with `KnownFields` (`config/load.go`).
- `Config.Inert` — the key parses into a field that nothing reads. Caught by the
  registry in `config/inert.go`, which `TestInertRegistryCoversDocComments`
  holds to the doc comments so the two cannot drift.
- `Config.Warnings` — the key parses, *is* read, and probably does not do what
  whoever wrote it expected. One sentence saying what the gateway will actually
  do with the value, because refusing the file would break the compatibility
  promise and accepting it silently would mislead.

Most of the reputation-tuning surface is honoured rather than reported:
`signal_impacts` for the five signal types SAGE still has, the
`tiered_selection` thresholds, `probation.{threshold,traffic_percent}` and
`min_threshold` are read by the selector and the scorer — globally, because one
selector serves every service, so a per-service copy of them changes nothing.

Thresholds that do not descend — the beta config we run has `tier2_threshold:
30` above `probation.threshold: 50` — still load. PATH accepts that file and
the selector copes, classifying probation before tier 2 so a band simply ends
up narrower than it reads. Refusing to boot on it would break the whole point
of layer 2 over something that works. It is not silent either: it produces a
third kind of startup complaint, `Config.Warnings`, one sentence saying what
the selector will actually do with the numbers given. Only values that describe
no behaviour at all (a traffic share above 100%, a chronic onset rate at or
above the full rate) are refused.

What stays `Inert` is what SAGE has no mechanism for: `latency_profiles` and the
two latency signal impacts (latency reports, it does not penalise),
`recovery_success` (the type is gone), `recovery_timeout` and
`probation.recovery_multiplier` (there is no cooldown and no multiplier),
`tiered_selection.enabled` and `probation.enabled` (both always run). An
operator who tuned those on PATH and moved the file across was, until this
existed, editing structs no code reads — and nothing said so.

**3. How it works inside — free to diverge.**
Middleware decomposition, reputation mechanics, QoS plugin interfaces, session
handling. This is where PATH's incidents live, and copying a mechanism is how
you inherit the failure mode that comes with it. Divergence here is the point of
the fork.

## The dangerous middle

The failure this document exists to prevent is not a missing key or a different
internal design. It is **the same key with different semantics** — present,
parsed, apparently honoured, quietly meaning something else. That is worse than
either extreme, because it is the only one an operator cannot detect.

The rule: if SAGE implements a PATH key with different behaviour, it goes in the
register below **and** the difference has to be observable — a startup log, a
refusal, or a doc the operator will actually meet. Silence is only acceptable
when the behaviour is identical.

## Register of deliberate divergences

| Area | PATH | SAGE | Why |
|---|---|---|---|
| Strike system / cooldowns | Endpoints are benched by a strike system and by two rate detectors, all sharing one `CooldownUntil` | None. Reputation is a continuous score feeding tiered selection | PATH accumulated five mechanisms that could each remove an endpoint, with separate counters and one shared timestamp; a first offence inherited a stale escalation. `strike_system` has no field here on purpose and lands in `Ignored` |
| Score deltas | `signal_impacts` config | Honoured for the five surviving signal types; `recovery_success`, `slow_response`, `very_slow_response` stay `Inert` | Types deleted — `docs/scoring.md` §7.2 |
| Latency scoring | `latency_profiles` (thresholds, bonuses, penalties) | Latency has reporting power only: a per-key EWMA in the admin listing; the block stays `Inert` | Decided, not open — `docs/scoring.md` §7.2 |
| Chronic violators | Critical-rate detector with its own EWMA, threshold, escalation and cooldown | A rate term inside the score: EWMA failure weight → penalty → same tiers | One mechanism, one state, one power — `docs/scoring.md` §7.3 |
| Archival routing | Positive proof required: an endpoint must be marked archival to serve archival requests | Tri-state; only a **fresh negative** excludes | A node fabricating state from its head produces the same success as a real archival node, so a positive mark cannot be trusted to gate traffic. A self-reported negative can |
| Solana `sync_allowance` | Unset means a strict `height >= perceived` comparison | Unset defaults to 1500 blocks | PATH's 2026-08-18 production incident: perceived is a max, so at 400ms/block only the last reporter survived |
| Supplier blacklisting on validation errors | Blacklists on `ErrRelayResponseValidationGetPubKey` | Deliberately excluded (`protocol/shannon/response_validation.go`) | That error is SAGE's own full node failing to answer, not the supplier's fault. During a local full-node outage PATH's rule empties the pool in one pass |
| WebSocket session rollover | Session rebind + subscription replay keeps the connection alive across boundaries | The bridge closes at the session boundary | Deliberate v1 scope. The rebind engine is ~1.5k lines with a subscription registry and ID remapper; port it if transparent survival is wanted |
| Block-height consensus | Redis-synchronised, max-merged | In-memory, median-anchored, with a plausibility ceiling | PATH's max-merge let one wrong reporter set perceived height for everyone |
| Response analysis | Byte-substring matching over the body | gjson top-level lookup plus `ErrorAttribution` | Substring matching produced false positives on nested fields; attribution makes "do not penalize the supplier for a blockchain error" structural rather than per-call-site |

## Porting rule

Read PATH's commits for the **incident**, not the patch. Their fix is shaped by
their structure; the thing worth importing is what production did to them.

The catch-up record says this is cheaper, not dearer: of the 21 commits on
their security audit branch, 12 were N/A here; of the 10 in the 2026-08-20 pass,
6 were N/A, three of them specifically because SAGE has none of the overlapping
detectors the fix was repairing. Divergence is what makes most of their bug
reports not apply — but only if every pass still reads all of them, and records
the N/A reasoning where the next pass will find it.

Procedure and history live in the fork-sync notes; the short version is: check
PATH's **branches**, not just `origin/main`, and write down why something did not
apply so nobody re-derives it in six weeks.
