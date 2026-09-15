# Automatic operator drains — design

Status: proposal, 2026-09-15. Nothing here is built. Shadow mode ships first.

## 1. Problem

On 2026-09-14/15 mainnet SAGE needed four manual operator drains, each applied
by a person reading metrics and calling `POST /admin/reputation/drain/{svc}`:

| Service | Drained | What the metrics showed | After the drain |
|---|---|---|---|
| sei json_rpc | rpcgate.xyz | 16.9% non-200 (PATH 4.2%); every nodefleet key below the floor, rpcgate holding 12 of 19 keys and answering 408 to everything | 500s 480 → 15 in 9 min |
| celo json_rpc | rpcgate.xyz | 90% non-200 (PATH 0%); retries landing on rpcgate's 408 | 0.1% |
| base json_rpc | rpcgate.xyz, stakeandrelax.net | 16.7% non-200 and 19,037 pool-collapse picks in 7 min once released without the fix | 0.1–0.2% |
| solana json_rpc | rpcgate.xyz | 46% non-200 during an upstream incident; kleomedes the only key above 0 | 0.7% (upstream eased in the same window) |

Every case had the same shape. Tiered selection had already scored the bad
operator to 0, so it received no tier traffic. It kept receiving the
**pool-collapse fallback's** picks, because when every candidate for a retry
is below `min_threshold` the guard serves the least-bad member uniformly
(`reputation/selector.go:252`), and a zero-scored operator that fails every
request is exactly as "least bad" as a slow one that sometimes answers.

The manual loop costs 10–30 minutes per incident: noticing, reading,
proposing, getting approval, applying. It also needs a person awake. The
engine automates that one decision, with a human veto that sticks.

Counter-case, which the engine must never act on: moonbeam and moonriver,
where rpcgate is the **only** responsive supplier. Draining it would empty the
pool. Their collapse counts are the highest on mainnet (moonbeam 1,201 per
10 min) and are correct behaviour.

## 2. What it measures, and which power it holds

`docs/scoring.md` §5: a new mechanism must measure something the score cannot
represent, and must state its power.

**What the score cannot represent.** A score is per key (URL × RPC type) and
ranks keys. At 0 it has nothing lower to say. It cannot express "among the
keys that are all at 0, this operator answers none of the requests the
collapse guard sends it, and a different operator exists that the pool vouches
for." That is a fact about the pool's fallback path across operators, not
about any one key. The engine measures that relation and nothing else.

**Power: exclusion, time-bounded.** It is the same power a manual drain holds,
and it goes through the same store (`drain.Store.Set`, `drain/store.go:43`).
It records **no** reputation signal and changes no score.

**Principle 3 (one fact, one power).** The engine never acts on the facts
scoring acts on. Scoring's power over this operator is already spent: the
operator is out of tier selection. The engine removes it from the one path
scoring cannot close, the collapse fallback, and only when the pool has a
vouched alternative. It does not re-punish a low score. A low score is a
precondition, never the trigger.

**Interaction with 516616b (chronic-term floor).** Before 516616b the chronic
term pushed working keys to 0, and collapse fired on pools that had usable
hosts. For sei, 355 collapse picks per 10 min became 122 after the roll;
robinhood dropped off the list. Collapse now mostly means real outages, which
is the baseline the trigger below is calibrated on. **Shadow data from before
516616b is not comparable and must not be used to set thresholds.**

## 3. Trigger

Evaluated per (service, rpc_type, operator), every 60 s, over a sliding
10-minute window. The 10 minutes is the cadence the manual reads used, and it
is long enough that one burst does not act. **All** of the following must hold:

1. **Collapse is firing for this pool:** at least 20 collapse picks for
   (service, rpc_type) in the window. mainnet sei had ~120 per pod per 10 min
   before its drain and base thousands; a quiet pool with 3 picks is not an
   incident.
2. **This operator absorbs the collapse:** at least 30% of those picks landed
   on this operator's endpoints. Below that it is not the operator the
   fallback is feeding.
3. **This operator answers nothing:** its success rate on attempts in the
   window is at most 2%, over at least 50 attempts (the attempt floor). rpcgate
   on sei, celo and solana was 0% (408 on everything). A slow-but-working
   operator (nodefleet on sei, 60–80%) never qualifies.
4. **Another operator is vouched:** at least one endpoint of a different
   operator in the **current session** passes `reputation.Service.Vouched`
   (score ≥ probation). This is stronger than the admin route's
   last-operator check (`router/admin_drain.go:299`), which only asks whether
   anything else exists. moonbeam fails this condition, and so does a pool
   where the alternative is also junk.
5. **The admin route's own guard passes:** `lastOperatorStanding` returns
   false. It is reused, not re-implemented.

Success, attempts and failures are counted from supplier-attributed attempts
only. Client- and blockchain-attributed outcomes are not attempts, the same
rule the chronic term uses (`reputation/rate.go:103`).

**Inputs the engine needs that the hooks do not carry today:**

- The collapse hook passes only the service (`reputation/selector.go:86`,
  wired at `cmd/sagegw/wire.go:506`). It must also carry the RPC type and the
  endpoint the guard served.
- The signal hook (`reputation/service.go:249`, wired at `wire.go:513`)
  carries service, RPC type, signal and probe, but not the endpoint. It must
  gain the `domain.EndpointAddr`, so outcomes can be grouped by
  `Operator()`.

Both are wire-time hooks with one caller each today. Widening their signature
is the smallest change; a second hook would be two ways to observe one event.

## 4. Action and TTL

On trigger, and only on the leader:

    drain.Store.Set(Entry{
      Key:    {ServiceID, Operator, RPCType},   // always scoped to one RPC type
      Until:  now + 2h,
      Reason: "auto: <n> collapse picks (<p>%), success <s>% over <a> attempts; vouched alt <operator>",
    })

- **Always RPC-type scoped.** An operator failing json_rpc may serve rest;
  scores are split by type for the same reason (`reputation/doc.go:25`).
- **TTL 2h, fixed.** It is shorter than the manual drains (6h, 10h) because
  nobody chose it. A drained operator receives no traffic, so it cannot prove
  recovery; only expiry can bring it back. If the conditions hold again after
  expiry, it re-drains, subject to the rate limit. Capped by
  `admin_config.max_drain` (`config/config.go:303`) like any drain.
- **Never released early by the engine.** Release is a human act or expiry.

**Fan-out.** `drain.RedisStore` writes one field in the `sage:drain` hash
(`drain/redis.go:31`, `HSet` at `:228`). Every replica re-reads it within the
5 s cache TTL (`defaultCacheTTL`), so a drain set on the leader applies
fleet-wide with no new mechanism.

**Leader.** `healthcheck.LeaderElector` already elects one writer per Redis
database with `SET NX EX 30s` (`healthcheck/leader.go:20`, `:139`). The engine
evaluates only while `IsLeader()` is true. Without Redis every instance is
leader (`leader.go:26`), and drains are process-local anyway, so each pod
drains its own view, which is correct for local-only mode.

**One consequence to accept.** The leader sees only its own pod's traffic
(one third of mainnet). The thresholds above are per pod on purpose, and they
held for every case in §1. Aggregating signals through Redis is not proposed.

## 5. Guards

| Guard | Mechanism | Default |
|---|---|---|
| Master switch | flag `auto_drain` in `featureflag.DefaultFlags` (`featureflag/defaults.go:82`) | **off** |
| Shadow | flag `auto_drain_shadow`: evaluate, log, count, never `Set` | **on** |
| Per-service opt-out | the existing per-service flag override on `auto_drain`; nothing new | none |
| Cap | at most 1 live auto drain per (service, rpc_type), and 5 across the fleet | 1 / 5 |
| Rate limit | at most 1 new auto drain per service per 30 min, and 3 per hour fleet-wide | 30 min / 3 h⁻¹ |
| Tagging | `Reason` starts with `auto:` (the field exists, `drain/store.go:32`) | — |
| Suppression | an `auto:` drain gone before its `Until` was released by a person: that key is not auto-drained again for 6 h | 6 h |
| Hands off manual drains | the engine never sets, extends or releases a key whose live entry lacks the `auto:` prefix | — |

Suppression needs no new storage. The leader compares its own record of
`auto:` drains with `drain.Store.Active` on each tick. A key that vanished
early was released by a person (duration 0 at `router/admin_drain.go:112`, or
`DELETE` at `:190`). The record is in the leader's memory and is lost on
failover. That cost is accepted: the worst outcome is one repeat drain that a
person releases again.

The thresholds in §3 start as constants. They become tuning knobs
(`tuning/knob.go:94`) only if shadow data shows a service needs different
ones. A knob nobody has a reason to turn is §5's "control that does not
control".

## 6. Interaction with manual drains

- A manual drain on the same key wins. The engine sees a live non-`auto:`
  entry and does nothing, not even to extend it.
- A person can promote an auto drain by re-POSTing the same key with their
  own reason. It stops being `auto:` and the engine leaves it alone.
- `GET /admin/reputation/drain/{svc}` already lists both, and the `auto:`
  prefix in `reason` tells them apart. `RUNTIME-OVERRIDES.md` does not need an
  entry for an auto drain; an auto drain that a person keeps (by promoting it)
  does.

## 7. Observability

- `sage_auto_drain_total{service_id, rpc_type, outcome}`, where outcome is one
  of `drained`, `shadow`, `suppressed`, `rate_limited`, `capped`,
  `no_vouched_alternative` or `last_operator`. It is a closed set. Operator is
  not a label: the existing drain gauge (`metrics.NewDrainCollector`,
  `wire.go:458`) already names the live drains by domain.
- One `Warn` log per decision other than `shadow`. It carries the full
  evidence: picks, share, success, attempts, the vouched alternative.
- The alert people will want is `increase(sage_auto_drain_total{outcome="drained"}[15m]) > 0`.
  It pages nobody, but it posts to the ops channel.

## 8. Rollout

1. Ship with `auto_drain` off and `auto_drain_shadow` on, on an image at or
   after 516616b. Run for 48 h on mainnet.
2. Compare shadow decisions with what ops did by hand. It should have
   proposed, within one tick, the drains on sei, celo, base and solana (if
   replayed), and nothing on moonbeam or moonriver. Any shadow drain ops
   would not have made is a threshold bug; fix it before step 3.
3. Turn `auto_drain` on per service for sei, celo and base (per-service flag
   override). Watch for 48 h.
4. Turn it on globally, keeping the per-service opt-out for anything that
   misbehaves.

It stays **mainnet only**. canary-sage (paid traffic) keeps `auto_drain` off
until the user decides otherwise.

## 9. Test plan

Unit tests drive the engine with a fake clock, a fake `drain.Store`, and
collapse and signal events fed directly:

- **sei shape:** nodefleet at 70% success, rpcgate at 0% absorbing 60% of
  picks, one nodefleet key vouched. Expect exactly one `Set` on
  (sei, rpcgate.xyz, json_rpc), with the `auto:` reason.
- **moonbeam shape:** rpcgate is the only operator. Expect no `Set` and
  `last_operator`/`no_vouched_alternative`.
- **Junk alternative:** a second operator present but below probation. Expect
  no `Set`.
- **Slow operator:** 60% success with collapse firing. Expect no `Set`
  (condition 3).
- **Below the attempt floor:** 40 attempts at 0%. Expect no `Set`.
- **Manual drain present on the key:** expect no `Set`, and no extension.
- **Suppression:** an auto drain released early; conditions hold again within
  6 h. Expect `suppressed`.
- **Rate limit and cap:** a second trigger within 30 min is `rate_limited`;
  a sixth fleet-wide is `capped`.
- **Shadow:** everything evaluates and counts, `Set` is never called.
- **Not leader:** nothing evaluates.
- **Flag readers:** `featureflag/readers_test.go` rows for both flags, per
  the rules of engagement.

Then a mock soak: `protocol/mock` with a per-endpoint failure rate reproduces
the collapse. It checks that the engine drains within two ticks and that
selection's 5xx fall afterwards.

## 10. Decisions (2026-09-15, the user)

1. **TTL:** 2 h, fixed.
2. **Scope:** every RPC type from the start, websocket included.
3. **Where it runs:** on every instance that is not following a peer. An
   instance that reads another's probe stream (`peer_probe_stream`, today
   canary-sage reading mainnet) honours that peer's `auto:` drains instead of
   running its own engine; if it stops following, it runs its own.
4. **One pod's view:** accepted. The leader decides from its own traffic.
5. **Vouched alternative:** one endpoint of another operator is enough. Unlike
   a method block, a drain does not move tier traffic onto the alternative —
   tier selection already sends it there — it only changes where the
   collapse fallback lands, so there is no capacity argument.
6. **Where decisions go:** a capped Redis stream, `sage:auto_drain:events`
   (the last ~1000 decisions, in memory without Redis), read through
   `GET /admin/auto-drain/events?service=&limit=`. It records every decision,
   shadow and declined ones included, with the evidence. Nothing pages;
   `sage_auto_drain_total` is for dashboards.
