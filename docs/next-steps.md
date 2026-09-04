# Next steps

The agreed work queue, so that any contributor or agent can pick up where the
last session left off without needing that session's context. Items are
ordered by priority within each section. Update this file when an item lands
or a decision changes it; delete items rather than marking them done, so the
file only ever lists open work.

Last updated: 2026-09-04 (the principles audit landed as six commits, baf236a
through the contract-docs commit; the canary runs `41db74a` with four
services on real QoS and the audit image is not yet built. PATH
`origin/main` at `274e9791`, 2026-08-25).

## Roll the audit image (raised 2026-09-04)

The six audit commits change what a client and a probe see. Before the
image rolls, ops needs to know, and after it rolls these are the checks:

- **Gateway-made failures are 5xx now, not 200.** Status-share panels on the
  canary move on rollout: what was a 200 with `-32603` in the body is a 500
  (504 on timeout, 429 on rate limit, 400 on a client mistake). Judge the
  roll on `sage_client_requests_total` by status *and* the JSON-RPC error
  rate together, not on the 200 share alone.
- **`/health` is liveness, `/healthz` is readiness.** Any manifest or LB
  rule that used `/health` as readiness must move to `/healthz` or `/ready`;
  the canary manifest in `docs/operations.md` uses `/livez` and `/ready` and
  is unaffected. `/ready/{service}` can now answer 503.
- **Client-side plain-text 403/401 on REST now grades the supplier.** The
  kalorius arc (44% to 0% over 80 minutes, probes alone) should shorten on
  the next such supplier; watch `http_4xx_page` in the timeline reasons on
  eth-beacon and tron.
- **`active_health_checks.enabled`** is honoured by presence. The canary
  config writes `enabled: true`; confirm the startup report carries no
  "probes are off" line after the roll.
- **`sage_singleflight_coalesced_total` and `sage_degraded_total` start
  moving.** A non-zero value is the counter working, not a regression.
- **The startup report** carries new line kinds: per-service RPC types no
  probe covers, and `enabled: false` decisions. Read it once.
- **Judging the status shift without a JSON-RPC error series.** SAGE exports
  no counter for a 200 that carries a JSON-RPC error body, so the pre-roll
  200 count includes gateway-made `-32603` failures and the post-roll one
  does not. Compare pre-roll 200 share against post-roll `200 + 500 + 504`;
  408/400/502/404 and attempts-per-client-request should be unchanged.
- **Export `sage_client_jsonrpc_errors_total{service_id}`** — a 200 whose
  body carries a top-level `error`, counted at the router. Ops asked for it
  on 2026-09-04 and it is what the 408 investigation lacks: the split
  between a supplier's own error delivered as 200 and a gateway failure.
  One gjson top-level lookup per JSON-RPC response; measure it on the mock
  backend before shipping rather than assuming it is free.






## Explore next (raised 2026-09-01)

- **eth-beacon 403: mechanism confirmed, hole closed, re-measure after the
  audit image.** Ops measured the arc on 2026-09-04: the two kalorius hosts
  fell from 44% of client relays to a permanent 0% over about 80 minutes,
  moved by probes alone, because client-side REST 4xx was never graded. The
  heuristic now attributes a plain-text 401/403 on REST to the supplier
  (`cd3eb39`). What is left to see is the arc on the next such supplier,
  which should be one probe cycle rather than eighty minutes.

- **radix cannot be probed until its config is fixed.** The canary declares
  `rpc_types: ["json_rpc"]` for what is a REST gateway API, so SAGE refuses a
  REST request to it with a 400 before any session lookup — inherited from the
  PATH config. It also has no staked suppliers. Both have to change before the
  parked radix declaration in `qos/jsonheight` can be verified, and it stays
  unwired until it is.

- **Choose the probe cadence on the histogram, now that it is a live knob.**
  `health_checks.interval` is tunable at runtime as of 2026-09-03, so this is
  an experiment rather than a rollout: set it, read
  `sage_health_check_cycle_seconds` and `sage_health_check_cycle_overruns_total`
  for a cycle or two, and put it back if it is wrong. Ops recommends 120s
  against a measured 73.7s mean cycle and 61% overruns; note their halving
  arithmetic understates the saving slightly, because the period today is the
  cycle (~74s), not the 60s tick, so the drop is nearer 14.5 to 9 probe
  relays/s than to 7. The thing to watch on the way up is
  `sage_chain_view_staleness_seconds`, which is how long a degraded supplier
  keeps taking traffic.

  Worth settling first, and only ops can see it: whether the canary config
  carries any `local[].check_interval` rows. One service at 10s pins the
  executor's tick for the whole fleet regardless of the global value, and PATH
  configs commonly carry exactly that.

- **Decide whether four health-check workers is enough, now that the cadence
  is measurable.** `sage_health_check_cycle_seconds` and
  `sage_health_check_cycle_overruns_total` (added 2026-09-03) say whether the
  configured interval is being achieved. If overruns are steady, the fix is
  more workers or a faster probe path — not a shorter interval, which only
  drops more ticks. Two shapes to weigh: raising `workers` costs concurrent
  load on the same suppliers the relay path is using, while taking dispatch off
  the ticker goroutine (queue the cycle's work and let the tick return) removes
  the coupling entirely but lets cycles overlap, which the per-cycle
  bookkeeping in `lastRun` and the traffic skipper both assume cannot happen.
  Get the before-and-after from the histogram rather than guessing.

- **Confirm the WebSocket control-frame path fires on mainnet.** The envelope
  decoding added 2026-09-03 (see the CHANGELOG) is a port of a PATH fix found
  by a live session-cycle test; SAGE's own exposure is inferred from the miner
  behaving the same way for us, not observed. It is safe either way — a payload
  that is not provably an envelope is passed through untouched — but if it
  never fires, the code is inert and worth knowing about. The signal is the
  `ws: endpoint returned a non-2xx response` WARN, which names the status;
  expect it at session boundaries on long-lived subscriptions, and zero
  elsewhere. If it fires often, the `410` count is also the rate at which
  clients were previously being handed protobuf.

- **A cold pod is served client traffic before readiness passes.** Confirmed on
  every pod of the 2026-09-02 `335a264` rollout: relay errors begin ~7-8 s after
  container start, 11-18 s before `/ready` returns true, and the requests fail
  with `relay timeout exceeded`. The warm gate cannot cause this — Service
  membership is the cluster's to decide — so the cause is on that side
  (`publishNotReadyAddresses`, or linkerd endpoint caching). Session prefetch
  makes it survivable rather than harmful, since a pod that gets traffic early
  can now serve it, but the gate is still being bypassed and that is worth
  fixing at the manifest.

- **Turn traffic-informed probing on for one service and measure it.** Built
  2026-09-02 and shipped OFF: the `traffic_informed_probing` flag defaults to
  false, so nothing changes until an operator enables it, globally or for one
  service through the admin API. What it does: a due health check against a
  backend that client traffic graded enough in the previous cycle is not sent,
  because every client attempt records a reputation signal and a busy backend
  is being graded continuously. Measured on the canary at 1% traffic before
  building it, 48.66% of probes in a ten-minute window went to backends traffic
  had graded in that same window (1,015 of 2,086), concentrated on the busy EVM
  services — arb-one, mantle, fantom and robinhood at or near 100%, eth 76.2% —
  and absent on the long tail.

  The experiment: enable it for one high-saving service (`arb-one` or `mantle`),
  and watch `sage_health_check_skipped_total` against
  `sage_health_check_results_total{source="probe"}` for the saving, and that
  service's block-height agreement plus
  `sage_client_requests_total{service_id="…"}` for the cost. **Judge on the
  client counter, not on `sage_relay_total`** — the latter counts attempts
  inside retry, hedge and batch, so its status shares move when retry volume
  moves and say nothing directly about what a caller got. That distinction is
  what took a day to establish during the 408 incident; do not re-learn it
  here. Roll it back with the same admin call if either moves. Then repeat at
  whatever traffic share this would run at, because both halves of the tradeoff
  scale with it — more keys become trafficked, so the saving rises and so does
  the exposure.

  What the code already guards, so the experiment does not have to: the skip
  only fires once the pod is warm (readiness counts coverage from applied probe
  results, and `warm` latches, so this costs one atomic read after startup); a
  key the reputation service does not know yet records no baseline, so a
  lifetime's cumulative traffic is never mistaken for one window's; a count
  that went backwards is treated as an eviction rather than negative traffic;
  and the threshold is derived from the observation pipeline's `sample_rate`
  rather than being "any traffic", because a probe is the only observation
  source that bypasses sampling. `health_checks.min_traffic_signals` overrides
  the derivation.
- **Re-land the 408 supplier attribution, one half at a time.** The combined
  change (retry + minor penalty, `26f22c5`) was reverted on 2026-09-02 after
  the canary quadrupled its client-facing 408 rate; the revert put it back
  (0.719% client 408, 200 at 99.140% per 100k, attempts per client request
  1.519), which confirms the attribution change as the cause. The measurement
  and the ruled-out mechanisms are in the CHANGELOG. What was never separated is which
  half did it. The retry half alone cannot raise client 408s — retry exhaustion
  delivers the upstream's own response (`router.go`), so a retried 408 ends as
  200 or the same 408 — which points at the penalty half and its effect on
  selection. Worth testing as two canary experiments rather than one: 408
  retryable with `ShouldPenalize: false`, then the penalty alone. Until then a
  supplier that times out keeps being handed to callers, which is the known
  cost of the safe state.
- **Canary counters need more than one window before they are a baseline.**
  Two targets set during the 408 incident were built on single windows and both
  were wrong. Probe volume was quoted at ~18/s from the d1f237f rollout and is
  really 2.4-3.2/s (4,389-5,776 probe attempts per 30 min, 3.1-4.1% of all
  relay attempts, measured 2026-09-02). Client-side 404 and 502 read 258 and
  120 per 100k in one `cac8818` window and 62 and 46 in another — a 4x spread
  inside one image — so
  the "404 and 502 return to 258/120" pass condition was measuring noise, and
  the ~300 per 100k of apparent 404/502-to-408 reclassification was noise too.
  Take an offset sweep before calling any of these a baseline.
- ~~Label probe vs client attempts in sage_relay_total.~~ Landed 2026-09-02,
  and the premise it was filed under was wrong: probes were not a third of
  relay_total, they were not in it at all. `healthcheck.Executor` calls
  `protocol.SendRelay` directly on the raw protocol handed to it in
  `cmd/sagegw/wire.go`, so a probe never enters the middleware chain and
  `RecordRelay` never sees it. The 1.557 ratio measured on 2026-09-01 is
  retries, hedge losers and **batch sub-relays** — the metrics middleware sits
  inside `MWBatch` as well as retry and hedge, so a ten-call JSON-RPC batch is
  ten increments against one client request. Anyone reconciling relay_total
  against client_requests_total should start there, not with probes.
  `request_type` (`client`|`probe`) is now on `sage_relay_total` and
  `sage_relay_latency_seconds`, and the executor records its own sends, so
  probe spend and probe latency are visible for the first time. Unfiltered
  panels step up by the probe rate; `{request_type="client"}` restores the old
  reading.

- ~~Dynamic blocked_domains from the admin UI.~~ Landed 2026-09-01: package
  `blocklist` owns the union of the config list and admin-set bans;
  `PUT/GET/DELETE /admin/blocked-domains[/{domain}]` and a "Blocked domains"
  tab in the UI; Redis hash `sage:blocked_domains` polled every 5 s so a ban
  reaches every replica and survives a restart; `sage_blocked_domains_admin`
  counts them. Not yet used in anger — banning nodefleet WS is the first use.

- **Consume PATH probe/reputation instead of running our own health checks.**
  Goal: two gateways probing the same suppliers pay twice; share one probe
  spend. Metrics comparison already exists (taiji dashboard) but is observability,
  not routing input. To actually skip SAGE probes, PATH must PUBLISH its
  probe results (an API or a cross-gateway stream, like SAGE's own sage:probes).
  Catch: PATH and SAGE score differently (scoring v2 diverged) — SAGE can consume
  PATH's raw probe *results* (answered/latency) and apply its own scoring, NOT
  import PATH's scores wholesale. Concrete question: can PATH expose a
  probe/health-result feed SAGE subscribes to? If yes, `healthcheck.Executor`
  gains an external result source (it already has a ProbeSource seam for the
  Redis stream — same shape).

- **WS cold reputation (what remains of the 1011 thread).** The dial bug is
  fixed: suppliers stake WS endpoints as https:// (nodefleet stakes every one
  that way — 78% of the WS pool) and the bridge handed that to gorilla, which
  rejects non-ws/wss as "malformed ws or wss URL" before a packet is sent.
  ConnectEndpoint now dials http as ws and https as wss (never downgrading);
  no ban needed. What remains is the cold-score half: WS volume is too low to
  warm scores and probes do not cover the websocket rpc_type, so HTTP's
  knowledge does not reach WS — (a) share operator/domain reputation across
  rpc_types, or (b) probe WS. Also solana: "no endpoints for rpc type
  websocket" is real staking scarcity, not a bug.


## Latency-aware selection (parked — acceptable for now)

The mainnet-canary p50 sits ~40ms vs PATH ~30ms. It is NOT gateway overhead —
eth's in-cluster whole-chain p50 is 25ms; selection reads an in-memory cache
and signing/validation use the same shannon-sdk. The cause is that selection
is latency-agnostic by design: latency is report-only in scoring
(docs/scoring.md), so a slow-but-healthy supplier scores the same as a fast one
and gets equal tier-1 traffic. Measured on the canary: 100 eth suppliers took
client traffic, all score 100, per-supplier send latency 10ms–555ms, ~equal
traffic each — so the slow ones drag p50 up. PATH weights toward faster
suppliers.

Decision (2026-08-31): tolerable for now, do not reverse the report-only
stance. Later, close most of the gap cheaply with **a soft latency ceiling plus
a within-tier tiebreak**: keep latency OUT of the score (no instability), but
order selection within tier 1 by the per-key latency EWMA (already tracked),
and/or demote suppliers above the tier's p90 latency out of the front of tier 1
even when they succeed. This shifts traffic to the fast suppliers without
penalizing the slow-but-working ones in the score. Prototype the tiebreak in
the selector (reputation/selector) when picked up.

## 1. Parked follow-ups

None blocking. The mechanical items from the scoring v2 and admin passes
landed on 2026-08-29; what is left needs a decision.

- **Cross-validation is report-only, by decision on 2026-09-04.** The
  `cross_validation` flag defaults on and the validator records digests and
  logs outliers; nothing feeds reputation or the blocks. Making it act needs
  a design that says which of three disagreeing endpoints is wrong (majority
  of digests is not it: two lagging nodes outvote one fresh one on a moving
  chain), what signal the loser gets, and how a JSON-RPC error body that is
  the chain's own answer is kept out of the digest. Until then the flag is a
  logging switch and the docs say so.
- **Follower reputation never re-reads Redis after boot**, by decision on
  2026-09-04: probe results converge through the `sage:probes` stream, and
  traffic-derived scores stay per replica for the pod's lifetime. Revisit
  if canary graphs show replicas disagreeing on a supplier; the fix shape is
  a periodic follower re-hydrate adopting keys with a newer `UpdatedAt` and
  no local signal.

- `featureflag.MemoryStore` cannot tell config-seeded global flags from
  admin-set ones, so a config reload overwrites admin global flips. This is
  what the reload spec mandates; a tuning-style base/override split for flags
  would let admin flips survive a reload if that is ever wanted.
- Probe increments of `sage_reputation_attempts_total` dropped from N per URL
  to 1 per backend at the v2 deploy. Intended (ruling F1); dashboards built on
  the old count will show a step.
- Option B from the tier-2 decision (`docs/scoring.md` §7.7): a cheaper
  critical on a key scoring 95 or more. Parked unless the one-blip duty cycle
  is still visible with the trickle on.

From the mainnet canary (2026-08-31), parked rather than done:

- **PATH's `active_health_checks.external` rule file.** Not fetched, by
  decision (`docs/path-compat.md`). What it has that the plugins do not:
  archival `eth_getBalance` probes on 22 EVM services and a websocket
  `eth_blockNumber`. Revisit if the canary shows archival routing is missed;
  if honoured, treat the file's `check_interval` as never faster than the
  global `interval`.

From the 2026-08-31 end-to-end read (see the standing caveat below), verified
but not fixed (the SYN-blackhole grading and the drain SCAN cost from the same
list were fixed the same day):

- ~~`LeaderElector` elects and nothing reads it.~~ Closed 2026-08-31: probe
  once, apply everywhere (`docs/design/specs/2026-08-31-probe-once-design.md`).
  Only the leader sends probe relays; it publishes every result to the
  `sage:probes` Redis stream and every replica applies them, so followers
  have the same reputation and block heights without spending a relay. The
  write-behind reputation store is leader-only now. The one hook a sharded
  assignment (each live replica probes its share) would replace is
  `healthcheck.Leader`; the stream already supports it.
- A config reload applies `feature_flags` through `FlagStore.Set`, which on
  the Redis store writes the fleet-wide global key — one replica's file edit
  reaches every replica, and a global flip an admin set through the API is
  overwritten without a warning. Accepted on 2026-08-31 on the grounds that
  every replica runs the same file; the tuning-style base/override split in
  the first item above is the fix if that stops being true.

## WebSocket liveness (closed 2026-08-31)

PATH's `feat/ws-stall-watchdog` was assessed for port. It is not an unmerged
branch: the single commit on it (`f5b14d65`, 2026-07-19) went into PATH main
with #522 on 2026-07-20, on top of the session-rebind work. What it does: a
ticker on the bridge goroutine tracks the last client-facing data frame; when
the client holds an established subscription and 60 s pass with no data, it
raises a synthetic disconnect that the rebind path handles by reselecting a
*different* supplier, and after three such rebinds with no data it closes the
client with 1012.

Decision: **defer with the rebind.** Every part of it that acts is the rebind
(the supplier-avoiding reconnect, the replay, the give-up cap over rebinds),
and every part that decides needs the subscription registry (`HasActiveSubscriptions`
is what stops it closing an idle but legitimate connection). SAGE now has the
registry (2026-08-31: `qos.SubscriptionRegistry`, fed by each plugin's
`qos.SubscriptionClassifier` from inside `ws_processor.go`, keeping the original
subscribe frame for a replay; beta-checked on a CometBFT `NewBlock`
subscription). The rebind followed the same day
(`docs/design/specs/2026-08-31-ws-rebind-design.md`): an endpoint loss —
close, write failure, or the 60 s unresponsive verdict — swaps in a supplier
this connection has not used, replays the live subscriptions under
gateway-owned request ids, consumes the acks and rewrites subscription ids
both ways, up to three times per connection, then 1012. Rebind itself is not
reproducible on beta (one live host); the bridge tests are the proof. The
watchdog followed the same day (60 s of silence on live subscriptions →
the rebind path, `sage_websocket_stalls_total`), and session rollover became
a rebind too — beta showed the relay miner closing its socket at the
boundary. `POST /admin/websocket/rebind/{service}` forces it for a drill or
after a drain. The WS liveness item is closed.

What SAGE does have, and what it lacks, so the next reader does not re-derive
it:

- A silent-stall's blast radius is bounded by the session: `WSRelayer.watchSessionExpiry`
  closes the bridge at the session boundary with 1012, and the client's
  reconnect selects afresh. PATH needed the watchdog *because* rebind had made
  its connections outlive sessions.
- ~~SAGE's bridge has no transport liveness at all.~~ Closed 2026-08-31: the
  bridge pings both peers every 20 s and declares a side gone after 60 s with
  no data and no pong (`websockets/connection.go`, `WithLiveness`); the
  client gets 1012 so its reconnect draws a fresh supplier. Same day: the WS
  path got its first metrics — `sage_websocket_connections`, `_frames_total`,
  `_bytes_total`, `_closes_total{initiator,code}`, `_unresponsive_total{side}`,
  `_rejected_total{reason}`. Beta-checked with a 90 s `tm.event='NewBlock'`
  subscription (30 s gaps between frames): pongs kept it open, counters and
  gauge correct, client close counted as 1000.
- If a standalone stall detector is ever wanted without the rebind, the shape
  is: detect (needs per-connection knowledge of an established subscription,
  chain-specific — `eth_subscribe` result, Solana `*Subscribe`, CometBFT
  `subscribe`), then record a major signal against the endpoint and close the
  client with 1012, so the client's reconnect draws a different supplier
  through reputation. That is PATH's give-up branch alone; it is worth having
  only if stalls are observed inside a session's lifetime.

## Standing caveat

Roughly 70 commits landed on main between 2026-08-25 and 2026-08-27 through
subagent-driven builds (method-aware blocks, admin pass, scoring v2). Every
task was reviewed and gated by tests and lint, and each branch was
beta-validated before its squash merge.

On 2026-08-31 the three squash diffs (`f8bd974`, `e1bbbaf`, `7c65bc5`) were
read end-to-end — every non-test line, tests skimmed — by an agent session,
not a person. Nothing introduced by the three commits was found wrong on
main. Two pre-existing hedge bugs (`d5a695a`, 2026-07-23) that they built on
were found and fixed: a both-arms-fail hedge did not merge the primary's
context back, so Retry excluded `""` and could redraw the failed endpoint;
and Hedge's wait ignored the request context, so the per-service
`relay_timeout` (and its tuning knob) did nothing while hedging was on. A
release-during-refresh resurrection in the drain store (one tick) was closed
alongside the set-side fix that already existed. An automated review pass over
the same three diffs added three more, all fixed the same day: `method_blocks`
filtered only a list `circuit_break` happened to fetch, so the filter stopped
applying with `circuit_breaker` flagged off (it now fetches for itself); three
of the five `methodUnsupportedPatterns` wordings were unreachable at -32000
and graded a minor supplier error instead of a method block; and the `_other`
bucket could be marked by a client-attributed -32601, diverting every
uncatalogued method for everyone (it is now neither filtered nor marked). A
human read remains worth scheduling before a public release; it is no longer
the only read.

The hedge-off residual (retry attempts sharing the request deadline, so a
late attempt was graded `transport_timeout` on a sliver of budget) was closed
the same day: Retry does not start an attempt with less than a fifth of the
deadline budget left.
