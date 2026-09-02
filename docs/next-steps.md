# Next steps

The agreed work queue, so that any contributor or agent can pick up where the
last session left off without needing that session's context. Items are
ordered by priority within each section. Update this file when an item lands
or a decision changes it; delete items rather than marking them done, so the
file only ever lists open work.

Last updated: 2026-09-02 (mainnet canary at 1% traffic; overnight OOM watch
passed — reputation timeline keys flat at ~2.3k over 17 h. PATH `origin/main`
at `274e9791`, 2026-08-25).




## Reputation memory bound (landed 2026-09-01, verification open)

The canary's first incident: after 14.7 h on `64a7e72` one of two pods was
OOMKilled (exit 137, 1 Gi limit, `GOMEMLIMIT=900MiB`). Working set climbed
~100 MB/h linearly from start, hit the limit at 04–05 h, and both pods sat
pinned for eight hours before one lost. The heap profile put 76% of in-use
memory in `reputation.(*Timeline).Record`: 82k reputation keys on the 14 h
pod against 6k on a fresh one, and 82k × ~13 KB per key ring is 1.07 GB.

Root cause: the timeline bounded events per key (a ring of 100) but never
evicted a key, and the canary ran `key_granularity: per-supplier`, so a key
was a staked registration that rotates every session — the set grew with the
network for the life of the process. The score cache
(`pruneUninformative` + shard cap) and the exporter (per-service cap) had
bounds; the admin-only timeline had none. The Redis write-behind grew the
same way (`HLEN sage:reputation:` reached 119,567) and nothing reads it back.

Landed in `aa5d63a` + `b4673b6`, image `ghcr.io/pokt-network/sage:b4673b6`:

- `reputation.Timeline` drops keys idle past 1 h (`DefaultIdleTTL`: three
  ~20-minute sessions) and holds at most 16,384 keys, ≤ ~213 MB worst case.
- `State.UpdatedAt` is stamped on every write-behind; the leader runs
  `StaleDeleter.DeleteStale` every 5 min (HSCAN + batched HDEL on Redis, a
  map sweep in memory) with the same 1 h TTL. Unstamped fields count as
  stale, so the first sweep drains what pre-stamp pods left.
- `sage_reputation_timeline_keys` and `sage_reputation_keys{service_id}` show
  the bounds working. `sage_endpoint_reputation_score` exports only keys
  below the full score, capped 500 per service (was every key, 2,000): one
  pod was emitting 104k series, 2.3% of the Prometheus head.
- Canary config moved to the default `per-url` (keys are backend URLs, which
  do not rotate). The PATH config's comment on `per-endpoint` ("each URL
  tracked separately") describes per-url; per-endpoint is the supplier × URL
  pair and grows the same way per-supplier did.

Open:

- **Verify on the canary.** Ops reports at 1 h and 3 h after the roll:
  `container_memory_working_set_bytes` flat within ±10% between the two
  readings is the pass line; `sage_reputation_timeline_keys` in the low
  thousands and flat after the first hour; series per pod well under 10k;
  `HLEN sage:reputation:` down from 119,567 within minutes of the leader
  coming up. Close this section when those land.
- ~~The reputation write-behind is write-only.~~ Resolved 2026-09-02 by the
  canary restart this was waiting for. A rolled pod covered 26 of 73 services
  after six minutes and was killed by its startup probe, so the warm-up read
  won over dropping the store: `reputation.Hydrate` loads `sage:reputation:`
  once at startup through the `Storage.GetStates` seam, and
  `healthcheck.Executor.SeedCoverage` credits the services it loaded towards
  the warm gate. Bounds: a state older than the idle TTL or carrying no
  `UpdatedAt` is skipped (the sweep's own rule, so a pod never adopts what is
  about to be deleted), live state is never overwritten, and the per-shard cap
  still applies. Storage is now read exactly once per process — a score that
  changes in the store mid-life still reaches nobody.
- ~~Other ever-seen maps.~~ Audited 2026-09-01: every map keyed by endpoint,
  supplier, URL, host, session or method on the relay or probe path has a
  bound — a sweep, a cap, a whole-map reset or a TTL. Three residuals, none
  growing per session, recorded rather than fixed: `grpcRelayTransport.conns`
  holds one `*grpc.ClientConn` per gRPC host ever relayed to and never closes
  one (bounded by hosts, but each is a live connection — idle eviction if it
  shows in a profile); `WSRelayer.activeLoad` keeps an 8-byte counter per
  endpoint ever bound and never deletes at zero (bounded by the endpoint
  set); `methodblock` marks are per (host, method) with method from the
  client, bounded only by the TTL — a client can inflate it for one TTL.

## Explore next (raised 2026-09-01)

- **Verify the request_type split and the 408 fix on the canary** (both landed
  2026-09-02, not yet imaged). After the roll: `sage_relay_total` should carry
  a `request_type="probe"` series at roughly the probe rate, and the 408 share
  of `sage_client_requests_total` should fall from the 0.89% measured over 17 h
  on `cac8818` as those relays rotate to another supplier instead of being
  handed back. Watch `sage_retry_total` for the offsetting rise, and check the
  408 suppliers actually lose score rather than the retries just costing more
  relays.
- **Split the >10s latency tail by service_id.** Raised by ops 2026-09-02: 4.8%
  of `sage_relay_latency_seconds` observations land in `+Inf` over a 17 h
  window, so the merged p99 is above 10 s. The label is already there, so this
  is a dashboard query, not a code change — but note the histogram uses
  `prometheus.DefBuckets`, whose top finite bucket is 10 s, so nothing
  distinguishes 11 s from 300 s. If the tail turns out to matter, the buckets
  need extending before it can be measured.

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

- **Traffic-informed probing.** A backend that served client relays within the
  last interval was graded by them (every attempt scores); probing it too buys
  a second copy of a fact the score middleware already has. Skipping those
  backends would cut probe spend roughly in proportion to traffic coverage,
  and it needs nothing new — the reputation store knows the last attempt time
  per key. Not done because the canary has no traffic yet to measure against.
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
