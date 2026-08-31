# Next steps

The agreed work queue, so that any contributor or agent can pick up where the
last session left off without needing that session's context. Items are
ordered by priority within each section. Update this file when an item lands
or a decision changes it; delete items rather than marking them done, so the
file only ever lists open work.

Last updated: 2026-08-31 (mainnet canary up, one pod, no external traffic; probe
cadence knobs and `rpc_type_fallbacks` landed from its first hour of metrics
and logs. PATH `origin/main` at `274e9791`, 2026-08-25).

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

- **`blocked_suppliers` and `endpoint_policy`.** Two PATH keys the canary
  config carries that SAGE parses and ignores. `blocked_suppliers` (a list of
  supplier operator addresses per service; the config bans one on `poly`) has
  no SAGE equivalent — `blocked_domains` keys on URL domain and the drain on
  operator domain — so that supplier is eligible on the canary.
  `endpoint_policy` (`require_https`, `require_domain`) would reject plain
  `http://` and raw-IP stakes; none exist on mainnet today (0 of 686 endpoint
  URLs in the canary's first log). Both land as fields plus a filter next to
  the blacklist in `protocol/shannon/relayer.go endpoints()`. Next image.

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
