# Next steps

The agreed work queue, so that any contributor or agent can pick up where the
last session left off without needing that session's context. Items are
ordered by priority within each section. Update this file when an item lands
or a decision changes it; delete items rather than marking them done, so the
file only ever lists open work.

Last updated: 2026-08-29 (main at `b1aec0d` + uncommitted parked cleanups, PATH `origin/main` unchanged
since 2026-08-25, nothing to catch up).

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

## WebSocket liveness (decided 2026-08-29, not scheduled)

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
is what stops it closing an idle but legitimate connection). SAGE has neither
— `websockets/bridge.go` is one endpoint per bridge for its lifetime, and
nothing in `protocol/shannon/ws_processor.go` parses `eth_subscribe` — so a
port is the rebind port plus this, not this alone.

What SAGE does have, and what it lacks, so the next reader does not re-derive
it:

- A silent-stall's blast radius is bounded by the session: `WSRelayer.watchSessionExpiry`
  closes the bridge at the session boundary with 1012, and the client's
  reconnect selects afresh. PATH needed the watchdog *because* rebind had made
  its connections outlive sessions.
- SAGE's bridge has **no transport liveness at all**: no ping/pong, no read
  deadline on either side (`websockets/connection.go` sets a read *limit* and a
  write deadline only). A half-open upstream TCP socket is noticed by the next
  write failure or the session boundary, whichever comes first. That is a
  larger gap than the data-staleness one, and cheaper to close — a read
  deadline refreshed by a pong handler is ~20 lines and needs no subscription
  model. Do this first if WS liveness becomes a problem.
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
beta-validated before its squash merge, but the diffs have not been read
end-to-end by a person. A human read of `f8bd974`, `e1bbbaf` and `7c65bc5` is
worth scheduling before any public release.
