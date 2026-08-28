# Next steps

The agreed work queue, so that any contributor or agent can pick up where the
last session left off without needing that session's context. Items are
ordered by priority within each section. Update this file when an item lands
or a decision changes it; delete items rather than marking them done, so the
file only ever lists open work.

Last updated: 2026-08-28 (main at `7c65bc5`, PATH `origin/main` unchanged
since 2026-08-25, nothing to catch up).

## 1. Mainnet-shaped validation of the chronic-failure rate term

The scoring v2 rate term (`docs/scoring.md` §7.3) has never demoted anything
outside a simulation. Beta cannot exercise it: beta has a single live backend
and two DNS-dead hosts, so the only failures the term ever saw were floored
outages, not chronic low-rate violators.

Either:

- run a mainnet config for long enough to see the first demotion, or
- extend the mock backend (`bench/mock-config.yaml`, `protocol/mock`) to inject
  a chronic ~0.2% failure rate on one endpoint while the others stay clean, and
  watch that endpoint drop to tier 2.

The numbers to reproduce are the ones the design was calibrated against, from
PATH's `EVIDENCE_EMPTY_RESPONSES_2026-08-19.md`: spacebelt 0.216% (must land in
tier 2), rpcgate 0.065% (must keep tier 1), nodefleet 0.00003% (untouched). A
6-attempt burst must cost about -0.4, not a demotion. See `docs/scoring.md` §7
for the penalty curve (0 at or below 0.02%, -40 at 1%, -70 at or above 10%,
half-life 20k attempts).

## 2. Assess PATH's `feat/ws-stall-watchdog` for port

PATH has an unmerged branch adding a data-staleness watchdog for WebSocket
subscriptions: a supplier that stays connected but silently stops sending
frames is detected and the session is rebound. Its sibling
`feat/ws-session-rebind` is the session-rollover-with-subscription-replay work
that SAGE deliberately left out of v1 scope.

Decide whether the watchdog stands alone (port it) or depends on the rebind
(defer with the rebind). Follow the SAGE ↔ PATH sync procedure in
`CONTRIBUTING.md`; check the branch, not `origin/main`, since neither has
merged.

## 3. Parked follow-ups from scoring v2

None blocking. Recorded so they are not rediscovered from scratch.

- `AnalysisResult.IsSuccess()` predicate instead of comparing
  `Reason == ReasonSuccess` at call sites.
- `OnceRecorder` fallback branch has no test.
- `TieredSelectionConfig` struct doc wording.
- `docs/configuration.md` cannot mark `defaults.reputation_config` as inert,
  because docgen keys on the Go field; the inert status is only stated in
  `docs/scoring.md`.
- With the scoring flag off, the legacy path still grades a blockchain-caused
  retry error as a minor supplier error (the flag-on path records success per
  the attribution invariant in `CLAUDE.md`).
- Probe increments of `sage_reputation_attempts_total` dropped from N per URL
  to 1 per backend at the v2 deploy. Intended (ruling F1); dashboards built on
  the old count will show a step.

## 4. Parked follow-ups from the admin pass

None blocking.

- `featureflag/redis.go:157` still uses `KEYS` on its refresh loop; apply the
  same `SCAN` fix the drain store uses (every replica runs this every 5s on a
  shared Redis).
- `BlockConsensus.SetExternalFloor` stores outside the consensus lock. Floor
  only, so it cannot resurrect a reset perceived height, but it is the one
  remaining store not under the lock after the reset fix.
- Drain `matched_endpoints` is counted after the filter, so a dry-run
  under-reports when some matches were already blocked for another reason.
- A drain `Set` landing mid-refresh can be wiped from the local cache for one
  tick before the next refresh restores it.
- `featureflag.MemoryStore` cannot tell config-seeded global flags from
  admin-set ones, so a config reload overwrites admin global flips. This is
  what the reload spec mandates; a tuning-style base/override split for flags
  would let admin flips survive a reload if that is ever wanted.
- Metrics help strings for the request-sample gauges do not mention "stale
  window" as a reason a series can be absent.

## Standing caveat

Roughly 70 commits landed on main between 2026-08-25 and 2026-08-27 through
subagent-driven builds (method-aware blocks, admin pass, scoring v2). Every
task was reviewed and gated by tests and lint, and each branch was
beta-validated before its squash merge, but the diffs have not been read
end-to-end by a person. A human read of `f8bd974`, `e1bbbaf` and `7c65bc5` is
worth scheduling before any public release.
