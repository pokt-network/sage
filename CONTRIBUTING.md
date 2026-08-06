# Contributing to SAGE

Thanks for helping. SAGE is a Go gateway that routes client RPC requests to
blockchain suppliers on Pocket Network's Shannon protocol. Before you change a
file, three documents set the boundaries:

- **`README.md`** — what SAGE is and how to run it.
- **`ARCHITECTURE.md`** — the source of truth for *why things are the way they
  are*: the middleware chain, QoS plugins, reputation, heuristic tiers, circuit
  breaker, transports. Read the relevant section before touching a subsystem.
- **`CLAUDE.md`** — the repo conventions and the step-by-step **Adding a
  Middleware Module** recipe. Much of what looks incidental is load-bearing, and
  this file says which.

## The shape of the codebase

SAGE is a **composable middleware chain**, not a monolith. Every relay flows
through small, single-concern middleware, and chain-specific logic (EVM, Cosmos,
Solana) lives in QoS plugins behind a two-method interface. The seam for
extending the gateway is the middleware layer — you should rarely need to touch
protocol code.

To add behavior, follow **`CLAUDE.md` → Adding a Middleware Module**: one file in
`relay/middleware/`, a name in `relay/chain_order.go`, registration in
`cmd/sagegw.Build` (`wire.go`), an optional feature flag, config, and a test next
to the file. The `Build` drift guard fails if a `DefaultChainOrder` name isn't
registered.

## Conventions that bite if ignored

These are in `CLAUDE.md` in full; the ones that most often trip people up:

- **Chain order is data, not code.** Middleware order comes from
  `gateway_config.middleware_chain`, falling back to `DefaultChainOrder()`. If
  order is load-bearing, add a `mustPrecede` rule — an unknown name gets *no*
  ordering protection.
- **`relay.Context.Clone()` is a shallow copy.** Hedge racers and batch
  sub-relays run on a clone, so any pointer/slice/map field is *shared*. Scalars
  are safe; anything else needs an atomic or must be read-only.
- **Config uses value types, no `*bool`/`*int`.** Choose the zero value so
  "unconfigured" is the safe state (`pprof_addr: ""` means off).
- **A config field SAGE parses but never reads is a bug** — it lands in
  `cfg.Ignored` and warns at startup. Wire it through or don't add it.
- **Feature flags live in one place** — `featureflag.DefaultFlags`, referenced by
  a `Flag*` constant, never a string literal.
- **`ShouldRetry` and `ShouldCircuitBreak` are independent.** Retry alone must
  never escalate to a domain-wide lockout — preserve the split.
- **`ErrorAttribution` matters.** Blockchain-caused errors (`execution reverted`,
  `block not found`) must not penalize supplier reputation.
- **Redis is optional.** The gateway must run local-only when Redis is
  unreachable. Never hard-require it on the hot path.
- **The observation pipeline is async and sampled.** Don't do deep parsing in the
  hot path; publish to `observe.Queue`.

## SAGE ↔ PATH

SAGE is a restructured successor to PATH and loads a PATH config unmodified. Some
behavior reproduces a production bug from PATH on purpose (e.g. the
retry/circuit-break split) — `CLAUDE.md` flags these. If you fix a bug that also
exists in PATH, say so in the PR.

## Development

```sh
make sage_build         # -> bin/sagegw (CGO off)
make test_unit          # go test ./... -short -count=1 -race   (canonical)
make test_all           # drop -short; slower/integration-flavored tests
make go_lint            # golangci-lint
go vet ./...
```

A single test:

```sh
go test ./relay/middleware/ -run TestRetry -race -count=1 -v
```

`-count=1` bypasses the test cache. `golangci-lint` is required for `make go_lint`
([install](https://golangci-lint.run/welcome/install/)); CI runs it too.

Integration and e2e runs need a live environment and their build tags —
`make integration_test` (a fullnode + `SAGE_CONFIG`) and `make e2e_test` (a
running gateway at `SAGE_URL`). The e2e suite is written to run against **both
SAGE and PATH** — keep it protocol-agnostic.

## Documentation

Anything countable is generated; only reasoning is hand-written.

- `make docs` regenerates [`docs/configuration.md`](docs/configuration.md),
  [`docs/metrics.md`](docs/metrics.md) and
  [`docs/admin-api.md`](docs/admin-api.md) from the config structs, the metrics
  collectors and the router's mux. Run it after touching any of the three; a
  golden test fails the build if the committed files are stale.
- **A new config field needs a doc comment.** A separate test fails if any key
  reaches the reference undescribed.
- Every package needs a package comment and every exported symbol a doc comment
  — `revive`'s `package-comments` and `exported` rules are enabled. Test files
  are exempt.
- Do not hand-write counts into markdown. `ARCHITECTURE.md` used to carry a
  stats table that drifted several thousand lines from the truth while still
  reading as authoritative.

The extension guides are [`docs/middleware.md`](docs/middleware.md) and
[`docs/qos-plugins.md`](docs/qos-plugins.md); operator-facing material is
[`docs/operations.md`](docs/operations.md).

## Testing expectations

- **Race-clean.** `-race` is on in `make test_unit`; keep it green. A `Clone`
  data race only surfaces if a test actually fans out (hedge/batch) — add one
  when your field is touched on a sub-relay.
- **Tests live next to the file.** `relay/middleware/ratelimit.go` →
  `ratelimit_test.go`.
- **Standard library + `testify`.** Anything gated behind a longer or
  integration-only run needs a build tag (`e2e`, `integration`) matching the
  Makefile targets.

## Security — non-negotiable

SAGE holds signing keys and every relay it sends spends staked POKT. See
`SECURITY.md` for the operational model; the rules that touch contributions:

- **Never commit `local/`** or any config containing a key. It is gitignored;
  keep it that way.
- **Never print or log a key.** Emit a length, never the string.
- **Don't move the admin API onto the public edge or default `pprof_addr` on.**
  The admin port is unauthenticated loopback; a heap dump holds signing keys.
  These defaults each close a specific attack — don't weaken them to make a test
  pass.

## Pull requests

- Branch off `main`. Keep PRs focused — one concern each.
- Run `make test_unit`, `go vet ./...`, and `make go_lint` before pushing.
- Explain the **why** in the description, not just the what. The git history
  favors reasoning over restatement — match it.
- New config fields: value types, safe zero value, yaml tags, and actually read
  somewhere (or they warn as `Ignored`).
- New runtime-toggleable behavior goes behind a feature flag, added in the one
  place — `featureflag.DefaultFlags`.

## Code of Conduct

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
