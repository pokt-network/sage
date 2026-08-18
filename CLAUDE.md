# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

SAGE is a Go blockchain gateway that routes client RPC requests through Pocket Network's Shannon protocol. For the full design (middleware chain, QoS plugins, reputation, heuristic tiers, etc.), read `ARCHITECTURE.md` — it is the source of truth for "why things are the way they are." This file only covers what isn't already there.

## Commands

All common tasks go through `make`:

- `make sage_build` — build binary to `bin/sagegw` (CGO disabled)
- `make sage_run CONFIG_PATH=path/to/config.yaml` — run from source
- `make test_unit` — `go test ./... -short -count=1 -race` (the canonical unit test run)
- `make test_all` — drop `-short`; includes slower/integration-flavored tests
- `make test_cover` — coverage report
- `make go_lint` — `golangci-lint run --timeout 5m`
- `make e2e_test` — runs `./e2e/...` with `-tags e2e` against `SAGE_URL` (default `http://localhost:3069`); requires a running gateway
- `make integration_test` — `./protocol/shannon/... -tags integration`; requires a live fullnode and `SAGE_CONFIG`
- `make docker_build` / `make docker_run CONFIG_PATH=…`

Single test: `go test ./relay/middleware/ -run TestRetry -race -count=1 -v`. Use `-count=1` to bypass the test cache.

Go toolchain: `go 1.26.5` (see `go.mod`).

## Entry Point & Wiring

- `cmd/sagegw/main.go` — CLI, config load, lifecycle (SIGINT/SIGTERM, 10s graceful shutdown).
- `cmd/sagegw/wire.go` — `Build(ctx, cfg, logger) (*App, error)` is the single place the whole dependency graph is constructed. When adding a new middleware, QoS plugin, or background service, wire it here and register it in the appropriate registry (e.g., `MiddlewareRegistry`, QoS plugin registry).
- Config is loaded from `-config <path>` or the `GATEWAY_CONFIG` env var.

## Key Conventions

- **One concern per middleware**: each file in `relay/middleware/` is ~50–150 lines, wraps `next.HandleRelay(ctx)`, and is registered by name. Tests live next to the file (`<name>_test.go`).
- **Chain order is driven by YAML — don't hard-code it**: add a middleware by registering a name on the `relay.MiddlewareRegistry` in `cmd/sagegw.Build`, then naming it in `gateway_config.middleware_chain`. Omitting the key takes `relay.DefaultChainOrder()`. An unknown name is a startup error listing what *is* registered; a duplicate registration is an error, not an overwrite. A name `relay/chain_order.go` doesn't know is allowed anywhere in the chain — which also means it gets no ordering protection, so if your middleware must sit before another, add a `mustPrecede` rule rather than trusting the default order to hold.
- **Middleware ordering matters**: see the chain in `ARCHITECTURE.md`. `SelectEndpoint` runs inside `Retry`/`Hedge` so rotation works; heuristic runs after `SendRelay`. Don't rearrange without understanding why.
- **Config uses value types, no `*bool`/`*int`**: zero values are sensible defaults. Don't introduce pointer fields to represent "unset." Choose the zero value so that "unconfigured" is the safe state — `pprof_addr: ""` means off, not `:6060` on every interface.
- **Config parsing is lenient but never silent**: SAGE must load a PATH config unmodified, so an unknown key is not an error. It is reported instead — `parse` collects keys with no matching field into `cfg.Ignored` (`config/load.go`), and `cmd/sagegw` warns on each at startup. If you add a config field SAGE parses but does not act on, you have built the bug this exists to catch. (`config/compat_test.go` only checks that a real PATH config's *values* parse, and skips entirely if that file is absent — it is not a guarantee of anything.)
- **Chain semantics belong to the QoS plugin, not to config**: config carries per-service values opaquely (e.g. `chain_id`); the plugin owns their format, validation, and comparison, via its own `Config.Validate` called at wire time. EVM chain IDs are hex and compare numerically; CometBFT's are names that compare exactly. Neither rule generalizes, so neither belongs in `config/`.
- **QoS plugins**: the core `Plugin` interface has two methods (`ParseRequest`, `SelectEndpoints`). Everything else (block height, archival detection, caching TTLs, coalescing) is an **optional extension interface** — add capability by implementing the interface, not by modifying the core. Shared machinery lives in `qos/endpointstore.go`, `qos/blockconsensus.go`, `qos/selector.go`; new chains shouldn't duplicate it.
- **`ShouldRetry` vs `ShouldCircuitBreak` are independent** (see `heuristic/`). Retry alone must never escalate to domain-wide lockout — circuit breaking is explicit opt-in. This is a reproduction of a production bug from PATH; preserve the split.
- **`ErrorAttribution`** (Supplier / Blockchain / Client / Unknown) is part of every heuristic result. Blockchain-caused errors (e.g., `execution reverted`, `block not found`) must not penalize supplier reputation.
- **RPCType is immutable** through the middleware chain once set by `Parse`. Don't mutate it later.
- **gjson for Tier 2+ parsing** in `heuristic/`; byte-pattern matching is only acceptable for Tier 1 structural checks.
- **Feature flags** (`featureflag/`) gate most middleware. New runtime-toggleable behavior goes behind a flag (global + per-service override via admin API) rather than a config-only boolean. A flag is defined in **one** place — `featureflag.DefaultFlags`, built from the exported `Flag*` constants — and referenced by that constant, never a string literal. Config carries only the overrides an operator set (a `map[string]bool`); anything unset falls back to `DefaultFlags`. Adding a flag is one line there plus its constant; see *Adding a Middleware Module*.
- **Redis is optional**: the gateway must run in local-only mode when Redis is unreachable (`wire.go` degrades gracefully). Don't write code that hard-requires Redis on the hot path.
- **Observation pipeline is async and sampled** (10% of relays, 100% of health checks). Don't do deep parsing in the hot path; publish to `observe.Queue` instead.

## Relay Context

`relay.Context` (see `relay/context.go`) is the per-request state passed through the chain. Each field is set by **exactly one** middleware and read by others — keep that discipline when adding fields.

One field exists purely because of that discipline: `SelectedEndpoint` is an `*atomic.Pointer[domain.EndpointAddr]` that `SelectEndpoint` publishes its pick into, and it is non-nil only on hedge arms. Hedge waits out the hedge delay and then needs the primary arm's endpoint to steer the hedge elsewhere — reading `ctx.Endpoint` for that is a data race, because the arm writes it from its own goroutine with nothing ordering the write against the read. Hedge allocates a fresh slot per arm so the shallow `Clone()` cannot alias two arms onto one.

It is a flat struct of typed fields; there is no generic `values` bag, so carrying new state means adding a field. Before you do: **`Clone()` is a shallow copy**. Hedge racers and batch sub-relays each run on a clone, so any pointer, slice, or map field is *shared* between them — a field that looks per-request is per-request-tree, and writing to it from a sub-relay is a data race the `-race` suite will only catch if a test actually fans out. Scalars are safe; anything else needs an atomic (see `Degraded` merging in `middleware/batch.go`) or must be treated as read-only.

## Adding a Middleware Module

The full step-by-step recipe lives in **[`docs/middleware.md`](docs/middleware.md)** —
file, name, registration, feature flag, config, test — along with the traps
(shallow `Clone`, unregistered names getting no ordering protection, a flag
missing from `DefaultFlags` silently never running). Follow it rather than
reconstructing the steps here; that document and this file must not drift into
two versions of the same instructions.

Adding a chain instead of a middleware: **[`docs/qos-plugins.md`](docs/qos-plugins.md)**.

Two things worth repeating because they are easy to get wrong from memory:

- Per-client state comes from **`ctx.ClientIP`** (set by the `client_ip`
  middleware, trusted-proxy aware), not `HTTPRequest.RemoteAddr`, which behind a
  proxy is the proxy.
- A request's service comes from the `Target-Service-Id` header, resolved by
  `Parse` **and re-read in `router.go`** for the WebSocket path. Keep those two
  in sync if you touch service resolution.

## Documentation

Anything countable is generated; only reasoning is written by hand.

- `internal/docgen` generates `docs/configuration.md`, `docs/metrics.md` and
  `docs/admin-api.md` from the config structs, the metrics collectors and the
  router's mux. Run `make docs` after touching any of those three.
- A golden test in `internal/docgen` fails if the committed files are stale, and
  another fails if a config key has no doc comment. So: **a new config field
  needs a doc comment**, and the reference updates itself.
- `revive`'s `exported` and `package-comments` rules are on. Every package needs
  a package comment and every exported symbol a doc comment starting with its
  own name. Test files are exempt.
- Do not hand-write counts (test totals, line counts, route lists) into
  markdown. That is what the stale stats table in `ARCHITECTURE.md` was, and it
  drifted by thousands of lines while still looking authoritative.

## Tests

- Unit tests use the standard library + `testify`. The canonical run is `-short -race`; anything gated behind longer/integration-only needs a build tag (`e2e`, `integration`) matching the Makefile targets.
- E2E tests in `e2e/` are designed to run against **both SAGE and PATH** — keep them protocol-agnostic when modifying.
