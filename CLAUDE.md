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

Go toolchain: `go 1.26.1` (see `go.mod`).

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

It is a flat struct of typed fields; there is no generic `values` bag, so carrying new state means adding a field. Before you do: **`Clone()` is a shallow copy**. Hedge racers and batch sub-relays each run on a clone, so any pointer, slice, or map field is *shared* between them — a field that looks per-request is per-request-tree, and writing to it from a sub-relay is a data race the `-race` suite will only catch if a test actually fans out. Scalars are safe; anything else needs an atomic (see `Degraded` merging in `middleware/batch.go`) or must be treated as read-only.

## Adding a Middleware Module

A middleware is one file in `relay/middleware/` that wraps `next.HandleRelay(ctx)` with a single concern. This is the seam for extending the gateway without touching protocol code. To add one — say a `rate_limit` — the steps, in order:

1. **The file.** `relay/middleware/ratelimit.go`. The constructor returns `relay.Middleware` and captures its dependencies in the closure (look at any existing middleware for the shape). If it should be runtime-toggleable, gate it on a flag — bail to `next` when off:
   ```go
   func RateLimit(flags featureflag.FlagStore, /* deps */) relay.Middleware {
       return func(next relay.Handler) relay.Handler {
           return relay.HandlerFunc(func(ctx *relay.Context) error {
               if flags == nil || !flags.IsEnabled(ctx.Ctx, featureflag.FlagRateLimit, ctx.ServiceID) {
                   return next.HandleRelay(ctx)
               }
               // reject over-limit here; otherwise fall through
               return next.HandleRelay(ctx)
           })
       }
   }
   ```

2. **The name.** Add a `MW…` constant to `relay/chain_order.go` and place it in `DefaultChainOrder()` where you want it to run (first = outermost). If order is load-bearing — must run before or after another middleware — add a `mustPrecede` rule in the same file. An unrecognised name is allowed *anywhere* and gets **no** ordering protection, so a rule is the only thing that enforces "before X".

3. **Register it** in `cmd/sagegw.Build` (`wire.go`), next to the others:
   ```go
   mwReg.Register(relay.MWRateLimit, func() relay.Middleware { return middleware.RateLimit(flags, /* … */) })
   ```
   A registered name not in the chain warns at startup; a chain name not registered is a startup error listing what *is* registered. A `Build` test fails if a `DefaultChainOrder` name isn't registered — that's the drift guard.

4. **The flag** (if you added one). Add its name to `featureflag.DefaultFlags` with a default, and an exported `Flag…` constant next to it — **same file**, `featureflag/defaults.go`. That is the only place; config picks it up as a `map[string]bool` override automatically, and unset means "use the default". A name absent from `DefaultFlags` resolves to `false`, so a flag you forgot to add there silently never runs.

5. **Config** (if the module needs settings). Add fields to the relevant config struct with yaml tags, value types, safe zero value (see conventions). A field SAGE parses but never reads is reported via `cfg.Ignored` at startup — wire it through or it warns.

6. **Test** next to the file, `ratelimit_test.go`; canonical run is `-short -race`.

Per-client state? Read **`ctx.ClientIP`** (set by the `client_ip` middleware, trusted-proxy aware) rather than `HTTPRequest.RemoteAddr`, which behind a proxy is the proxy. **Redis is optional** — fail open when it's nil; never hard-require it on the hot path. A request's service comes from the `Target-Service-Id` header, resolved by `Parse` (and re-read in `router.go` for the WebSocket path — keep those two in sync if you touch service resolution).

## Tests

- Unit tests use the standard library + `testify`. The canonical run is `-short -race`; anything gated behind longer/integration-only needs a build tag (`e2e`, `integration`) matching the Makefile targets.
- E2E tests in `e2e/` are designed to run against **both SAGE and PATH** — keep them protocol-agnostic when modifying.
