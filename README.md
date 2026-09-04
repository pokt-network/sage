# 🧙 SAGE — Supplier-Aware Gateway Engine

> A composable, PATH-compatible gateway for Pocket Network.

[![CI](https://github.com/pokt-network/sage/actions/workflows/ci.yml/badge.svg)](https://github.com/pokt-network/sage/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pokt-network/sage)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

SAGE is a Go gateway that routes client RPC requests to blockchain suppliers on
Pocket Network. A client sends a standard JSON-RPC request
(`eth_blockNumber`, a CometBFT `/status`, a Solana call); SAGE picks a healthy
supplier endpoint, signs and sends the relay, validates the response, and feeds
the result back into per-endpoint reputation so the next request routes better.

It is a restructured successor to PATH — same config format, a middleware-chain
core in place of PATH's monolith. It loads a PATH config unmodified.

## Features

- **Composable middleware chain** — every relay concern (parse, cache, retry,
  hedge, circuit-break, select, send, analyse) is one small file, ordered by
  YAML, toggleable per-service at runtime via feature flags.
- **QoS plugins** for EVM, Cosmos/CometBFT, and Solana behind a two-method
  interface; block height, archival detection, and caching are opt-in extensions
  a new chain implements only as needed.
- **Per-endpoint reputation & heuristics** — responses are graded and attributed
  (Supplier / Blockchain / Client), so a reverted call never penalizes a healthy
  supplier and routing improves with every relay.
- **Five transports** — JSON-RPC, REST, CometBFT, WebSocket (incl. subscriptions),
  and gRPC (native + gRPC-Web fallback).
- **Redis-optional** — shares reputation, flags, and circuit-breaker state across
  instances when present; runs local-only when not.
- **PATH-compatible, in three layers** — loads a PATH config unmodified; every
  key it does not honour is reported at startup; the client-visible contract
  (headers, error envelopes, health routes) is a table in
  [`docs/path-compat.md`](docs/path-compat.md), each row pinned by a test.

## Quick start

```bash
make sage_build                                 # → bin/sagegw (CGO disabled)
./bin/sagegw -config path/to/config.yaml        # or: make sage_run CONFIG_PATH=…
```

Config comes from `-config <path>` or the `GATEWAY_CONFIG` env var. To run
without a fullnode or suppliers — for load tests, or just to see it serve — use
the in-process mock backend:

```bash
./bin/sagegw -config bench/mock-config.yaml
curl -sX POST localhost:3069/v1 -H 'Target-Service-Id: eth' \
  --data-binary '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

A request names its service with the `Target-Service-Id` header. The gateway
listens on three separate ports, by design:

| Port (default) | Serves | Exposure |
|---|---|---|
| `3069` (`router_config.port`) | relays (`/v1`), health (`/health`, `/ready`) | **behind an authenticating, rate-limiting proxy** — SAGE does not authenticate clients, and every relay spends your stake |
| `9091` (`admin_config.addr`) | admin API (`/admin/*`) | **loopback** — unauthenticated, keep it off the public edge |
| `9090` (`metrics_config.prometheus_addr`) | Prometheus (`/metrics`) | scrape-only |

SAGE has no client authentication and no rate limiter of its own: it assumes an
edge in front of it, the way PATH assumes GUARD. An open `3069` is a stake drain,
not a misconfiguration — see [`SECURITY.md`](SECURITY.md).

`pprof` (`metrics_config.pprof_addr`) is **off** unless set — it hands out heap
dumps, which hold signing keys. Deployment, degraded modes and a runbook are in
[`docs/operations.md`](docs/operations.md).

## How it works

Every relay flows through a composable chain of middleware, each a small file
with one concern. Each wraps the next, so the chain reads as nesting — outermost
runs first:

```
HTTP Request
  [ClientIP]         resolve attributed client (trusted-proxy aware)
   [Parse]           service ID, RPC type, load QoS plugin
    [Cache]          LRU for finalized data
     [Retry]         retry with endpoint rotation
      [Hedge]        race primary vs delayed secondary
       [CircuitBreak]      skip broken domains
        [SelectEndpoint]   reputation + QoS filtering
         [Heuristic]       grade response, attribute the error
          [SendRelay]      sign, send, validate
```

*(abridged — the full chain and its ordering rules are in `ARCHITECTURE.md`.)*

The order is data, not code: it comes from `gateway_config.middleware_chain`,
falling back to `relay.DefaultChainOrder()`, and load-bearing invariants (e.g.
endpoint selection must sit inside retry so rotation works) are enforced at
startup.

Chain-specific logic (EVM, Cosmos, Solana) lives in QoS plugins behind a
two-method interface; everything else — block height, archival detection,
caching — is an optional extension interface, so a new chain implements only
what it needs. Redis is optional: with it, reputation, feature flags, and
circuit-breaker state are shared across instances; without it, SAGE runs
local-only.

`ARCHITECTURE.md` is the source of truth for the design and the reasoning
behind it. The reference docs — every config key, route and metric — are in
[`docs/`](docs/), generated from the source so they cannot drift.

## Extending it

SAGE is meant to be extended at the middleware layer without touching the
protocol code. Register a named middleware in `cmd/sagegw.Build`, name it in
`gateway_config.middleware_chain`, and it runs. The step-by-step recipe — file,
name, registration, feature flag, config, test — is in
**[`docs/middleware.md`](docs/middleware.md)**, along with the conventions worth
knowing before you start (the shallow `Context.Clone`, the single-source
feature-flag list, reading `ctx.ClientIP` for per-client state).

Adding a chain instead? **[`docs/qos-plugins.md`](docs/qos-plugins.md)**.

## Development

```bash
make test_unit          # go test ./... -short -count=1 -race   (canonical)
make test_all           # drop -short; slower/integration-flavored tests
make go_lint            # golangci-lint
make test_cover         # coverage report
make docs               # regenerate docs/ from source
```

Single test: `go test ./relay/middleware/ -run TestRetry -race -count=1 -v`
(`-count=1` bypasses the test cache).

Integration and e2e runs need a live environment and their build tags —
`make integration_test` (a fullnode + `SAGE_CONFIG`) and `make e2e_test` (a
running gateway at `SAGE_URL`); the e2e suite is written to run against both
SAGE and PATH.

Go toolchain: see `go.mod`.

## Contributing

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) — it covers the middleware recipe, the
conventions that bite if ignored (shallow `Context.Clone`, value-type config,
single-source feature flags), and how SAGE stays in sync with PATH. The full
design rationale is in [`ARCHITECTURE.md`](ARCHITECTURE.md), and repo doctrine in
[`CLAUDE.md`](CLAUDE.md). By participating you agree to the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Security

SAGE holds signing keys and every relay spends staked POKT. Do not open a public
issue for anything security-sensitive — see [`SECURITY.md`](SECURITY.md) for
private reporting and the operational security model (three-port layout, admin on
loopback, pprof off).

## License

[MIT](LICENSE) © Pocket Network. Release notes are in [`CHANGELOG.md`](CHANGELOG.md).
