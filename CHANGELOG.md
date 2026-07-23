# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First public release. Everything below is what ships in it. `ARCHITECTURE.md` is
the source of truth for the design and the reasoning behind it.

### Added — core

- **Composable middleware chain.** Every relay flows through small, single-concern
  middleware (parse, validate, cache, retry, hedge, circuit-break, select
  endpoint, send, analyse). Order is driven by `gateway_config.middleware_chain`,
  falling back to `relay.DefaultChainOrder()`, with load-bearing invariants
  enforced at startup (e.g. endpoint selection sits inside retry so rotation
  works).
- **QoS plugin system.** Chain-specific logic (EVM, Cosmos/CometBFT, Solana) lives
  behind a two-method `Plugin` interface; block height, archival detection,
  caching, and coalescing are optional extension interfaces, so a new chain
  implements only what it needs.
- **Reputation.** Per-endpoint scoring with block-staleness as a signal;
  probation prepends to, never replaces, the healthy set.
- **Heuristic response analysis** with tiered parsing (byte-patterns for Tier 1
  structural checks, `gjson` for Tier 2+) and **`ErrorAttribution`** (Supplier /
  Blockchain / Client / Unknown) — blockchain-caused errors don't penalize
  suppliers.
- **Circuit breaker**, with `ShouldRetry` and `ShouldCircuitBreak` kept
  independent so a retry can never escalate to a domain-wide lockout.
- **Cross-validation**, **response caching**, and **request coalescing
  (singleflight)**.

### Added — transports (Shannon RPC types)

- **JSON-RPC** — HTTP POST at the supplier root.
- **REST** and **CometBFT** — path-addressed HTTP (CometBFT also via JSON-RPC
  method names over POST).
- **WebSocket** — bidirectional bridge including subscriptions.
- **gRPC** — a real gRPC call to the miner's relay service, native with an
  auto-detected **gRPC-Web fallback** (`protocol.grpc_mode`), with gRPC-aware
  response analysis (`grpc-status` drives retry/attribution) and client framing
  preserved on the way back.

### Added — operation

- **Three-port model by design**: public relays/health (`3069`), **loopback**
  unauthenticated admin API (`9091`), scrape-only Prometheus (`9090`). `pprof` is
  off unless set — heap dumps hold signing keys.
- **Feature flags** (`featureflag/`) gate most middleware — global default plus
  per-service override via the admin API — defined in one place
  (`featureflag.DefaultFlags`).
- **Health checks** (`/health`, `/ready`) and a config-driven health-check runner.
- **Observation pipeline** — async and sampled (10% of relays, 100% of health
  checks), publishing to `observe.Queue` off the hot path.
- **Redis optional**: with it, reputation, flags, and circuit-breaker state are
  shared across instances; without it, SAGE runs local-only and degrades
  gracefully.
- **Admin API** for runtime inspection and per-service toggles; supplier health
  timeline exposed as a per-endpoint ring buffer.
- **`client_ip` middleware** — trusted-proxy-aware `X-Forwarded-For`, exposed as
  `ctx.ClientIP` for per-client middleware to key on.
- **Graceful shutdown** (SIGINT/SIGTERM, 10s drain) and ldflags-stamped version
  info logged at startup.

### Added — config & compatibility

- **Loads a PATH config unmodified.** Parsing is lenient but never silent: an
  unknown key is collected into `cfg.Ignored` and warned at startup rather than
  dropped.
- **Value types throughout** (no `*bool`/`*int`) — the zero value is the safe
  default. Chain semantics (chain IDs, comparison rules) belong to the QoS plugin,
  validated at wire time, not to `config/`.

### Added — build & tooling

- `make sage_build` (CGO-free), `make test_unit` (`-short -race`), `make test_all`,
  `make go_lint`, `make test_cover`, `make docker_build` / `make docker_run`.
- In-process **mock backend** (`bench/mock-config.yaml`) to run and load-test the
  gateway with no fullnode or suppliers.
- E2E suite written to run against **both SAGE and PATH**; integration tests gated
  behind build tags.

### Validated

- **Beta TestNet** — SAGE served a PATH config **unmodified** and sustained a load
  run (~1000 RPS, ~29k relays) with zero client-attributed errors; the
  HTTP/JSON-RPC, REST, CometBFT, and WebSocket-subscription transports were
  exercised against a live service. gRPC relaying (native + gRPC-Web) is
  implemented and unit-tested; see `ARCHITECTURE.md → Transports → gRPC`.

### Notes

- SAGE is a restructured successor to **PATH**. 17 named bugs and operational
  issues from PATH production are structurally prevented — see
  `ARCHITECTURE.md → Production Lessons Baked Into Architecture`. Some behavior
  reproduces a PATH bug *on purpose* (the retry/circuit-break split); those spots
  are flagged in `CLAUDE.md`.

[Unreleased]: https://github.com/pokt-network/sage/commits/main
