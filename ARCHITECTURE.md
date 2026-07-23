# SAGE — Supplier-Aware Gateway Engine

SAGE is a blockchain gateway that routes client RPC requests through the Pocket Network's Shannon protocol to decentralized supplier endpoints. It replaces PATH, reducing 43,000 lines to ~14,000 while adding 14 new capabilities.

**Detailed design plan**: [SAGE Architecture Plan](../../.claude/plans/polymorphic-singing-turtle.md)

---

## What SAGE Does

SAGE sits between clients and blockchain suppliers. A client sends a standard RPC request (e.g., `eth_blockNumber`), and SAGE:

1. Selects the best supplier endpoint based on reputation, block height, and health
2. Signs the request using Shannon protocol ring signatures
3. Relays it to the supplier through the Pocket Network
4. Validates the response (signature verification + heuristic analysis)
5. Returns the response to the client

SAGE supports 72 blockchain services across 4 chain types (EVM, Cosmos, Solana, generic) with JSON-RPC, REST, CometBFT, WebSocket and gRPC protocols — see [Transports](#transports).

---

## How It Works

### Middleware Chain Architecture

Every request flows through a composable chain of middleware components. Each middleware is a single file (~50-150 lines) that wraps a `next.HandleRelay(ctx)` call with one concern:

```
HTTP Request
  [Shadow]           — dry-run mode (process but don't serve)
    [Tracing]        — OpenTelemetry spans
      [Timeout]      — per-request deadline
        [RequestID]  — correlation ID (X-Request-ID header)
          [ClientIP] — resolve the attributed client address (trusted-proxy aware)
          [Metrics]  — Prometheus counters + latency histogram
            [Parse]  — extract service ID, detect RPC type, load QoS plugin
              [Validate]     — check RPC type is supported by service
                [Cache]      — LRU response cache for finalized data
                  [Batch]    — decompose batch → fan-out → recombine
                    [Singleflight] — coalesce identical concurrent requests
                      [Observe]    — async reputation + deep parsing
                        [CrossValidate] — response digest consensus
                          [Retry]  — retry with endpoint rotation
                            [Hedge]    — race primary vs delayed secondary
                              [Affinity] — sticky supplier after writes
                                [CircuitBreak] — skip broken domains
                                  [SelectEndpoint] — reputation + QoS filtering
                                    [DebugLog]     — full request/response logging
                                      [Heuristic]  — response quality analysis
                                        [SendRelay] — sign, send, validate
```

Each middleware can be **enabled/disabled at runtime** per-service via feature flags (Redis-backed, no redeploy needed).

### Core Abstraction

```go
type Handler interface {
    HandleRelay(ctx *Context) error
}

type Middleware func(next Handler) Handler

func Chain(handler Handler, middlewares ...Middleware) Handler
```

The middleware chain order is configurable via YAML. Middlewares are registered by name in a `MiddlewareRegistry` and assembled at startup.

---

## Key Components

### QoS Plugin System

Chain-specific logic is encapsulated in plugins. Adding a new blockchain requires implementing 2 methods:

```go
type Plugin interface {
    ParseRequest(ctx, req, body, rpcType) ([]Payload, error)
    SelectEndpoints(endpoints, payloads) (EndpointAddrList, error)
}
```

Optional extension interfaces add capabilities (block height tracking, archival detection, response caching, etc.) without changing the core.

| Plugin | Chain Type | Interfaces Implemented |
|---|---|---|
| `qos/evm/` | Ethereum, Polygon, Base, etc. | 10 (full feature set) |
| `qos/cosmos/` | Osmosis, Akash, Sei, etc. | 6 (dual block height formats) |
| `qos/solana/` | Solana | 7 (slot + epoch tracking) |
| `qos/noop/` | Generic (Near, Sui, Tron) | 2 (passthrough) |

All plugins share a generic `EndpointStore[T]` and `BlockConsensus` — no code duplication.

### Shannon Protocol Integration

`protocol/shannon/` handles the Pocket Network relay flow:

1. **Session management** — fetch/cache sessions from the Shannon fullnode via gRPC
2. **Endpoint extraction** — derive supplier endpoints with RPC-type URL mapping from session data
3. **Request signing** — ring signatures via the shannon-sdk (cached per app+session)
4. **Relay transport** — HTTP POST of signed protobuf to the supplier's relay miner
5. **Response validation** — verify supplier signature, extract payload
6. **Supplier blacklisting** — time-based blacklist for signature/validation failures (15min default)

### Reputation System

`reputation/` tracks endpoint quality with tiered selection:

- **Signals**: success (+5), minor error (-3), major (-10), critical (-25), fatal (-50), stale block (-15)
- **Tiers**: T1 (score >= 80), T2 (>= 50), T3 (remaining above minimum)
- **Probation**: low-score endpoints get limited traffic for recovery. Probation endpoints are prepended to the healthy list, never replace it.
- **Timeline**: per-endpoint ring buffer of the last 100 events for debugging via admin API
- **Storage**: in-memory with async Redis persistence for cross-pod sharing

### Heuristic Response Analysis

`heuristic/` uses 4-tier analysis to catch errors HTTP status codes miss:

| Tier | What | Examples |
|---|---|---|
| 0. HTTP Status | Status code | 5xx → retry + circuit break, 429 → retry only |
| 1. Structural | Response shape | Empty body, HTML error page, XML |
| 2. Protocol | JSON-RPC parsing (gjson) | Error codes, `result:null + error`, fabricated responses |
| 3. Indicators | Content patterns | "missing trie node", "connection refused" |

**Key architectural improvement**: `ShouldRetry` and `ShouldCircuitBreak` are independent decisions. Circuit breaking requires explicit opt-in — a retry alone never triggers domain-wide lockout.

**Error attribution**: Each result carries `ErrorAttribution` (Supplier / Blockchain / Client / Unknown). Blockchain-caused errors (execution reverted, block not found) don't penalize the supplier.

### Circuit Breaker

`circuitbreaker/` tracks broken domains across pods via Redis:

- Escalating TTL: 1min → 2min → 4min → 8min → 16min → 30min cap
- Local cache with lazy 5s refresh (zero Redis calls on hot path)
- Only triggered by `ShouldCircuitBreak`, never by `ShouldRetry` alone
- Admin API: `POST /admin/circuit-breaker/clear/{serviceId}`

### Feature Flags

`featureflag/` provides runtime toggles without redeployment:

| Flag | Default | What It Controls |
|---|---|---|
| retry | on | Retry with endpoint rotation |
| hedge | on | Parallel race (primary + delayed secondary) |
| circuit_breaker | on | Domain-wide broken tracking |
| singleflight | on | Coalesce identical concurrent requests |
| cache | on | LRU response cache for finalized data |
| cross_validation | on | Cross-endpoint response consensus |
| heuristic | on | Response quality analysis |
| observation_pipeline | on | Async deep parsing |
| health_checks | on | Active endpoint health checks |
| tracing | off | OpenTelemetry spans |
| supplier_affinity | on | Sticky supplier after write operations |
| websocket_relays | on | WebSocket relay path (bidirectional bridge) |
| debug_log | off | Full request/response body logging |
| shadow_mode | off | Process traffic but don't serve responses |

Flags can be toggled globally or per-service via admin API:
```
PUT /admin/flags/{flag}              — global toggle
PUT /admin/flags/{flag}/{serviceId}  — per-service override
```

### Health Checks

`healthcheck/` runs periodic synthetic probes:

- **Leader-elected**: only one pod runs health checks (Redis SET NX EX)
- **Worker pool**: bounded parallel checks across all services
- **Executor**: sends health check payloads through the full relay path, records reputation signals, feeds the observation pipeline
- **External block sources**: polls public RPCs for ground-truth block heights as a floor for perceived block number

Checks come from two places, and they add up rather than replace one another:

1. **QoS plugins** — every plugin implementing `qos.HealthChecker` supplies its
   own probes. These are what keep block height and chain ID tracking fed, so
   they always run.
2. **Config** — `active_health_checks.local[]` declares per-service checks in
   YAML (name, method, path, body, RPC type, `reputation_signal`). A rule that
   cannot be built is skipped with a startup warning, and a block that defines
   checks while `enabled` is unset is warned about too: a check that silently
   never runs reads to an operator exactly like a check that keeps passing.

Config never switches the plugin's checks off. A YAML block that could disable
them would degrade endpoint selection — no block heights, no chain ID assertion
— without saying so.

### Observation Pipeline

`observe/` provides async deep parsing without blocking the hot path:

- **Sampling**: 10% of relay responses are deep-parsed (health checks: 100%)
- **Worker pool**: configurable concurrency
- **Multi-instance**: Redis pub/sub shares observations across pods (JSON serialization, no proto)
- **Extracted data**: block height, chain ID, sync status, archival capability

### Cross-Validation

`crossvalidation/` detects bad actors via response consensus:

- Records response digests (SHA-256) per (service, method)
- Background goroutine runs majority consensus every 30s
- Endpoints disagreeing with the majority are flagged as outliers
- Minimum quorum of 3 before flagging

### Response Caching

`responsecache/` provides LRU caching for finalized/deterministic responses:

- Per-method TTL declared by QoS plugins (e.g., `eth_getTransactionReceipt` = 5min)
- Cache key: SHA-256 of serviceID + method + raw request payload
- Hit/miss/eviction statistics exposed via admin API

### Request Coalescing (Singleflight)

When multiple clients send identical requests concurrently (e.g., `eth_blockNumber`), only one relay is sent. Others receive the shared response. QoS plugins declare which methods are coalescable.

---

## Admin API

```
GET  /admin/flags                              — all feature flags
PUT  /admin/flags/{flag}                       — toggle globally
PUT  /admin/flags/{flag}/{serviceId}           — toggle per-service
GET  /admin/reputation/{serviceId}             — all endpoint scores
POST /admin/reputation/reset/{serviceId}/{ep}  — reset score
GET  /admin/timeline/{serviceId}               — endpoint event history
GET  /admin/timeline/{serviceId}/{endpoint}    — single endpoint timeline
POST /admin/circuit-breaker/clear/{serviceId}  — clear circuit breaker state
GET  /admin/circuit-breaker/{serviceId}        — view broken domains
GET  /admin/config                             — effective running config
```

---

## Operational Endpoints

```
GET  /health            — gateway health (200 OK / 503 Unavailable)
GET  /ready/{service}   — per-service readiness
GET  /ready             — all services readiness
POST /v1                — relay endpoint (requires Target-Service-Id header)
GET  /v1/{path...}      — REST/CometBFT relay (also handles WebSocket upgrade)
```

The `/v1` mount point is the gateway's, not the service's: the router strips it
before the chain runs. JSON-RPC does not care, but a REST, CometBFT or gRPC
request is *addressed* by its path, and relaying `/v1/status` rather than
`/status` is a 404 at the supplier's backend. The path (query string included),
the HTTP verb and the media type travel on the `Payload` and are replayed
verbatim by the relay miner.

---

## Transports

| RPC type | Sent to the supplier as |
|---|---|
| `json_rpc` | HTTP POST at the supplier root; the body is the whole request |
| `rest` | HTTP, path-addressed |
| `comet_bft` | HTTP, path-addressed, or JSON-RPC method names over POST |
| `websocket` | Bidirectional bridge, including subscriptions |
| `grpc` | A **gRPC call** to the miner's relay service — see below |

### gRPC

A gRPC relay is not an HTTP POST of the signed request. The relay miner routes
gRPC and HTTP down separate paths, and only the gRPC one reaches the h2c client
it uses to talk to gRPC backends; the HTTP path rebuilds the request as
HTTP/1.1, which a native gRPC backend rejects. So SAGE calls
`/pocket.service.RelayService/SendRelay` with the signed `RelayRequest` as the
message, and an `rpc-type` metadata header telling the miner which backend to
use.

That call goes out in one of two framings, chosen by `protocol.grpc_mode`:

| Mode | Behaviour |
|---|---|
| `""` (auto, default) | Try native once per supplier host, fall back to gRPC-Web and remember |
| `native` | Native gRPC over HTTP/2 |
| `web` | gRPC-Web over HTTP/1.1 |

Both exist because neither works everywhere. SAGE next to the relay miners can
speak native gRPC over h2c. SAGE reaching them through an ingress that
terminates HTTP/2 and forwards HTTP/1.1 cannot — the miner answers such a call
`505 gRPC requires HTTP/2` — but gRPC-Web goes through that same ingress
untouched, because it carries its trailers as a frame *inside* the body rather
than as HTTP trailers. Auto is the default because it is the only setting that
is right in both, and the fallback is keyed on the one error that means "wrong
framing" so a genuinely broken supplier is never silently re-tried as a
different protocol.

Two consequences worth knowing:

- **Response analysis is separate.** Every heuristic tier reads the body as
  text, and `IsPlainText` is true for anything not starting with `{`, `[` or
  `<` — so a correct protobuf reply would be graded `plain_text_response` and
  retried, penalizing suppliers for answering. `heuristic.AnalyzeGRPC` handles
  gRPC instead, reading the outcome from `grpc-status`.
- **`grpc-status` decides attribution.** A status about the *request*
  (`NOT_FOUND`, `INVALID_ARGUMENT`, `UNIMPLEMENTED`) is the chain's answer,
  delivered faithfully: no retry, no penalty. A status about the *serving*
  (`UNAVAILABLE`, `INTERNAL`) is the supplier's problem and costs it both. Same
  rule as `ErrorAttribution` elsewhere, applied to a different protocol.

The client's own framing is preserved on the way back: a gRPC-Web caller gets
the trailer frame it requires appended, a native caller does not.

---

## Configuration

SAGE parses the **same YAML config file** used by PATH. Internal representation uses value types (no pointer fields — zero values are sensible defaults). Config is validated at load time; unknown fields in critical sections produce errors.

---

## Production Lessons Baked Into Architecture

17 bugs and operational issues from PATH production are structurally prevented:

1. **Heuristic → circuit breaker amplification** — `ShouldRetry` and `ShouldCircuitBreak` are independent
2. **CometBFT protocol chameleon** — RPCType is immutable through the chain
3. **Reputation ignoring block staleness** — block staleness is a reputation signal
4. **Safety valve returning error** — graceful degradation tiers with X-Degraded header
5. **Probation without fallback** — probation prepends, never replaces healthy endpoints
6. **Retry/hedge bypassing QoS** — structurally impossible (middleware chain runs on every attempt)
7. **sync_allowance silently ignored** — strict config validation at load time
8. **Non-leader replicas start empty** — observation pub/sub shares state continuously
9. **Block height parsing bugs** — one parser per chain (plugin interface), uint64 everywhere, validation wrapper
10. **Deceptive supplier detection** — response format validation + cross-validation consensus
11. **`result:null` false positive** — gjson parsing for critical checks, null != result
12. **Supplier vs blockchain fault** — ErrorAttribution in heuristic results
13. **Byte-pattern matching pitfalls** — gjson for Tier 2+, byte patterns only for Tier 1
14. **Debug logging** — per-service runtime toggle via feature flag
15. **Supplier health timeline** — ring buffer per endpoint, exposed via admin API
16. **Middleware ordering** — configurable via YAML
17. **Shadow/dry-run mode** — validate with real traffic, zero risk

---

## Repository Structure

```
sage/
  cmd/sagegw/          — main binary (wire.go + main.go)
  domain/              — core types (zero dependencies)
  config/              — YAML config loader (same format as PATH)
  relay/               — middleware chain core (Handler, Context, Chain)
  relay/middleware/     — middleware components (one file each)
  protocol/            — transport interfaces (Relayer, EndpointProvider)
  protocol/shannon/    — Shannon protocol (signing, sessions, gRPC)
  qos/                 — plugin system + shared infra (EndpointStore[T], BlockConsensus)
  qos/evm/             — EVM plugin
  qos/cosmos/          — Cosmos plugin
  qos/solana/          — Solana plugin
  qos/noop/            — passthrough plugin
  reputation/          — scoring, tiered selection, timeline, storage
  heuristic/           — 4-tier response analysis
  circuitbreaker/      — Redis-backed domain circuit breaker
  featureflag/         — runtime per-service toggles
  observe/             — async observation pipeline
  crossvalidation/     — response consensus + outlier detection
  responsecache/       — LRU response cache with TTL
  healthcheck/         — periodic probes, leader election, external block sources
  websockets/          — generic bidirectional bridge (rotation-ready)
  metrics/             — Prometheus metrics
  router/              — HTTP routing + admin API
  e2e/                 — end-to-end tests (works against SAGE and PATH)
```

---

## Build & Test

```bash
make sage_build      # Build binary to bin/sagegw
make test_unit       # Run 596 unit tests with race detector
make e2e_test        # Run 13 E2E tests (requires SAGE_URL)
make go_lint         # Run linters
make docker_build    # Build Docker image
```

---

## Stats

| Metric | PATH | SAGE |
|---|---|---|
| Source lines | ~43,000 | 14,372 |
| Unit tests | ~200 | 596 |
| Packages | ~10 | 25 |
| God objects | 2 (2,935 + 1,809 lines) | 0 |
| Duplicated retry logic | 3 copies | 1 |
| Duplicated endpoint stores | 3 copies | 1 (generic) |
| Runtime feature toggles | 0 | 14 |
| Config pointer fields (*bool) | ~40 | 0 |
