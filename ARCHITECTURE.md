# SAGE — Supplier-Aware Gateway Engine

SAGE is a blockchain gateway that routes client RPC requests through the Pocket Network's Shannon protocol to decentralized supplier endpoints. It is a restructured successor to PATH: the same protocol surface, a fraction of the code, and the operational machinery PATH lacked (see `docs/path-compat.md` for what diverges on purpose).

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
      [RequestID]    — correlation ID (X-Request-ID header)
        [ClientIP]   — resolve the attributed client address (trusted-proxy aware)
        [Metrics]    — Prometheus counters + latency histogram
          [Parse]    — extract service ID, detect RPC type, load QoS plugin
            [Validate]     — check RPC type is supported by service
              [Timeout]    — per-request deadline (after Parse: resolved per service)
                [Cache]    — LRU response cache for finalized data
                  [Batch]  — decompose batch → fan-out → recombine
                    [Singleflight] — coalesce identical concurrent requests
                      [Observe]    — async reputation + deep parsing
                        [CrossValidate] — response digest consensus
                          [Retry]  — retry with endpoint rotation
                            [Hedge]    — race primary vs delayed secondary
                              [Affinity] — sticky supplier after writes
                                [CircuitBreak] — skip broken domains
                                  [MethodBlocks] — skip hosts blocked for this method
                                    [SelectEndpoint] — reputation + QoS filtering
                                      [Score]        — one reputation signal per attempt
                                        [DebugLog]   — full request/response logging
                                          [Heuristic] — response quality analysis
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
7. **Operator domain blocklist** — `blocked_domains` in gateway config (widened at
   restart by `SAGE_BLOCKED_DOMAINS`). Unlike blacklisting and circuit breaking,
   which are *earned* by an endpoint's behaviour and expire, this is a standing
   operator decision: every endpoint at the named domain is refused for the named
   RPC types, on every service, and it does not yield when honouring it would
   empty the pool. Matched on the endpoint URL, so it survives session rollover
   without anyone re-applying it. Applied in `AvailableEndpoints` — the one place
   endpoints are handed out, so selection, retry/hedge/batch, WebSocket bind and
   health checks all inherit it — and re-checked in `SendRelay` so an address
   held from before the ban cannot be used.

### Reputation System

`reputation/` tracks endpoint quality with tiered selection:

- **Signals**: success (+5), minor error (-3), major error (-10), critical error (-25),
  fatal error (-50). Every delta is configurable via `signal_impacts`. Those five are
  the whole set — the latency- and staleness-derived types PATH also carried were
  deleted rather than left unwired, for the reasons in
  [`docs/scoring.md`](docs/scoring.md) §7.2.
- **Tiers**: T1 (score >= 80), T2 (>= 50), T3 (remaining above minimum)
- **Probation**: low-score endpoints get limited traffic for recovery. Probation endpoints are prepended to the healthy list, never replace it.
- **Timeline**: per-endpoint ring buffer of the last 100 events for debugging via admin API
- **Storage**: in-memory, with a write-behind copy pushed to Redis. The service never
  reads that copy back — a cache miss answers `initial_score` — so it is for external
  tooling and for a load path that does not exist yet, not for cross-pod sharing.

**A score has two terms** (`reputation/rate.go`, and **[`docs/scoring.md`](docs/scoring.md)
§7** for the decisions and the beta/mainnet data behind them). The *additive* term is
the running sum of the signal deltas above, clamped to `[0, 100]`; it reacts to outages
in seconds. The *chronic* term is a penalty derived from an EWMA failure rate, and
exists because the additive term cannot see a violator that fails a fraction of a
percent of the time — at `+5/-25` the break-even failure rate is one in six, so an
endpoint that fabricates a response on 0.2% of its traffic holds a perfect score
forever. The effective score the selector uses is `clamp(additive + chronic_penalty)`;
the admin listing reports all three, plus the attempt counts and whether anything but a
health check has ever graded the key. Signals are recorded **per relay attempt** by the
`score` middleware (flag `scoring_v2`), so a retry loser is charged for its own failure
rather than being erased by the attempt that rescued the request.

**Key granularity** (`reputation/key.go`, `reputation_config.key_granularity`) decides
what a score is attached to. An `EndpointAddr` is a (supplier, backend URL) pair, and
several staked suppliers routinely front one machine — on Pocket beta, `pnf-anvil` has
32 registrations behind a single `rm.beta.infra.pocket.network`. The default is
**per-URL**: score the backend, so a shared machine is learned to be broken once rather
than 32 times. `per-endpoint`, `per-domain` and `per-supplier` are available; coarser
than per-URL blends backends that fail independently. An unrecognised value is a
startup error, never a silent fallback.

**The RPC type is always part of the key**, at every granularity — keys look like
`<identity>|<rpc_type>`. A Shannon supplier stakes one service for several RPC types at
once and the relay miner routes each to a different backend, so a dead WebSocket backend
says nothing about that supplier's REST backend. Blending them lets one broken transport
eject an endpoint from traffic it was serving correctly, and the coarser the granularity
the wider that blast radius: at per-URL a single key would otherwise cover every
transport of every supplier fronting the URL. `ResetScore` deliberately clears all RPC
types — an operator resetting an endpoint means the endpoint, not one of its protocols.
The corollary: a health check only grades the RPC type its payload carries, so a
transport with no health check of its own is graded by live traffic alone.

**Pool-collapse guard**: when every endpoint scores below the minimum threshold, the
selector serves the least-bad one instead of nothing. Reputation ranks a pool; it must
never empty one. Returning nothing would surface as "no endpoint for service" — a total
outage produced by reputation alone, on suppliers that are all still reachable. The
guard fires a `degraded{tier="reputation_pool_collapse"}` metric so the condition is
visible rather than absorbed.

**Per-operator concentration cap** (`reputation/concentration.go`, flag
`operator_aware_selection`): bounds the fraction of a service's selections any single
operator (eTLD+1) receives, water-filling the excess across the others. Selection is
registration-weighted by construction — a candidate is a supplier registration, which is
what the chain settles against — so the cap only bounds blast radius, it does not
reweight. Three rules make that safe:

- `max_operator_share`, default **0.50**, is the knob.
- **Displacement ceiling 3×**: a receiver cannot be pushed past 3× its own entitlement,
  because each registration carries a per-session allowance and exceeding it produces
  429s, not served relays. Excess nobody can absorb stays with the capped operator.
- **Two-operator pools use 0.65**: `0.50 × 2 = 1.0` is the infeasibility boundary, so the
  tighter cap could only ever force an even split.

The same flag makes retry and hedge prefer a *different operator*, not merely a different
endpoint — two hostnames run by one provider share a rack, an upstream, and an outage, so
avoiding only the failed endpoint defeats the purpose of a second attempt. It is a
preference: when no other operator is available, both fall back to their previous
behavior rather than giving up the attempt.

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

**Breaking is gated on failure RATE, not the first error.** A domain must fail at
least 5 times within a 30s window *and* fail at least 20% of its attempts in that
window before it is removed. First-error breaking is volume-sensitive rather than
quality-sensitive: the operator serving the most traffic reaches its first error
soonest after every TTL expiry, so the healthiest high-volume host is the one broken
most often — and a hostname fronting many endpoints takes a large share of the pool
down with it. The `CircuitBreak` middleware therefore reports *both* outcomes: a
`ShouldCircuitBreak` result as a candidate failure, and a clean relay as the success
that forms the denominator. A relay that failed without asking for a break counts as
neither.

Escalation counts **episodes**, not marks. Duplicate marks arriving during a break
already in effect (batch sub-relays, hedge arms) neither escalate the TTL nor extend
the expiry; the hit count rises only when a domain breaks again after being let back
in, remembered for 60 minutes past the break's own expiry. `Clear` drops that history
along with the break, so undoing a false positive does not leave the domain primed to
re-break as a repeat offender.

### Feature Flags

`featureflag/` provides runtime toggles without redeployment:

| Flag | Default | What It Controls |
|---|---|---|
| retry | on | Retry with endpoint rotation |
| hedge | on | Parallel race (primary + delayed secondary) |
| circuit_breaker | on | Domain-wide broken tracking |
| method_blocks | on | Per-host, per-method memory: a host that timed out on a method stops receiving it for a TTL |
| singleflight | on | Coalesce identical concurrent requests |
| cache | on | LRU response cache for finalized data |
| cross_validation | on | Cross-endpoint response consensus |
| heuristic | on | Response quality analysis. Gates body analysis only: transport errors are graded on the way out regardless of the flag, because attribution is what the breaker, the method blocks and reputation key on |
| observation_pipeline | on | Async deep parsing |
| health_checks | on | Active endpoint health checks |
| tracing | off | OpenTelemetry spans |
| supplier_affinity | on | Sticky supplier after write operations |
| websocket_relays | on | WebSocket relay path (bidirectional bridge) |
| operator_aware_selection | on | Per-operator concentration cap; operator-aware retry/hedge |
| debug_log | off | Full request/response body logging |
| shadow_mode | off | Process traffic but don't serve responses |
| request_sampler | on | Per-service request-shape sampling for diversity metrics and the admin request-sample routes |
| scoring_v2 | on | Per-attempt reputation scoring: the score middleware records each attempt against its own endpoint; batch collapses to one signal per endpoint; Observe records nothing. Off restores once-per-request scoring in Observe |

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
- **Backend-URL dedup** (on by default, `active_health_checks.disable_backend_url_dedup`):
  one relay per unique backend URL rather than one per supplier, with a representative
  that rotates each cycle. A check measures the backend, not the registration pointing
  at it, so probing one machine through five suppliers costs 5× the relays for one
  machine's worth of information — and blurs the signal, since a backend sampled once
  per cycle shows an outage immediately while one sampled five times shows five copies
  of the same moment. Rotation matters because a relay spends the probing supplier's
  per-session allowance, and a registration that is never probed is never observed to
  be individually broken. Backend-derived results (block height, the pass/fail grade)
  fan out to every supplier on that URL; a **transport error does not** — it can be the
  probing registration rather than the backend, and there is no response to tell them
  apart.

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

## HTTP surface

The full route list — relay, health, readiness and the admin API — is generated
from the router's own mux registrations into
[`docs/admin-api.md`](docs/admin-api.md). It is not repeated here, because a
hand-copied route table is a route table that eventually lies.

Two properties of that surface are architectural rather than incidental, and
belong here:

**Three listeners, not one.** Relays, the admin API and Prometheus each get
their own port. The admin API defaults to loopback, and binding it anywhere
else without an `admin_config.auth_token` (or `SAGE_ADMIN_TOKEN`) is refused
at startup — turning on `shadow_mode` alone stops the gateway answering
anything, and an admin surface reachable from off-host with no credential is
the same class of mistake. It used to share the relay port, which meant only
network topology stood between the internet and a control plane. See
[`docs/operations.md`](docs/operations.md).

Beyond the per-middleware controls above, the admin API is also where an
operator reaches for direct intervention: benching one supplier operator for
one service without touching the endpoints it happens to be serving right now
(operator drain, keyed on registrable domain so it survives session
rotation — see package `drain`); clearing a QoS plugin's cached chain state
(block height, sync status) for a service without touching its reputation
scores, when that state has drifted from the network's; reloading
`gateway_config` from the file the gateway booted with, either via
`POST /admin/reload` or a `SIGHUP` to the process, with a response that
separates settings applied live from settings that need a restart to take
effect; and reading back a live summary of a service's recent request shapes
(method/parameter diversity) from the request-shape sampler, gated by the
`request_sampler` feature flag above.

**The `/v1` mount point is the gateway's, not the service's.** The router strips it
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
| `websocket` | Bidirectional bridge, including subscriptions; ping/pong liveness on both sides (60 s silence = gone); a lost, stalled (60 s no data) or session-expired supplier is replaced under the live client connection and its subscriptions replayed (3 loss-rebinds, then 1012) |
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

SAGE parses the **same YAML config file** used by PATH, unmodified. Every key is
listed in [`docs/configuration.md`](docs/configuration.md), generated from the
config structs. Three design rules explain the shape of it:

**Value types, never pointers.** No `*bool`, no `*int`, so "unset" and "zero"
are the same state — and each zero value is chosen so the unconfigured state is
the safe one. `pprof_addr: ""` means pprof is off, not `:6060` on every
interface. Optional-by-pointer would mean a nil check at every read and a
default expressed nowhere in particular.

**Lenient, but never silent.** An unknown key is not an error — SAGE has to load
a PATH config describing features it does not have. It is collected into
`Config.Ignored` and warned about individually at startup. The failure this
guards against is a key that reads as configuration and does nothing.

**Chain semantics belong to the QoS plugin.** Config carries per-service values
opaquely; the plugin owns their format and comparison, validated at wire time.
EVM chain IDs are hex compared numerically, CometBFT's are names compared
exactly, and neither rule generalizes — so neither belongs in `config/`.

Note that the guard has a hole the generated reference now documents: a key with
a *declared* field parses without warning even when nothing reads it. Roughly a
third of the surface is in that state, inherited from PATH. See the "Parsed but
not implemented" section of the configuration reference.

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
  healthcheck/         — periodic probes (leader only; results streamed to every replica), leader election, external block sources
  websockets/          — generic bidirectional bridge with endpoint rebind, ping/pong liveness, Observer hook for metrics
  metrics/             — Prometheus metrics
  router/              — HTTP routing + admin API
  internal/docgen/     — generators for docs/ (build-time only, not in the binary)
  cmd/docgen/          — `make docs` entry point
  docs/                — generated reference + hand-written operator guides
  e2e/                 — end-to-end tests (works against SAGE and PATH)
```

---

## Build & Test

```bash
make sage_build      # Build binary to bin/sagegw
make test_unit       # Unit tests, short, with race detector
make e2e_test        # E2E suite (requires a running gateway at SAGE_URL)
make go_lint         # Run linters
make docs            # Regenerate the reference docs under docs/
make docker_build    # Build Docker image
```

---

## Reference documentation

Anything countable is generated from source rather than written here, because
this file previously carried a hand-maintained stats table that drifted several
thousand lines away from the truth while still reading as authoritative. The
generators live in `internal/docgen`, run via `make docs`, and are verified by a
golden test — so a config key added without documenting it fails CI.

| Document | Generated from |
|---|---|
| [`docs/configuration.md`](docs/configuration.md) | the config structs in `config/` |
| [`docs/metrics.md`](docs/metrics.md) | the collectors in `metrics/` |
| [`docs/admin-api.md`](docs/admin-api.md) | the mux registrations in `router/` |

The configuration reference also flags every key that parses into a field
nothing reads — inherited PATH keys that look live and are not.

This file keeps what a generator cannot produce: the reasoning.
