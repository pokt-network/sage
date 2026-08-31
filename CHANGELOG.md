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
- **Redis optional**: with it, feature flags, operator drains and health-probe
  results are shared across replicas and only the elected leader sends probe
  relays; without it, SAGE runs local-only and degrades gracefully. Reputation,
  method blocks and circuit breakers are per replica by design.
- **Admin API** for runtime inspection and per-service toggles; supplier health
  timeline exposed as a per-endpoint ring buffer.
- **`client_ip` middleware** — trusted-proxy-aware `X-Forwarded-For`, exposed as
  `ctx.ClientIP` for per-client middleware to key on.
- **Graceful shutdown** (SIGINT/SIGTERM, 10s drain) and ldflags-stamped version
  info logged at startup.
- **`blocked_domains`** — a standing operator ban, distinct from the two things
  beside it: supplier blacklisting and circuit breaking are *earned* by an
  endpoint's behaviour and expire, while this is "not this infrastructure, not
  ever". Entries name a registrable domain or an exact hostname plus optional
  `rpc_types`; `SAGE_BLOCKED_DOMAINS` unions in more at restart and can only
  widen a ban. Matched on the endpoint URL, so it survives session rollover
  without anyone re-applying it, and applied where endpoints are handed out, so
  selection, retry/hedge/batch, WebSocket bind and health checks all inherit it.
  A malformed entry refuses to boot: a ban that silently covers less than it
  reads as covering is worse than no ban, because it is trusted.
- **Panic containment on every goroutine** (`internal/safego`). `net/http`
  recovers the goroutine serving a request; every `go` statement crosses that
  boundary, so a panic on a hedge arm would take the process down while the same
  panic without hedging cost one 500. Background work is contained and logged
  with a stack; request-shaped work (hedge arms, batch sub-relays) converts the
  panic to an error, because recovering *without delivering a result* hangs the
  request instead of crashing it. Surfaced as
  **`sage_recovered_panics_total`** — non-zero means a bug was contained, not
  that nothing happened.
- **Reputation scores are exported** as `sage_endpoint_reputation_score`, derived
  at scrape time rather than pushed: a pushed gauge keyed on an endpoint identity
  never evicts, and supplier registrations rotate every session.
  `sage_endpoint_reputation_scores_dropped` reports truncation, so a trimmed
  scrape reads as trimmed rather than as complete.

### Added — August 2026, after the beta validation

- **Method-aware blocks** (`method_blocks`): per-host, per-method memory at
  selection. A host that timed out on a method, or said it does not serve it,
  stops receiving that method for a TTL and keeps everything else; three
  supplier-attributed marks escalate to a host-wide block. Transport errors are
  graded on the way out (`heuristic.AnalyzeTransportError`), so a dead host
  reaches the circuit breaker and a client hang-up is nobody's signal — with
  the fact "never connected" observed through `httptrace`, not inferred from
  the error's shape.
- **Scoring v2**: one reputation signal per attempt (retry and hedge losers
  included; batches collapse to worst-of per endpoint), and a chronic-failure
  rate term beside the additive score. PATH's `signal_impacts`,
  `tiered_selection` and `min_threshold` keys are honoured; inconsistent
  thresholds warn, impossible ones refuse.
- **Admin pass**: bearer-token auth (`admin_config.auth_token` /
  `SAGE_ADMIN_TOKEN`, mandatory off loopback); **operator drain** (service ×
  operator × RPC type, Redis-shared, one HASH); **chain-state reset**;
  **config reload** (`POST /admin/reload`, `SIGHUP`) with an honest
  applied / needs-restart / warnings report; a **request-shape sampler**;
  runtime **tuning knobs**; an admin UI.
- **WebSocket**: ping/pong liveness on both sides; the first WS metrics; a
  **subscription registry** (EVM, Solana, CometBFT) that translates ids across
  suppliers; **rebind** — a lost, stalled (60 s of silence) or session-expired
  supplier is replaced under the live client connection with its subscriptions
  replayed; `POST /admin/websocket/rebind/{service}` forces it, for a drill or
  after a drain.
- **Probe once, apply everywhere**: only the elected leader sends health-check
  relays (each is paid for from the app's stake); every result goes through the
  `sage:probes` Redis stream and every replica applies it, so followers carry
  the same reputation and block heights without spending a relay.
- **Probe cadence knobs**: `active_health_checks.interval` (30 s unless set)
  and PATH's per-service `local[].check_interval`, now honoured; the EVM
  chain-id check runs every 5 minutes instead of every cycle (a chain id does
  not change), which halves an EVM service's probe spend on its own. Every
  probe is a paid relay: one idle pod on the mainnet config was spending ~58
  a second.
- **`rpc_type_fallbacks`** is live, with PATH's key and pool-level one-hop
  semantics: when no supplier staked the requested RPC type, selection uses
  the mapped type's URLs. Mainnet cosmos suppliers are still catching up on
  `comet_bft` stakes. (A first cut applied it per supplier and put tron's
  REST-only suppliers into its json_rpc pool — 405s on a fifth of tron
  responses on the canary; fixed the same day.)
- **Kubernetes probes**: `/healthz` (PATH's spelling of `/health`, readiness)
  and `/livez` (process liveness); on-demand container images from any branch
  (`image.yml`), tagged `<branch>-<sha7>` and `<sha7>`.

### Fixed — August 2026

- `timeout` now runs after `parse`, so per-service `timeout_config` and the
  `timeout.relay_timeout` tuning override apply (they resolved for service `""`
  before). A pinned `middleware_chain` in the old order is refused at startup.
- Hedge: a both-arms-fail no longer hides the failed endpoint from Retry, and
  the wait honours the request deadline (arms stay detached to flush).
- Retry does not start an attempt with less than a fifth of the deadline
  budget left.
- Drain refresh is one `HGETALL`, not a `SCAN` of the whole keyspace every
  tick; a release racing a refresh is not resurrected.
- The `_other` method bucket is never marked or filtered; three
  method-unsupported wordings that were unreachable now produce the block
  they were listed for.
- `sage_retry_total`, `sage_hedge_total`, `sage_cache_hits_total` and
  `sage_cache_misses_total` are now emitted — they were defined and documented
  but never incremented (no writer), so Prometheus had no series. Retry records
  a reason per attempt (`rollover`, a timeout, or the error kind); hedge records
  `primary_won` / `hedge_won` / `both_failed`.
- A session rollover between endpoint selection and send no longer 502s the
  client or penalizes the supplier: the selected endpoint being absent from
  the now-current session is treated as retryable, Retry reselects from the
  fresh session, and no reputation signal is recorded (no relay reached the
  supplier). Was ~1/s of hard errors on the mainnet canary, logged as ERROR
  and dragging supplier scores; now a Warn and a transparent retry.
- On a JSON-RPC request, a 4xx whose body is not a JSON-RPC envelope (an HTML
  404 page, an empty body) is graded as the supplier's HTTP layer, not the
  client's mistake: retried elsewhere, minor penalty, method-blocked on that
  host. A mainnet supplier answered 74% of its solana JSON-RPC posts with a
  404 page and both gateways passed it through; PATH still does.
- A retry verdict with no better attempt behind it now delivers the upstream's
  own response (the chain's `execution reverted`, the node's `block not
  found`) instead of a gateway-made `-32603`; a failed relay logs one ERROR
  line, not two. Found on the mainnet canary's first hour of traffic: ~1% of
  requests, invisible in `sage_relay_total`, which recorded the upstream 200.
- The cosmos health check reached json_rpc-only suppliers as `comet_bft` and
  graded them `minor_error` every cycle for a mismatch on SAGE's side (the
  plugin's JSON-RPC variant keyed on a store field nothing wrote). The
  fallback above is the fix; the dead variant is gone.
- Log noise from an idle canary: an unparseable probe body no longer logs two
  ERROR lines per relay; a service with no suppliers reports the session
  failure once, then again only when it changes or recovers.
- `TestTieredSelector_Tier1` no longer fails one run in twenty.

### Added — config & compatibility

- **Loads a PATH config unmodified.** Parsing is lenient but never silent: an
  unknown key is collected into `cfg.Ignored` and warned at startup rather than
  dropped.
- **Value types throughout** (no `*bool`/`*int`) — the zero value is the safe
  default. Chain semantics (chain IDs, comparison rules) belong to the QoS plugin,
  validated at wire time, not to `config/`.
- **`sync_allowance: 0` means the plugin's own default, and the plugins disagree
  about what that is, because their chains do.** EVM and Cosmos read it as "no
  block-height filtering" — a block there is seconds to tens of seconds, so an
  unset allowance costs a bounded amount of staleness. Solana reads it as 1500
  blocks (~10 minutes): at ~400ms per block, an unfiltered pool serves deeply
  stale state within minutes. Zero never means "require the chain tip", which
  would admit only whichever endpoint reported last and starve every other one
  of the traffic that keeps its height current.

### Added — build & tooling

- `make sage_build` (CGO-free), `make test_unit` (`-short -race`), `make test_all`,
  `make go_lint`, `make test_cover`, `make docker_build` / `make docker_run`.
- In-process **mock backend** (`bench/mock-config.yaml`) to run and load-test the
  gateway with no fullnode or suppliers.
- E2E suite written to run against **both SAGE and PATH**; integration tests gated
  behind build tags.
- **Tagged releases** build binaries for linux and darwin on both architectures
  with checksums, and push a multi-arch image to `ghcr.io`.
- **CI gates on `govulncheck`** against a reviewed allowlist
  (`.github/vuln-allowlist.txt`). SAGE links cosmos-sdk and cometbft through the
  shannon-sdk and inherits findings with no upstream fix, so a bare scan would be
  permanently red and therefore ignored; the allowlist records why each survivor
  is accepted, and the checker also flags an entry that has stopped being
  reported. **Dependabot** covers gomod, docker and github-actions.

### Security

- **The relay port is documented as what it is.** SAGE authenticates no clients
  and rate-limits nothing — the edge authenticates, SAGE relays — and every relay
  it accepts is signed with the gateway key and spends staked POKT. An
  unauthenticated `3069` on the open internet is a stake drain, not a
  misconfiguration, and README, `SECURITY.md` and the generated route reference
  now say so rather than labelling that port "public" beside two marked loopback
  and scrape-only.
- **Error responses no longer carry internal detail.** Rendering `err.Error()`
  into the body included the whole cause chain, so a fullnode dial failure
  reached the caller with the operator's own host and port in it. Clients get the
  error kind and the message SAGE wrote; the chain stays in the log.
- **Prometheus label values are bounded by policy, not by call site.** One
  mechanism replaces three, and sanitising is no longer something a new metric
  can forget — an unbounded label is a memory leak with a network interface.
- Outbound gRPC pins TLS 1.2 as a floor on both the supplier and fullnode
  connections; the container base and Go toolchain track current patch releases
  (Go 1.26.6 closes six standard-library findings reachable from the request
  path).

### Validated

- **Beta TestNet** — SAGE served a PATH config **unmodified** and sustained a load
  run (~1000 RPS, ~29k relays) with zero client-attributed errors; the
  HTTP/JSON-RPC, REST, CometBFT, and WebSocket-subscription transports were
  exercised against a live service. gRPC relaying (native + gRPC-Web) is
  implemented and unit-tested; see `ARCHITECTURE.md → Transports → gRPC`.
- That run predates the hardening above. `blocked_domains`, the panic
  containment, the cache eviction work and the current toolchain are covered by
  the unit suite and not yet by a beta run.

### Notes

- SAGE is a restructured successor to **PATH**. 17 named bugs and operational
  issues from PATH production are structurally prevented — see
  `ARCHITECTURE.md → Production Lessons Baked Into Architecture`. Some behavior
  reproduces a PATH bug *on purpose* (the retry/circuit-break split); those spots
  are flagged in `CLAUDE.md`.

[Unreleased]: https://github.com/pokt-network/sage/commits/main
