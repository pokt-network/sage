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
- **`sage_client_requests_total{service_id, status}`** — the client-facing HTTP
  status per relay request (one count per request), distinct from
  `sage_relay_total`'s per-attempt view. A JSON-RPC error is HTTP 200 here, so
  this is what an edge/client dashboard sees and reconciles against; it makes a
  gateway's real 4xx/5xx client-error rate visible directly.
- **`blocked_suppliers`** (per-service) and **`endpoint_policy`**
  (`require_https`, `require_domain`) are honoured — both were PATH config keys
  SAGE parsed and ignored. A blocked supplier operator address is never
  selected (matched on the address, so it survives session rollover); a
  plaintext http/ws endpoint or a raw-IP host is dropped when the matching
  policy is on. Applied at the one place endpoints are handed out, so they
  cover selection, retry, hedge, WebSocket bind and health checks, and are
  reported in the AvailableEndpoints debug counters.
- A non-2xx status from the relay miner (503 overload, its own 500, 413) is
  graded as a retryable endpoint error instead of being unmarshalled as a
  RelayResponse — which failed and blacklisted the supplier for 15 minutes over
  a transient miner error. The supplier now stays in the pool with a recoverable
  score penalty, and retry reaches another one. Blacklisting is reserved for a
  2xx response whose body is genuinely an invalid/unsigned RelayResponse.
- The client-facing message for a relay-response verification failure is now a
  generic "protocol error: upstream response failed verification" rather than
  leaking the internal reason ("unmarshal_error"); the reason stays in the log.
- Hedge returns a retryable error when its wait ends on a deadline, so the
  per-attempt timeout below actually recovers on a hedged relay. Hedge is on
  for every service, and it returned a raw context.DeadlineExceeded (not
  retryable), so a capped hedged attempt failed fast but the retry never fired
  — the recovery half was inert on the canary. A client cancel stays
  non-retryable.
- Each relay attempt is bounded to a share of the request deadline, so a
  supplier that accepts the connection but never responds ("awaiting headers")
  cannot consume the whole timeout and starve the retry that would reach a
  healthy one. On the mainnet canary this was the robinhood 502 baseline —
  a few relayminer hosts blackholed and every relay to them hung the full 5s.
  The last attempt runs unbounded; a request with no deadline is unaffected;
  the hedge race already honours the request context, so a hedged attempt's
  wait is bounded too while its detached arms still finish and self-score.
- Session refresh is coalesced, and sessions are served through their protocol
  grace period. SAGE expired a session the instant the chain height reached its
  end and forced a synchronous refresh — a grace window too early: the protocol
  keeps relays for a session valid for GracePeriodEndOffsetBlocks (an on-chain
  shared parameter) after its end. SAGE now reads that offset and keeps serving
  a session through end+grace while refreshing the next one in the background,
  so no relay blocks at the boundary and every relay is signed against a session
  the chain still honours. Below the coalescing that removed the boundary
  stampede: At a session boundary every in-flight relay
  for a service saw its cached session as expired and independently called the
  full node's GetSession; because num_blocks_per_session aligns every service
  to one boundary, this stampeded the node with hundreds of redundant calls at
  once, which overran it and hung relays to the 10s relay timeout — a ~1-2 min
  stall with a 35-50%% error spike every ~20 min, which PATH (grace-period
  rollover) does not have. A singleflight now runs one GetSession per
  (service, app) per boundary and shares the result; the fetch is detached from
  the caller's context so a relay hitting its deadline does not abort the
  shared refresh.
- Readiness (`/ready`) now gates on reputation warm-up, not just a session
  existing. A fresh or rolled pod is 503 on `/ready` until health-check
  results (the leader's first probe cycle, or a follower's `sage:probes`
  stream replay) have covered the configured services, so it is not put into
  the Service while selection is still blind — which served ~90s of failures
  per pod on every roll. Point the readinessProbe and a startupProbe there;
  `/healthz` stays the session-only check for PATH parity.
- Chain-native REST is routed as REST instead of defaulting to JSON-RPC. A
  request to a service that declares `rest`, on a path other than a JSON-RPC
  entry point (`/`, `/jsonrpc`), is classified REST — covering TRON's
  `/wallet/*`, Pocket's `/poktroll/*` and `/pokt/*`, and any Cosmos chain's
  REST namespace without a per-chain path table. A JSON-RPC envelope in the
  body still wins on any chain. Fixes tron 405s and pocket-native 501s seen on
  the canary. (A REST request still needs a REST-staked supplier to serve it.)
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

### September 2026, from the mainnet canary

- **A WebSocket session expiry reached the client as an undecoded protobuf
  blob, and cost the supplier reputation.** The relay miner puts a backend's
  raw WebSocket frame straight into `RelayResponse.Payload`, so SAGE forwarded
  that field verbatim — correct for every data frame, and wrong for the miner's
  own control responses, which come through the same field as a serialized
  `POKTHTTPResponse`. A connection landing on an already-ended session got an
  HTTP 410 envelope: the client saw protobuf where it expected JSON, and the
  per-frame heuristic graded the unparseable bytes as a supplier fault for a
  session boundary the supplier does not control. SAGE became exposed to this
  when WebSocket rebind let connections outlive a session boundary at all.
  `extractEndpointFrameBody` now decodes only what is provably an envelope —
  a real HTTP status, since `proto.Unmarshal` accepts almost anything — and a
  non-2xx forwards the decoded body while grading nothing in either direction.
  Found upstream in PATH (`1ff57772`) with a live session-cycle test.

- **Traffic-informed probing: don't probe a backend client traffic is already
  grading.** Every client attempt records a reputation signal, so a busy
  backend is graded continuously and the health check against it buys a second
  copy of a fact the score middleware has. Measured on the mainnet canary at 1%
  traffic before building it — two snapshots of the admin reputation listing
  ten minutes apart — 48.66% of probes in the window went to backends traffic
  had graded in that same window, concentrated on the busy EVM services and
  absent on the long tail. Behind the `traffic_informed_probing` flag, which
  defaults to **off**: the saving and the exposure both scale with traffic
  share, so this wants an experiment per deployment rather than the canary's
  numbers assumed. `sage_health_check_skipped_total` counts what it saves.

  The threshold is derived, not chosen. A probe is the only observation source
  that bypasses sampling (`observe.Queue.Submit` exempts `SourceHealthCheck`
  alone), so client traffic only stands in for a probe once the sampler
  forwards at least one observation from it — ten relays at the default 10%
  rate, ten times that at 1%. `health_checks.min_traffic_signals` overrides it.
  There is deliberately no setting meaning "skip on any traffic".

  Three things it will not do. It never skips before the pod is warm, because
  readiness counts coverage from applied probe results. It never treats a
  reputation key it is seeing for the first time as a window with no traffic,
  which would let a lifetime's cumulative count skip a probe on its first real
  reading. And one transport's traffic never excuses another's probe, because
  the RPC type is part of the reputation key.

- **The reputation timeline never evicted a key, and it OOMKilled a canary
  pod.** After 14.7 h one of two pods died (exit 137, 1 Gi limit), working set
  climbing ~100 MB/h from start; the heap put 76% of in-use memory in
  `reputation.(*Timeline).Record`, 82k keys against 6k on a fresh pod at ~13 KB
  per key ring. The timeline bounded events per key (a ring of 100) but never
  dropped a key, and the canary ran `key_granularity: per-supplier`, where a
  key is a staked registration that rotates every session — so the key set grew
  with the network for the life of the process. The score cache and the
  exporter both had bounds; the admin-only timeline had none, and the Redis
  write-behind grew the same way (`HLEN sage:reputation:` reached 119,567).
  Keys now drop after 1 h idle and cap at 16,384; `State.UpdatedAt` is stamped
  on every write-behind and the leader sweeps stale fields every 5 min with the
  same TTL, treating unstamped fields as stale so the first sweep drains what
  pre-stamp pods left. `sage_endpoint_reputation_score` exports only keys below
  the full score, capped at 500 per service — one pod had been emitting 104k
  series, 2.3% of the Prometheus head. Canary config moved to the default
  `per-url`, whose keys are backend URLs and do not rotate; note that the PATH
  config's comment on `per-endpoint` ("each URL tracked separately") actually
  describes `per-url`, while `per-endpoint` is the supplier x URL pair and grows
  the way `per-supplier` did. Verified on the canary over 17 h: timeline keys
  flat at ~2.3k, working set flat, and no pod has approached the limit since.

- **`request_type` (`client`|`probe`) on `sage_relay_total` and
  `sage_relay_latency_seconds`.** Health-check probes are paid relays, but they
  call `protocol.SendRelay` directly and never enter the middleware chain, so
  they appeared in no relay metric at all — the probe share of relay spend, and
  probe latency, were invisible, and the only probe-inclusive counter was
  `sage_reputation_attempts_total`. The executor now records each send it makes
  under `request_type="probe"`; client attempts keep the same series under
  `request_type="client"`. A panel that wants what it had before adds
  `{request_type="client"}`; an unfiltered one now includes probes.
- **HTTP 408 from a supplier was treated as the supplier's fault, and reverted
  the same day.** The change made a 408 retryable, attributed to the supplier
  and worth a minor penalty, on the reasoning that a supplier whose own server
  timed out should not end the relay. The canary disagreed: over equal
  30-minute windows on `sage_client_requests_total`, the client-facing 408 rate
  went from 0.674% to 2.597%, and roughly 1,600 requests per 100k that had been
  answered 200 came back 408. Attempts per client request did not move (1.555
  to 1.524), so it was not retry amplification, and SAGE emits no 408 of its
  own anywhere — every client 408 is an upstream response forwarded verbatim —
  so it was not the gateway manufacturing them. The mechanism left standing is
  the penalty: scoring every timing-out supplier down concentrates traffic onto
  a smaller tier-1 set, which sheds under the load it inherits, which scores it
  down in turn. 408 goes back to the generic 4xx branches. Re-landing this
  needs an experiment that separates the retry half from the penalty half.
  Confirmed on the canary the same day: client 408 back to 0.719% and 200 to
  99.140% per 100k, attempts per client request 1.519 — so the attribution
  change was the cause, and essentially all of the extra 408s were requests
  that would otherwise have been answered 200.

- **A pod inherits the fleet's reputation instead of re-learning it.** The
  write-behind had been persisting scores all along and nothing ever read them
  back, so every restarted or rolled pod started every endpoint at 100 and
  rebuilt the pool from probes — minutes behind the readiness gate, which on
  the mainnet canary meant a rolled pod covering 26 of 73 services after six
  minutes and being killed by its startup probe. `reputation.Hydrate` now
  loads the store once at startup and the health-check executor credits the
  services it loaded towards the warm gate, so a pod is ready in well under a
  second rather than cycles. A state older than the idle TTL, or with no
  `UpdatedAt` stamp, is skipped — the same rule the storage sweep applies, so
  nothing is adopted that is about to be deleted — live state is never
  overwritten, and the per-shard bound still holds. Redis unreachable, or a
  store with nothing fresh in it, starts cold exactly as before.

- **Sessions are warmed before a pod takes traffic.** Reputation hydration
  made a pod ready in seconds — before the first probe cycle, and so before any
  session existed. `getSession` falls through to a synchronous full-node fetch
  on the request path, so the first relay for each service paid it and the
  client waited out its whole deadline: on the canary that was one
  `relay timeout exceeded` per service across bsc, fuse, sei, linea, opbnb and
  zksync-era. `PrefetchSessions` now warms every configured service at startup
  before readiness is credited, and only services holding both a score and a
  session count towards the warm gate — readiness means "can serve", not "has
  opinions". It is paced rather than parallel-everything: at most 4 fetches in
  flight and no more than 20 a second, because the full node is shared and
  rate-limited and a rolling fleet must not arrive as a burst. Seventy-odd
  services finish in under four seconds; running out of time warms what it can
  and leaves the rest to the request path, as before.
- **`sage_reputation_hydrated_keys` / `sage_reputation_hydrated_services`** —
  what the startup warm-up read loaded, set once and constant thereafter. The
  log line saying the same thing is INFO, which production log levels suppress:
  the canary roll that carried hydration had to confirm it by inferring from
  `sage_reputation_keys`. Zero on a pod that should have inherited state is the
  value worth alerting on. The "found nothing, starting cold" case moved from
  INFO to WARN for the same reason. `internal/docgen` now also recognises the
  bare `prometheus.NewGauge`/`NewCounter`/`NewHistogram` forms, which it was
  silently omitting from the reference.
- **A pod that is not ready says why.** The warm gate answered a bare 503 and
  the path that usually causes it logs at DEBUG, which production levels
  suppress — so a stalled rollout had no logged explanation at all. The
  executor now logs one WARN per cycle until it is warm, carrying the coverage
  count, the threshold, and the services still awaited (bounded to ten, with a
  count of the rest). It stops the moment the pod warms.

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
