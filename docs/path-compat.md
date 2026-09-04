# PATH compatibility — what it means, and where it stops

SAGE is a restructured fork of PATH. It is meant to be droppable into PATH's
place, and it is also meant not to inherit the bugs that came out of PATH's
structure. Those two goals only conflict if "compatible" is treated as one
thing. It is three, and they have different rules.

## The three layers

**1. What a client sees — must match.**
Request and response shapes, headers, status codes, the `Target-Service-Id`
routing, batch semantics, WebSocket framing. A client must not be able to tell
which gateway it is talking to. Divergence here is a bug, not a decision.

**2. What an operator configures and observes — must match, or be loud.**
Config keys, admin routes, metric names. SAGE loads a PATH config unmodified,
which means tolerating keys for features it does not have. Tolerating is not the
same as pretending, so every key SAGE does not act on is reported at startup:

- `Config.Ignored` — the key has no Go field at all. Caught by a second decode
  with `KnownFields` (`config/load.go`).
- `Config.Inert` — the key parses into a field that nothing reads. Caught by the
  registry in `config/inert.go`. Two tests hold it in place:
  `TestInertRegistryCoversDocComments` holds the registry to the doc comments
  (on parent *and* key, since 2026-09-04 — a leaf-only match let
  `retry_config.enabled` pass on the strength of `reputation_config.enabled`),
  and `TestUnwiredConfigKeysAreRegistered` in `internal/docgen` fails on any
  field the generator finds unread that the registry does not name. A key can
  no longer be parsed, unread and unreported.
- `Config.Warnings` — the key parses, *is* read, and probably does not do what
  whoever wrote it expected. One sentence saying what the gateway will actually
  do with the value, because refusing the file would break the compatibility
  promise and accepting it silently would mislead.

Most of the reputation-tuning surface is honoured rather than reported:
`signal_impacts` for the five signal types SAGE still has, the
`tiered_selection` thresholds, `probation.{threshold,traffic_percent}` and
`min_threshold` are read by the selector and the scorer — globally, because one
selector serves every service, so a per-service copy of them changes nothing.

Thresholds that do not descend — the beta config we run has `tier2_threshold:
30` above `probation.threshold: 50` — still load. PATH accepts that file and
the selector copes, classifying probation before tier 2 so a band simply ends
up narrower than it reads. Refusing to boot on it would break the whole point
of layer 2 over something that works. It is not silent either: it produces a
third kind of startup complaint, `Config.Warnings`, one sentence saying what
the selector will actually do with the numbers given. Only values that describe
no behaviour at all (a traffic share above 100%, a chronic onset rate at or
above the full rate) are refused.

What stays `Inert` is what SAGE has no mechanism for: `latency_profiles` and the
two latency signal impacts (latency reports, it does not penalise),
`recovery_success` (the type is gone), `recovery_timeout` and
`probation.recovery_multiplier` (there is no cooldown and no multiplier),
`tiered_selection.enabled` and `probation.enabled` (both always run). An
operator who tuned those on PATH and moved the file across was, until this
existed, editing structs no code reads — and nothing said so.

**3. How it works inside — free to diverge.**
Middleware decomposition, reputation mechanics, QoS plugin interfaces, session
handling. This is where PATH's incidents live, and copying a mechanism is how
you inherit the failure mode that comes with it. Divergence here is the point of
the fork.

## The dangerous middle

The failure this document exists to prevent is not a missing key or a different
internal design. It is **the same key with different semantics** — present,
parsed, apparently honoured, quietly meaning something else. That is worse than
either extreme, because it is the only one an operator cannot detect.

The rule: if SAGE implements a PATH key with different behaviour, it goes in the
register below **and** the difference has to be observable — a startup log, a
refusal, or a doc the operator will actually meet. Silence is only acceptable
when the behaviour is identical.

## Register of deliberate divergences

| Area | PATH | SAGE | Why |
|---|---|---|---|
| Strike system / cooldowns | Endpoints are benched by a strike system and by two rate detectors, all sharing one `CooldownUntil` | None. Reputation is a continuous score feeding tiered selection | PATH accumulated five mechanisms that could each remove an endpoint, with separate counters and one shared timestamp; a first offence inherited a stale escalation. `strike_system` has no field here on purpose and lands in `Ignored` |
| `retry_config.enabled` | `false` turns retries off; absent means on | Same since 2026-09-04, read from the YAML's own presence of the key, since a value-typed bool cannot tell absent from false; the startup report says when it was honoured. It was parsed and never read, so `enabled: false` under `max_retries: 3` retried three times silently | A switch that is sometimes honoured is worse than one that never is |
| `retry_config` merge across `gateway_config.retry_config`, `defaults`, `unified_services.defaults` | One block | Field by field since 2026-09-04, `defaults` first. A block was picked whole on the strength of its `max_retries`, so a production config carrying `retry_config: {hedge_delay: 100ms}` lost the hedge delay | Every field an operator wrote should reach the code that reads it |
| `retry_config.retry_on_5xx` | Per-cause switch | Inert and reported, like `retry_on_timeout`: retryability is the heuristic's verdict on the response | Was documented as honoured; it never was |
| `retry_config.max_latency` | Slow-response threshold feeding reputation | The total time budget across retry attempts; latency never penalises (`docs/scoring.md` §7.2). The doc comment said the former until 2026-09-04 | Same key, different semantics, now said |
| `local[].checks[].expected_status_code`, `checks[].timeout` | Honoured per check | Honoured since 2026-09-04; were parsed and unread (any 2xx, the global probe timeout) | A check that names a status or a deadline means it |
| `gateway_mode: delegated` | Signs for the app `App-Address` names | Loads with a startup warning saying SAGE signs with its configured keys and ignores the header | Client-visible, so not silent |
| Score deltas | `signal_impacts` config | Honoured for the five surviving signal types; `recovery_success`, `slow_response`, `very_slow_response` stay `Inert` | Types deleted — `docs/scoring.md` §7.2 |
| Latency scoring | `latency_profiles` (thresholds, bonuses, penalties) | Latency has reporting power only: a per-key EWMA in the admin listing; the block stays `Inert` | Decided, not open — `docs/scoring.md` §7.2 |
| Chronic violators | Critical-rate detector with its own EWMA, threshold, escalation and cooldown | A rate term inside the score: EWMA failure weight → penalty → same tiers | One mechanism, one state, one power — `docs/scoring.md` §7.3 |
| Archival routing | Positive proof required: an endpoint must be marked archival to serve archival requests | Tri-state; only a **fresh negative** excludes | A node fabricating state from its head produces the same success as a real archival node, so a positive mark cannot be trusted to gate traffic. A self-reported negative can |
| Solana `sync_allowance` | Unset means a strict `height >= perceived` comparison | Unset defaults to 1500 blocks | PATH's 2026-08-18 production incident: perceived is a max, so at 400ms/block only the last reporter survived |
| Supplier blacklisting on validation errors | Blacklists on `ErrRelayResponseValidationGetPubKey` | Deliberately excluded (`protocol/shannon/response_validation.go`) | That error is SAGE's own full node failing to answer, not the supplier's fault. During a local full-node outage PATH's rule empties the pool in one pass |
| WebSocket session rollover | Session rebind + subscription replay keeps the connection alive across boundaries | Same outcome since 2026-08-31, different shape: a generic bridge with an endpoint-lost hook, a registry that translates subscription ids both ways, rebind on loss / stall / session end, `POST /admin/websocket/rebind/{service}` | `docs/design/specs/2026-08-31-ws-rebind-design.md` |
| Health probes across replicas | Every replica probes every supplier | The elected leader probes; results go through a Redis stream and every replica applies them | A probe is a paid relay; N replicas bought N copies of one fact — `docs/design/specs/2026-08-31-probe-once-design.md` |
| `/v1/{portal_app_id}/…` path prefix | Stripped when PEAS sets `Portal-Application-ID` | Not stripped: `/v1/{path…}` is forwarded as the REST path | Grove-portal specific; PATH's own TODO moves it to the edge. Strip at the edge, or ask for it here |
| `App-Address` header (delegated mode) | Signs for the app the header names | Ignored; signs with the configured `owned_apps_private_keys_hex` | SAGE derives how to sign from the keys it holds, not from a declared mode. A per-request delegated app is not supported |
| `blocked_suppliers` (per service) | A list of supplier operator addresses never routed to | Honoured since 2026-08-31: matched on the supplier address, applied wherever endpoints are handed out | An operator's standing "not this supplier" decision, distinct from the earned blacklist |
| `endpoint_policy` (`require_https`, `require_domain`) | Rejects plaintext or raw-IP supplier URLs | Honoured since 2026-08-31, gateway-wide | A plaintext supplier relays keys and payloads in the clear |
| `Target-Suppliers` header | Pins the relay to the listed suppliers, bypassing reputation | Ignored | A client-controlled bypass of selection has no place in a gateway that scores suppliers |
| `/health`, `/healthz`, `/livez` | `/health` is unconditional liveness; `/healthz` is readiness (503 until a session exists) | Same meanings since 2026-09-04; `/livez` is a second liveness spelling. `/health` was readiness here until then, which turned a full-node outage into a restart loop under a PATH-written livenessProbe | Kubernetes probes written for PATH work unchanged — and mean the same thing |
| `/ready/{service}` | 503 when the service has no session or no endpoints; body lists `endpoint_count`, `has_session` | Same condition since 2026-09-04; body is `{ready, service, endpoint_count, error?}`. Was 200 for anything configured | A per-service readiness probe that cannot fail is not a probe |
| Gateway-generated errors (no usable supplier response, timeout) | HTTP 500, JSON-RPC code `-31001` with `data.retryable` | HTTP 500 (504 on timeout, 429 on rate limit, 400 on a client mistake), JSON-RPC code `-32603`, no `data`. Was HTTP 200 until 2026-09-04 | The status is what a load balancer, a dashboard and a client's retry policy branch on; a 200 hid every failure from all three. `-32603` is the standard code for it; PATH's `-31001` is its own invention and a client keyed on it is a client keyed on PATH |
| Pre-relay rejections (unconfigured service, unsupported RPC type, malformed JSON-RPC, bad `RPC-Type` header) | 400, JSON-RPC envelope, `-32600`, `data` naming what is allowed | Same since 2026-09-04: 400, envelope with the request's id, `-32600`, `data.available_services` / `data.allowed_rpc_types`. A non-JSON-RPC request gets `{"error", "data"}` | A typo in `Target-Service-Id` used to reach the protocol layer and come back as 200 `relay failed`, which reads as an outage |
| Oversized request | Body over `max_request_body_bytes` (75 MiB) is refused by net/http; a batch over the payload cap is a 500 `-31001` | 413 for both, JSON body. `max_request_body_bytes` honoured since 2026-09-04 with PATH's default; it was a hard-coded 1 MiB, so batches PATH accepted were 400s only SAGE produced | 413 is the status that means this; nothing on the client's side is retryable about it |
| `Content-Type` on relay responses | `application/json` on every QoS-served body | Same since 2026-09-04: JSON-RPC and CometBFT always, REST when the body is JSON; gRPC keeps its own | net/http sniffed every response as `text/plain` before |
| CORS | Mirrors `Origin`, allows `GET, POST, PUT` and `Content-Type` (+ `solana-client` for solana), answers `OPTIONS` 200 | Same shape since 2026-09-04, allowing every verb SAGE routes and the headers a client actually sends (`Target-Service-Id`, `RPC-Type`, `Authorization`, `solana-client`); `OPTIONS` answers 204; `Vary: Origin` | A browser dapp could not complete preflight against SAGE at all |
| Verbs on `/v1` | Any | Any since 2026-09-04; was POST and GET, so PUT/DELETE/PATCH to a REST service were 405 | A REST API is addressed by its own verbs |
| `RPC-Type` request header | Overrides detection; an unknown or undeclared value is a 400 | Same since 2026-09-04 | A client that says what it is sending should be believed, and told when it is wrong |
| Server timeouts and header cap when unset | read 60 s, write 120 s, idle 180 s, headers 2 MB | Same defaults since 2026-09-04 (`router_config.{read,write,idle}_timeout`, `max_request_header_bytes`); were 30/30/120 s and 1 MiB | A 30 s write timeout cut off slow archival calls PATH served |
| Relay metadata response headers | `X-Archival-Request`, `X-Retry-Count`, `X-Suppliers-Tried` always; `X-Hedge-Result`, `X-Environment`, `X-App-Address`, `X-Supplier-Address`, `X-Session-ID` sometimes | None of them. `X-Request-ID` is echoed and `X-Degraded` is set when selection degraded, neither of which PATH has | Supplier addresses and session ids are the operator's business, not the caller's; the two SAGE sets are for the caller's own correlation and for an edge that wants to shed degraded answers |
| `GET /config`, `GET /disqualified_endpoints` on the relay port | Present | Absent; the admin port carries the equivalents behind the token | A relay port serves relays. Operator introspection with no auth on the public listener is what the admin port was split off to end |
| Block-height consensus | Redis-synchronised, max-merged | In-memory, median-anchored, with a plausibility ceiling | PATH's max-merge let one wrong reporter set perceived height for everyone |
| Response analysis | Byte-substring matching over the body | gjson top-level lookup plus `ErrorAttribution` | Substring matching produced false positives on nested fields; attribution makes "do not penalize the supplier for a blockchain error" structural rather than per-call-site |
| `rpc_type_fallbacks` | Per service, when no supplier in the session staked the requested RPC type, the pool switches to the mapped type's URLs (`comet_bft: json_rpc`) | Honoured since 2026-08-31: same key, same pool-level one-hop semantics for selection, validated at load; additionally applied per endpoint at send time, which is what lets the cosmos health check probe json_rpc-staked suppliers | Mainnet cosmos suppliers are still catching up on their `comet_bft` stakes; without the mapping they are invisible to `comet_bft` traffic and the cosmos health check probes them with a type they cannot take, grading healthy suppliers `minor_error` every cycle |
| `active_health_checks.enabled` | `false` turns probing off; absent means on | Same since 2026-09-04, read from the key's presence like `retry_config.enabled`. Until then the value gated only the readiness warm-up and probes ran regardless | Every probe is a paid relay; a switch that says "off" must stop them |
| `health_checks` feature flag | (no equivalent) | Read per cycle and per service since 2026-09-04. It had a row in the defaults table and an admin route since the start, and no reader | A control must control |
| `local[].checks` on a service with no plugin checks | Run | Run since 2026-09-04; they were built, listed at startup and never sent, because the executor skipped a service whose plugin declared no checks before reading the configured ones | A YAML rule for the passthrough is the only probe such a service has |
| `external_block_sources` | Trusted heights from outside the pool | A floor under the perceived head since 2026-09-04 (`qos.ExternalFloorSetter`), which is what the consensus code always had. Wiring fed the height in as an ordinary observation from a fake endpoint named `external`, one vote among many | A trusted source outvoted by the pool it corrects is not trusted |
| Readiness warm-up denominator | n/a | 75% of the services some check can grade, since 2026-09-04; was 75% of every configured service, which a config with more than a quarter of its services on the passthrough could never reach | A pod cannot wait for a probe that does not exist |
| An Essential probe answering 2xx with none of the fact it asked for | n/a | Graded a minor error since 2026-09-04. A REST probe carries no body to recognise itself by, so an HTML 200 on eth-beacon's header path passed as healthy | The probe exists to learn one thing; not learning it is the failure |
| `POST /admin/circuit-breaker/clear` across replicas | n/a | Reaches every replica within one `cache_ttl` since 2026-09-04: a refresh drops local breaks Redis no longer lists. Merging only ever added, so a clear on one pod left the others broken until their local expiry | An operator undoing a false-positive lockout must undo it everywhere |
| `POST /admin/reputation/reset` on a follower | n/a | Written through to storage since 2026-09-04; a follower's ordinary writes are still dropped (leader-only storage). The reset used to be dropped with them, and the leader's next signal restored the old score | A reset is a decision about the fleet's view, not one replica's |
| `DELETE /admin/flags/{flag}/{service}` | n/a | Added 2026-09-04. A per-service override could be set and never unset: no TTL, and a reload deletes globals only | A control must be reversible |
| `GET /admin/chain-state/{service}` | n/a | Added 2026-09-04: the perceived head and the latest height per endpoint. Consensus also warns at ingest when an endpoint reports less than half the perceived head | One sui endpoint reported a near-zero height for two cycles and there was no way to ask which |
| `sage_singleflight_coalesced_total`, `sage_degraded_total` | n/a | Fed since 2026-09-04. The first was never incremented (the middleware took no recorder); the second missed every path that sets `X-Degraded` | A metric that cannot move is a lie on a dashboard |
| QoS extension interfaces | n/a | `BlockHeightParser`, `ResponseFormatValidator`, `LifecycleHooks` and `ArchivalDetector.IsArchivalEndpoint` removed 2026-09-04: implemented by plugins, asserted by nothing, advertised in `docs/qos-plugins.md`. `ExternalFloorSetter` and `EndpointHeightLister` added, both asserted | An interface no consumer asserts is a promise the docs make and the code does not keep |
| `active_health_checks.external` | Fetches a fleet-wide health-check rule file from a URL and refreshes it | Not fetched; the key is reported as ignored at startup | The plugin's own checks cover the file's block-number / chain-id / status rows, with a real chain-id comparison and sync from block consensus; the file's `check_interval: 10s` rows are most of PATH's probe volume. Archival and websocket rows are the gap — revisit if the canary shows archival probing is missed |
| Probe cadence | Per-service `check_interval` from the rule file (mostly 10 s), every supplier, every check | `active_health_checks.interval` (30 s unless set) with `local[].check_interval` per service; one probe per backend URL; the EVM chain-id check every 5 min | Each probe is a paid relay against the app's stake. One idle SAGE pod on the mainnet config spent ~58 relays/s before the cadence knobs; PATH's leader ~171/s |

## Porting rule

Read PATH's commits for the **incident**, not the patch. Their fix is shaped by
their structure; the thing worth importing is what production did to them.

The catch-up record says this is cheaper, not dearer: of the 21 commits on
their security audit branch, 12 were N/A here; of the 10 in the 2026-08-20 pass,
6 were N/A, three of them specifically because SAGE has none of the overlapping
detectors the fix was repairing. Divergence is what makes most of their bug
reports not apply — but only if every pass still reads all of them, and records
the N/A reasoning where the next pass will find it.

Procedure and history live in the fork-sync notes; the short version is: check
PATH's **branches**, not just `origin/main`, and write down why something did not
apply so nobody re-derives it in six weeks.
