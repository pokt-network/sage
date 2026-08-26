# Admin pass — operator drain, chain-state reset, config reload, request sampler

Status: design approved 2026-08-26 (Otto: "go with it"). Implementation plan in
`docs/superpowers/plans/2026-08-26-admin-pass.md`.

## 1. Why one pass

Every open admin question shared one decision — how the admin API authenticates —
and that was settled on 2026-08-21 (bearer token, `admin_config.auth_token` or
`SAGE_ADMIN_TOKEN`, loopback-only without one). With it settled, the four items
that were parked behind it land together on the same footing: an operator drain
(PATH #526), a chain-state reset (PATH #523), a config reload, and a traffic-shape
sampler (PATH #528). Adding them one at a time would have meant four small trust
decisions; adding them together means none.

Decisions taken in discussion:

| Question | Decision |
|---|---|
| Where a drain lives | Redis when `redis.address` is configured, local memory otherwise — the feature-flag store's pattern. The response says which applied. |
| What a chain-state reset clears | QoS state only: the service's block consensus and its per-endpoint QoS store. Reputation, circuit breaker and method blocks keep their own routes. |
| What a reload may change | Only what has a runtime seam: gateway/service defaults (retry, timeout, method-block knobs), feature-flag config overrides, active health-check rules, `blocked_domains`. Services, keys, listeners, Redis, `chain_id`, middleware chain need a restart and are reported as such. |
| Request sampler | Included, SAGE-shaped: 1-in-N fingerprinting per service, bounded table, two gauges keyed on `service_id` only, one read-only admin route. |
| Drain placement | At `shannon.AvailableEndpoints`, next to `blocked_domains` — the one chokepoint selection, retry/hedge/batch, WebSocket bind and health checks all inherit. |

## 2. Goals and non-goals

Goals:

- An operator can take one supplier operator out of one service for a bounded time,
  fleet-wide when Redis is present, and see it as a deliberate bench rather than a
  quality incident.
- An operator can throw away a service's learned chain state when it is wrong (a
  poisoned perceived height, a stale chain-id assertion) without a restart.
- An operator can edit the config file and have the runtime-mutable parts take
  effect, with an honest list of what did not.
- An operator can tell repetitive traffic from diverse traffic per service — the
  detection tool the method-blocks threat model (a cache-fronted operator) needs.

Non-goals:

- WebSocket "tumble" (PATH's forced rebind) — needs the rebind engine SAGE does not have.
- Per-endpoint drain. Endpoint addresses rotate every session; a drain keyed on them
  expires silently. PATH learned this the hard way; the drain keys on the operator.
- Persisting reload results or tuning overrides across restarts.
- A hot-reload of the service list. That is a rebuild, not a reload.

## 3. Drain

### 3.1 Semantics

A drain is a predicate `(service, operator, rpc_type?) → until`. `operator` is the
registrable domain (eTLD+1, the same value `domain.EndpointAddr.Operator()` returns),
lowercased. An empty `rpc_type` drains every RPC type. It is matched against each
endpoint's live URL at `AvailableEndpoints` time, so an endpoint rotated into the
session at a drained operator is benched the moment it appears, and the drain is
unaffected by session rollover.

- `duration` is required and capped by `admin_config.max_drain` (zero = 24h). A request
  above the cap is refused, not clamped — an operator who typed `72h` should learn
  the ceiling, not silently get a day.
- `duration: 0` releases an existing drain.
- `dry_run: true` reports what would happen — including how many live endpoints
  the predicate matches right now — without changing anything.
- A drain on the last remaining operator of a service is refused: selection would
  have nothing left, and the pool-collapse guard exists precisely so nothing else can
  empty a pool.

### 3.2 Package `drain`

```go
type Key struct { ServiceID domain.ServiceID; Operator string; RPCType domain.RPCType } // RPCType "" = all
type Entry struct { Key; Until time.Time; Reason string }

type Store interface {
    // Set installs or refreshes a drain. Until is absolute; the caller has already
    // applied the ceiling.
    Set(ctx context.Context, e Entry) error
    Release(ctx context.Context, k Key) error
    Active(ctx context.Context, serviceID domain.ServiceID) []Entry
    // Drained is the hot-path check: does any live drain cover this endpoint's
    // operator for this service and RPC type? Read-only, allocation-free.
    Drained(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) bool
}
```

`MemoryStore` holds a `sync.RWMutex` map keyed by `Key`; expiry is lazy on read.
`RedisStore` wraps the memory store as a cache with a short TTL (the flag store's
`cacheEntry` pattern): writes go to Redis (`sage:drain:<service>:<operator>:<rpc>` with
the drain's own expiry) AND to local memory; `Drained` reads local memory; a refresh
goroutine (`safego`) pulls the service's keys from Redis every cache TTL so a drain
set on another replica arrives within seconds. A Redis write failure is returned to
the caller as a `propagation_error` while the local drain still applies — PATH's
"this pod only" honesty.

### 3.3 Chokepoint

`shannon.Protocol` gains an optional `drains drain.Store` (nil-safe). In
`AvailableEndpoints`, next to the `blockedDomains.IsBlocked(url, rpcType)` check, an
endpoint is skipped when `drains.Drained(serviceID, operatorOf(url), rpcType)`.
`operatorOf` uses the same eTLD+1 derivation as `EndpointAddr.Operator()`, memoized
on the URL like the existing `matchKey`.

Health checks call `AvailableEndpoints` too, so a drained operator is not probed
while drained. That is accepted: the drain is bounded, and probing a benched
operator would let it earn reputation while receiving no traffic.

### 3.4 Routes and metrics

- `POST /admin/reputation/drain/{serviceID}` body
  `{"domain": "...", "duration": "30m", "rpc_type": "websocket"?, "reason": "..."?, "dry_run": false?}` →
  `{service_id, domain, rpc_type, applied, released, dry_run, matched_endpoints, drained_until?, propagation_error?, active_drains: [...]}`.
  PATH's path is kept for operator muscle memory.
- `GET /admin/reputation/drain/{serviceID}` → `[]Entry` (empty array, never null).
- `DELETE /admin/reputation/drain/{serviceID}/{domain}` → releases every RPC type for that operator.
- Gauge `sage_drained_operators{service_id, domain, rpc_type}` = 1 while a drain is live,
  scrape-time collector, absent otherwise. `rpc_type` is `all` for an unscoped drain.

## 4. Chain-state reset

Optional plugin extension:

```go
// StateResetter is implemented by plugins that hold learned chain state — block
// consensus, per-endpoint heights, chain-id assertions, archival marks — that an
// operator may need to discard without a restart.
type StateResetter interface { ResetState() }
```

EVM, Cosmos and Solana implement it: `BlockConsensus.Reset()` (drops observations and
the external floor) and `EndpointStore.Clear()`. Nothing else is touched; the next
health-check cycle and the next relays repopulate it, and `SelectEndpoints` already
treats an unknown endpoint as passing, so there is no outage window.

`POST /admin/chain-state/clear/{serviceID}` → `{"service_id", "reset": true}` or
`{"reset": false, "message": "plugin keeps no chain state"}` for a plugin without
the interface; 404 for an unknown service.

## 5. Reload

`POST /admin/reload` (and `SIGHUP`) re-reads the config from the path SAGE started
with (`-config` or `GATEWAY_CONFIG`), through the same `config.LoadFromFile` and
validation boot uses. A file that would not boot is refused and nothing changes; the
response carries the error.

On success the new config is diffed against the running one, section by section:

| Section | Seam | Applied by |
|---|---|---|
| gateway/service defaults: retry, timeout, method-block knobs | the `configFn(serviceID)` closures already read `cfg` per request | `cfg` becomes an `atomic.Pointer[config.Config]`; the closures load it |
| `feature_flags` | flag store | overrides re-applied with `Set`/`SetForService`; a flag removed from the file is `Delete`d so `DefaultFlags` applies again |
| `active_health_checks` rules | `Executor.SetConfiguredChecks` (exists) | rebuilt with `BuildConfiguredChecks`; warnings returned in the response |
| `blocked_domains` | new `Protocol.SetBlockedDomains` (atomic swap; `SAGE_BLOCKED_DOMAINS` still unions in) | replaced |
| `method_blocks` knobs | `methodblock.Store.SetTTL/SetEscalation` | replaced |
| everything else | none | listed under `needs_restart` with the key path when it changed |

Response:

```json
{"applied": ["gateway_config.defaults.retry_config", "feature_flags"],
 "needs_restart": ["gateway_config.services[eth].chain_id"],
 "ignored": [...], "inert": [...], "warnings": [...]}
```

Reload is serialised (one at a time); a reload does not touch tuning overrides — an
operator's runtime override still wins over the reloaded base, which is what the
override layer promises.

## 6. Request sampler

Package `traffic`:

- `Sampler` with `rate` (1-in-N, default 100), `window` (default 5m), `maxFingerprints`
  (default 1000 per service).
- `Observe(serviceID, payloads)` runs on every relay: a per-service counter decides
  whether this request is sampled; if so, each payload is fingerprinted as
  `fnv64(method + "\x00" + compactedParams)` where `compactedParams` is the `params`
  member with whitespace removed and the `id` excluded. Non-JSON-RPC payloads
  fingerprint `httpMethod + path + compactedBody`.
- Per service: a current and a previous fixed window; each window keeps
  `count[fingerprint]`, `count[method]`, `distinct[method]` and a `sample` of the
  first 200 bytes of one payload per fingerprint for the report. Past
  `maxFingerprints` distinct entries a new fingerprint increments an `overflow`
  counter instead of being stored, and the report says so.
- Hooked from the Observe middleware on the 100% path (before the async queue),
  behind flag `request_sampler` (default on). Cost: one counter increment per relay,
  a hash on 1% of relays.
- `GET /admin/request-sample` → per-service summary; `GET /admin/request-sample/{serviceID}?window=current|previous&top=20` → the window's summary plus top-N fingerprints.
  Summary fields: `sampled`, `distinct`, `distinct_ratio`, `top1_share`, `overflow`,
  `per_method: {method: {sampled, distinct, distinct_ratio}}`, `window_start`, `window_end`.
- Gauges (scrape-time collector, `service_id` only): `sage_request_sample_distinct_ratio`
  and `sage_request_sample_top_share`, read from the previous (complete) window.

## 7. Cross-cutting

- Every route sits behind the existing bearer middleware; none is exempt.
- `make docs` regenerates `docs/admin-api.md`, `docs/configuration.md` (`admin_config.max_drain`),
  `docs/metrics.md`; the goldens fail otherwise.
- The admin UI gains a Drain panel (list, set, release), a Reset button on the
  reputation panel, a Reload button with the diff response, and a Traffic panel.
- Tests: unit per package; the drain proven through `AvailableEndpoints` with a real
  session shape (two operators, one drained, WS and HTTP RPC types); the reload proven
  through a real file edit; the sampler proven with a repetitive and a diverse stream.
  Every fix revert-checked; `-race` throughout.
- Beta: drain `pocket.network` on `pnf-anvil` — refused (last operator); drain
  `purroofgroup.com` — `sage_drained_operators` shows it, health checks skip it,
  release restores; reset `pnf-anvil` — heights repopulate within a cycle; reload after
  editing `hedge_delay` — next relay uses it, `needs_restart` names a changed key.

## 8. Open items

- Drains do not survive a Redis flush; they are bounded so this is acceptable.
- `SetBlockedDomains` and drains are two lists checked in sequence; if a third
  domain-shaped exclusion appears, unify them.
