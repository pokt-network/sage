# Method-aware blocks — design

Status: approved in discussion 2026-08-26, not implemented. Implementation plan
follows in `docs/superpowers/plans/`.

## 1. Problem

A host that cannot answer one method is, today, either treated as able to
answer everything or as able to answer nothing.

PATH measured the second failure on 2026-08-22 (`DESIGN_METHOD_SCOPED_EXCLUSION.md`
in the PATH repo): three operators hang ~15s on ~37% of a service's method mix
and answer the rest normally. Their circuit breaker keys on hostname, so a
hedge timing out on a heavy method removed each host for every method; the only
traffic a removed host still received was hedges on exactly the methods it
fails, so it re-broke within minutes, all day. One operator carried ~97% of the
service while three healthy ones sat at tier 1 receiving nothing.

SAGE has the first failure. Its breaker is fed only by heuristic body verdicts
(`relay/middleware/circuitbreak.go` reads `ctx.HeuristicResult`; the Heuristic
middleware returns early on a transport error), so a hanging host never breaks.
A relay timeout is scored by `relay/middleware/observe.go` as MinorError (-3)
against +5 per success: an operator hanging on 37% of traffic nets +204 per 100
relays and stays pinned at 100, tier 1, indistinguishable from the operator
that answers in 200ms. SAGE keeps sending heavy calls into 30-second hangs and
lets the hedge answer. Reputation is keyed `<identity>|<rpc_type>`
(`reputation/key.go`) — as method-blind as PATH's.

Both gateways lack the same thing: memory that host H could not answer method
M recently, consulted at selection.

## 2. Goals and non-goals

Goals:

- A host that times out on method M stops receiving M for a bounded time and
  keeps receiving every other method.
- Hedge and retry arms honour and feed that memory — a losing hedge arm's
  timeout is the incident, and it never reaches `Observe` today.
- A host timing out on many methods is recognised as dead rather than learned
  one method at a time.
- Transport errors are graded: a dead host reaches the circuit breaker, a
  client hang-up penalises nobody.
- Every method-keyed value set (store keys, metric labels) is bounded by a
  vocabulary SAGE owns, never by the client's string.

Non-goals, deliberately:

- Method-keyed reputation scores (touches storage and health-check writes;
  measure this design first).
- Latency-relative marks ("slow, not timing out") — `docs/scoring.md` §4.4.
- Sharing marks through Redis. One mark is one relay_timeout of evidence.
- Detection tooling for cache-fronted operators (request sampler, drain) —
  admin backlog.

## 3. Decisions taken

| Question | Decision | Why |
|---|---|---|
| Scoring scope | Classify **and** grade transport errors | Otto's call 2026-08-26. Closes three `docs/scoring.md` §7 items: client cancel, dead host, timeout |
| Mark key | Hostname (`EndpointAddr.Domain()`) | Same key as the breaker; survives session rollover (supplier addrs rotate, hosts do not); the hang is the operator's infrastructure |
| Mark trigger | Timeout after connect, plus JSON-RPC errors meaning "this host cannot do this **method**" | A timeout is the one failure with no structured signal; -32601 and "api is not supported" are the structured form of the same fact |
| Not a trigger | Missing historical state, Solana per-key index exclusion | Per block / per program, not per method. Marking would exclude a host from every `eth_getBalance` for one honest pruned-state answer. Archival tri-state already owns the first |
| Escalation | ≥3 distinct methods marked on a host within one TTL ⇒ host-level block for one TTL | A dying host would otherwise cost a full relay_timeout per method before it is learned |
| Mechanism | New middleware + generic store; plugins contribute vocabulary only | Rejected: inside plugins (Observe is sampled and sees only winners; per-plugin stores duplicate) and PATH's `host\|method` breaker key (rate-gated, fed by body verdicts; a mark is count-gated and thin per key) |
| Empty pool after filtering | Degrade: keep the unfiltered list, count a bypass | A block must never be able to empty a pool — the reputation pool-collapse lesson |

## 4. Architecture

Four units, each testable alone:

```
heuristic.AnalyzeTransportError      classifies the error SendRelay returned
qos.MethodNormalizer (per plugin)    bounded method vocabulary
methodblock.Store                    host × method × expiry, escalation, TTL
relay/middleware.MethodBlocks        filters before selection, marks after the attempt
```

Chain position:

```
Retry → Hedge → SupplierAffinity → CircuitBreak → MethodBlocks → SelectEndpoint → DebugLog → Heuristic → SendRelay
```

Inside Retry and Hedge so every arm and every attempt both honours and feeds
the store; after CircuitBreak so both prune `ctx.Endpoints` before selection;
before SelectEndpoint so the plugin filter and reputation work on survivors.

### 4.1 Data flow, one attempt

1. MethodBlocks asks the service's plugin for `NormalizeMethod(ctx.Payloads[0])`.
   Empty string ⇒ pass through untouched (plugin has no method awareness for
   this payload, or no plugin).
2. `ctx.Endpoints` is filtered to hosts where `Store.Blocked(service, host,
   method)` is false. If nothing survives: `ctx.Degraded = true`, the list is
   left as it was, `bypass` is counted.
3. The inner chain runs. SendRelay fails or Heuristic analyses the body; either
   way `ctx.HeuristicResult` is set (new: also on transport errors).
4. Post-relay, if `ctx.Endpoint != ""` and the result has `MethodBlocking`
   set, `Store.Mark(service, host, method)`. The store reports whether the mark
   escalated the host; the middleware counts `mark` or `escalate`.

Batch sub-relays carry one payload each (`batch.go` sets
`sub.Payloads = []domain.Payload{payload}`), so per-item evaluation needs no
special case.

## 5. Components

### 5.1 Transport error grading — `heuristic/transport.go`

`AnalyzeTransportError(err error, requestCtxErr error) AnalysisResult`, called
by the Heuristic middleware in the branch that returns early today. The
request context's error is passed in because the relayer cannot tell a client
hang-up from anything else.

| Verdict | Detected by | Result |
|---|---|---|
| Client cancelled | `requestCtxErr == context.Canceled` | `AttrClient`, no retry, no penalty, **no signal** |
| Connect-level | `*net.OpError` with `Op == "dial"`, DNS (`*net.DNSError`), TLS handshake (`tls.RecordHeaderError`, `x509` errors), `ECONNREFUSED`/`ECONNRESET` before any byte | `AttrSupplier`, critical, `ShouldCircuitBreak`, retry |
| Timeout after connect | `net.Error.Timeout()`, `context.DeadlineExceeded` from the HTTP client, or `requestCtxErr == context.DeadlineExceeded` (the Timeout middleware fired mid-attempt) | `AttrSupplier`, major, retry, `MethodBlocking = true` |
| Other | anything else the relayer wraps (`ErrProtocol`, session fetch, signing, relay-miner validation) | today's behaviour: `AttrUnknown`, minor, retry per `IsRetryable` |

Ordering: client-cancel first, then connect-level, then timeout. A client
cancel and a timeout can coincide on an unhedged attempt (the client gives up
while the host is hanging); the cancel wins because whatever the host was
doing, nobody is waiting for the answer. Hedge arms run on
`context.WithoutCancel`, so a losing arm always reaches a real verdict.

`AnalysisResult` gains one field, `MethodBlocking bool`, set by this
classifier for timeouts and by Tier 2/3 for `-32601` and the
"api is not supported" / "lite fullnode" indicators. It is **not** set for
`capabilityLimitationPatterns` (archival) or the account-index exclusion.

`relay/middleware/observe.go` `buildSignal` gains one rule before anything
else: `AttrClient` with a relay error ⇒ return a zero signal and record
nothing. (`successResult` also uses `AttrClient`, but with no error; the rule
keys on both.)

The relayer's error shapes are catalogued by a table test in
`protocol/shannon` before the classifier is written: it drives real
`net/http` failures against a listener that refuses, one that accepts and
never writes, one that closes after headers, and asserts the `ErrorKind` and
wrapped cause SAGE sees. The classifier's tests use those exact shapes.

### 5.2 Vocabulary — `qos.MethodNormalizer`

```go
// MethodNormalizer is implemented by plugins that can name a payload's method
// from a bounded set. The returned string is a key in method-aware state and
// a metric label, so it must come from the plugin's own catalogue — never
// from the request verbatim.
type MethodNormalizer interface {
    // NormalizeMethod returns the catalogued name, MethodOther for a method
    // the plugin does not list, or "" when the payload has no method notion
    // (a raw REST body under a noop plugin, a WebSocket frame).
    NormalizeMethod(payload domain.Payload) string
}

const MethodOther = "_other"
```

Per plugin:

- **EVM**: one `knownMethods` map (~60 names: `eth_*`, `net_*`, `web3_*`,
  `debug_*`/`trace_*` that appear in the archival and coalescable lists today,
  plus the standard read set). Existing `coalescableMethods` and the archival
  method set become subsets of it so one list is the source.
- **Solana**: the same shape (~40 names); `coalescableMethods` in
  `qos/solana/parser.go` folds into it.
- **Cosmos**: CometBFT JSON-RPC methods from `cometBFTMethods` verbatim; REST
  paths templated by replacing each path segment that parses as a number,
  hex, or bech32 with `:var` (`/cosmos/tx/v1beta1/txs/block/:var`), then
  matched against a listed set of templates; unlisted ⇒ `_other`.
- **noop**: not implemented ⇒ middleware passes through.

A golden test per plugin pins the catalogue: adding a method is a diff someone
reads, and the bound on every label is visible in review. The same interface
is what the per-service method allowlist will consume.

### 5.3 Store — `methodblock/store.go`

```go
type Store struct { ... }                       // one per process, all services
func New(opts ...Option) *Store                 // WithTTL, WithEscalation, WithLogger
func (s *Store) Blocked(service, host, method string) bool
func (s *Store) Mark(service, host, method string) (escalated bool)
func (s *Store) Clear(service string) int
func (s *Store) Active(service string) []Block  // for the collector and admin GET
type Block struct { Host, Method string; Expiry time.Time } // Method "" = host-level
```

Rules:

- `Mark` sets `expiry = now + TTL`. A re-mark refreshes to one TTL from now;
  nothing ever extends past that. No escalating TTL for method marks — a
  method mark is cheap to be wrong about, and the breaker already escalates
  the expensive case.
- On the mark that brings a host to ≥ `escalation` distinct live method marks,
  the host is blocked for every method for one TTL, its method marks are
  dropped into it, and `escalated` is true.
- `Blocked` is `hostBlocked(host) || methodBlocked(host, method)`. Expiry is
  lazy on read; a sweep goroutine (`safego.Go`) drops expired entries every
  TTL so the map does not grow with dead hosts.
- One `sync.RWMutex`. `Blocked` runs per candidate endpoint per attempt, so
  the read path takes only the read lock and does no allocation.
- Local memory only. Redis is not consulted and the Redis-optional rule holds.

Defaults: TTL 5m, escalation 3.

### 5.4 Middleware — `relay/middleware/methodblocks.go`

Registered as `relay.MWMethodBlocks` ("method_blocks"), added to
`DefaultChainOrder` between `MWCircuitBreak` and `MWSelectEndpoint`, with
`mustPrecede` rules: `MWHedge` before `MWMethodBlocks` ("each arm must honour
and feed method blocks"), `MWMethodBlocks` before `MWSelectEndpoint` ("method
blocks prune before selection").

Gated by `featureflag.FlagMethodBlocks` ("method_blocks"), **default on** in
`DefaultFlags`. The per-service admin override is the kill switch — runtime,
no restart.

Constructor: `MethodBlocks(store *methodblock.Store, registry *qos.Registry,
flags featureflag.FlagStore, events MethodBlockRecorder)`; `events` is nil-safe
like `CircuitBreakRecorder`.

Behaviour is §4.1. Two details:

- The filter allocates a new slice only when something is actually removed;
  it never compacts `ctx.Endpoints` in place. Hedge's `Clone()` is shallow, so
  the primary arm and the parent share one backing array, and an in-place
  filter on one arm is a write racing the other's read. `circuitbreak.go`
  compacts in place today; the plan moves it to the same copy-on-filter
  helper so both prunes are safe under hedging.
- The post-relay mark reads `ctx.Endpoint` for the host, not the pre-filter
  list: the attempt's endpoint is what timed out.

### 5.5 Configuration

```yaml
gateway_config:
  method_blocks:
    ttl: 5m                 # zero = default (5m); negative = disable marking
    escalation_threshold: 3 # zero = default (3); negative = never escalate
```

`config.MethodBlocksConfig` on `GatewayConfig`, value types, doc comments on
both fields (docgen), fixture entries in `config/testdata/path_config*.yaml`
(the exhaustiveness test requires them), and an `Effective*()` accessor per
field following the zero-is-default convention. Not per-service: a per-service
knob can come later through the tuning-override layer if a service needs it.

### 5.6 Observability and admin

- `sage_method_blocks{service_id, domain, method}` — gauge, 1 while a block is
  active, absent otherwise. Scrape-time collector (`metrics.MethodBlockCollector`)
  over `Store.Active`, the `BreakerCollector` pattern: blocks expire lazily and
  a pushed gauge would never clear. `method` is `""` for a host-level block.
- `sage_method_block_events_total{service_id, method, event}` —
  `event ∈ {mark, escalate, bypass}`. No `domain` label on the counter: the
  gauge already names the host, and a counter keyed on host is the series
  growth PATH's cardinality incident was about.
- `service_id` bounded by the existing `labelPolicy`; `method` bounded by the
  plugin catalogue; `domain` through `sanitizeLabel`.
- `GET /admin/method-blocks/{serviceId}` → `[]Block`;
  `POST /admin/method-blocks/clear/{serviceId}` → `{"cleared": n}`. Same shape
  and bearer auth as the circuit-breaker routes. `make docs` regenerates
  `docs/metrics.md`, `docs/admin-api.md`, `docs/configuration.md`.

## 6. Error handling

- No plugin, or plugin without `MethodNormalizer`, or `NormalizeMethod` returns
  `""`: pass through, nothing recorded.
- Filter empties the list: degrade, bypass, unfiltered list. Never an error.
- Store operations cannot fail. `Mark` on an empty host or method is a no-op.
- The classifier never returns an error; an unrecognised shape is the "other"
  row, which is today's behaviour.
- Flag off at runtime: the middleware passes through on the next attempt;
  existing marks age out.

## 7. Testing

- `protocol/shannon`: the error-shape catalogue (real listeners: refuse,
  accept-and-hang, close-after-headers, client cancel mid-flight).
- `heuristic`: table test per verdict on those shapes; `-32601` and the
  unsupported-API indicators set `MethodBlocking`; archival and per-key
  patterns do not. Revert-check each row.
- `qos/*`: golden catalogue per plugin; normaliser returns `_other` for an
  unknown name, `""` for a payload with no method; cosmos REST templating on
  numeric, hex and bech32 segments.
- `methodblock`: TTL expiry, re-mark refresh does not extend, escalation on
  the third distinct method and not on the third re-mark of one method,
  host block covers every method, `Clear` drops marks and escalation state,
  sweep removes expired hosts. `-race` with concurrent `Blocked`/`Mark`.
- `relay/middleware`: through the real chain order with Retry and Hedge —
  a losing hedge arm's timeout marks the host and the next request's hedge
  does not land there; a client cancel records no signal; a connect-level
  failure reaches the breaker; the bypass path when every host is blocked.
  Every filter test is built so exactly **one** endpoint survives — filter-all
  and filter-none are indistinguishable through the selection cascade, which
  is how two archival revert-checks passed against a mutation.
- `relay`: chain-order rule tests for the two new `mustPrecede` entries.
- `metrics`, `router`: collector output, admin routes, `newIsolatedRecorder`
  updated (it builds the Recorder by hand).
- `internal/docgen` goldens after `make docs`.
- Every fix revert-checked; full `-short -race` suite, vet, gofmt,
  golangci-lint clean.

## 8. Rollout

1. Transport grading lands first, alone, and is validated on beta: watch
   `sage_circuit_breaker_outcome_total` for connect-level failures now
   counting, and reputation signals for client cancels disappearing.
2. Vocabulary + store + middleware land together behind the flag (on).
3. On beta: `sage_method_blocks` should be empty on a healthy pool; force a
   mark by pointing a service at a listener that accepts and never answers,
   confirm the method is routed elsewhere while other methods still reach it,
   confirm escalation after three methods, confirm expiry.
4. Read `sage_method_block_events_total{event="bypass"}` for a day before
   trusting the filter; a non-zero bypass rate means the TTL or threshold is
   emptying pools.

## 9. Open items

- Whether `_other` should ever escalate a host on its own: three unknown
  methods timing out is three marks today. Left as-is; revisit with data.
- Per-service TTL through the tuning-override layer, if one chain's heavy
  methods need a different memory.
- Recording the winner's per-method latency (the cache-versus-capacity tell)
  needs a bounded `method` label on the latency histogram — enabled by §5.2,
  not done here.
