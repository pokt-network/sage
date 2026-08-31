# Admin Pass Implementation Plan

**Goal:** Land the four parked admin capabilities on one trust footing: operator drain (fleet-wide via Redis when present), chain-state reset, config reload with an honest diff, and a request-shape sampler.

**Architecture:** A `drain` package (memory + Redis stores, flag-store pattern) consulted at `shannon.AvailableEndpoints` beside `blocked_domains`; an optional `qos.StateResetter` plugin interface; a `reload` orchestration in `cmd/sagegw` that re-loads and diffs the config and pushes each section through its existing runtime seam (`atomic.Pointer[config.Config]` for the `configFn` closures, flag store, `SetConfiguredChecks`, new `SetBlockedDomains`, new method-block setters); a `traffic` sampler hooked from the Observe middleware. Every route behind the existing bearer auth; docgen regenerates the three references.

**Tech Stack:** Go (per `go.mod`), stdlib + testify, `go-redis/v9` (already a dependency), `tidwall/gjson`, Prometheus client.

**Spec:** `docs/design/specs/2026-08-26-admin-pass-design.md` — binding; read it first.

## Global Constraints

- Never a bare `go` statement — `internal/safego` (`Go`, `GoCtx`, `Run`, `Call`).
- Config fields are value types; zero = default, negative = off; every new field has a doc comment AND an entry in both `config/testdata/path_config*.yaml` (`TestConfigFixtureIsExhaustive`).
- A feature flag is defined once in `featureflag.DefaultFlags` and referenced by its `Flag*` constant.
- Every exported symbol and every new package has a doc comment (revive). Handlers registered with `mux.HandleFunc("METHOD /path", a.handler)` string literals and a doc comment on the handler (docgen).
- Metric label values come from a bounded set SAGE owns (configured service IDs, plugin catalogues, closed enums). Never a client string.
- `relay.Context.Clone()` is shallow; never write into a shared slice/map from an arm.
- Redis is optional: every feature must work with `redisClient == nil`.
- Run `make docs` after touching config structs, metric constructors or admin routes.
- Every behaviour change is revert-checked (disable it, the new test fails, restore) and the report shows the commands.
- Commit after every task, prose messages. Gate before "done": `go build ./... && go vet ./... && gofmt -l . && make test_unit && make go_lint && make docs && git diff --stat docs/`.
- Branch: `feat/admin-pass` off `main`.

---

## File map

| Path | Responsibility |
|---|---|
| `drain/doc.go`, `drain/store.go`, `drain/memory.go`, `drain/redis.go` (+tests) | Drain key/entry, `Store` interface, memory store, Redis-backed store |
| `protocol/shannon/relayer.go`, `protocol/shannon/drain.go` (new) | `SetDrains`, `operatorOf` memo, skip drained endpoints in `AvailableEndpoints`; `SetBlockedDomains` |
| `config/config.go` | `AdminConfig.MaxDrain` |
| `metrics/drain.go` (new) | `DrainCollector` gauge |
| `router/admin_drain.go` (new) | drain routes |
| `qos/plugin.go`, `qos/blockconsensus.go`, `qos/endpointstore.go`, `qos/{evm,cosmos,solana}/plugin.go` | `StateResetter`, `Reset`, `Clear`, implementations |
| `router/admin_state.go` (new) | chain-state clear route |
| `methodblock/store.go` | `SetTTL`, `SetEscalation` |
| `cmd/sagegw/reload.go` (new), `cmd/sagegw/wire.go`, `cmd/sagegw/main.go` | reload orchestration, config snapshot, SIGHUP |
| `router/admin_reload.go` (new) | reload route |
| `traffic/doc.go`, `traffic/sampler.go` (+tests) | request-shape sampler |
| `relay/middleware/observe.go`, `featureflag/defaults.go` | sampler hook, `FlagRequestSampler` |
| `metrics/traffic.go` (new), `router/admin_traffic.go` (new) | sampler gauges + routes |
| `router/admin_ui.html` | Drain, Reset, Reload, Traffic panels |
| `docs/*.md` (generated), `ARCHITECTURE.md` | references |

---

### Task 1: `drain` package — types, interface, memory store

**Files:**
- Create: `drain/doc.go`, `drain/store.go`, `drain/memory.go`, `drain/memory_test.go`

**Interfaces (Produces):**
```go
package drain
type Key struct { ServiceID domain.ServiceID; Operator string; RPCType domain.RPCType } // Operator lowercased eTLD+1; RPCType "" = all
type Entry struct { Key; Until time.Time; Reason string }
type Store interface {
    Set(ctx context.Context, e Entry) error
    Release(ctx context.Context, k Key) error
    Active(ctx context.Context, serviceID domain.ServiceID) []Entry   // live only, sorted by Operator then RPCType; nil when none
    Drained(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) bool // hot path, RLock only, no alloc
}
func NewMemoryStore() *MemoryStore
```
`Drained` is true when a live entry matches `(serviceID, operator, rpcType)` OR `(serviceID, operator, "")`. `Set` with `Until` in the past is a release. `Release` of an unknown key is a no-op returning nil.

- [ ] **Step 1: Failing tests** (`drain/memory_test.go`)

```go
package drain

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

func TestMemoryStore_SetDrainedRelease(t *testing.T) {
	s := NewMemoryStore()
	k := Key{ServiceID: "eth", Operator: "slow.example", RPCType: domain.RPCTypeJSONRPC}
	if err := s.Set(context.Background(), Entry{Key: k, Until: time.Now().Add(time.Minute), Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	if !s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("scoped drain must match its rpc type")
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeWebSocket) {
		t.Fatal("scoped drain must not match another rpc type")
	}
	if s.Drained("poly", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("drain is per service")
	}
	if err := s.Release(context.Background(), k); err != nil {
		t.Fatal(err)
	}
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("released drain still matches")
	}
}

func TestMemoryStore_UnscopedDrainCoversEveryRPCType(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "slow.example"}, Until: time.Now().Add(time.Minute)})
	for _, rt := range []domain.RPCType{domain.RPCTypeJSONRPC, domain.RPCTypeWebSocket, domain.RPCTypeREST} {
		if !s.Drained("eth", "slow.example", rt) {
			t.Fatalf("unscoped drain must cover %s", rt)
		}
	}
}

func TestMemoryStore_ExpiryIsLazy(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "slow.example"}, Until: time.Now().Add(20 * time.Millisecond)})
	time.Sleep(30 * time.Millisecond)
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("expired drain still matches")
	}
	if got := s.Active(context.Background(), "eth"); got != nil {
		t.Fatalf("expired drain still listed: %+v", got)
	}
}

func TestMemoryStore_ActiveSortedAndLiveOnly(t *testing.T) {
	s := NewMemoryStore()
	now := time.Now()
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "b.example"}, Until: now.Add(time.Minute)})
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "a.example", RPCType: domain.RPCTypeREST}, Until: now.Add(time.Minute)})
	_ = s.Set(context.Background(), Entry{Key: Key{ServiceID: "eth", Operator: "z.example"}, Until: now.Add(-time.Second)})
	got := s.Active(context.Background(), "eth")
	if len(got) != 2 || got[0].Operator != "a.example" || got[1].Operator != "b.example" {
		t.Fatalf("Active = %+v", got)
	}
}

func TestMemoryStore_PastUntilIsARelease(t *testing.T) {
	s := NewMemoryStore()
	k := Key{ServiceID: "eth", Operator: "slow.example"}
	_ = s.Set(context.Background(), Entry{Key: k, Until: time.Now().Add(time.Minute)})
	_ = s.Set(context.Background(), Entry{Key: k, Until: time.Now().Add(-time.Second)})
	if s.Drained("eth", "slow.example", domain.RPCTypeJSONRPC) {
		t.Fatal("Set with a past Until must release")
	}
}
```

- [ ] **Step 2: Run** `go test ./drain/ -count=1` → build failure (undefined).

- [ ] **Step 3: Implement** — `drain/doc.go` package comment (why a predicate on the operator, not endpoint addresses: addresses rotate every session so a drain keyed on them expires silently — PATH #526's lesson); `drain/store.go` with the types and interface above, doc comments starting with each name; `drain/memory.go`:

```go
// MemoryStore keeps drains in process memory. It is the whole store when Redis
// is absent and the read-through cache when Redis is present.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[Key]Entry
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{entries: make(map[Key]Entry)} }

func (s *MemoryStore) Set(_ context.Context, e Entry) error {
	e.Operator = strings.ToLower(e.Operator)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !e.Until.After(time.Now()) {
		delete(s.entries, e.Key)
		return nil
	}
	s.entries[e.Key] = e
	return nil
}

func (s *MemoryStore) Release(_ context.Context, k Key) error {
	k.Operator = strings.ToLower(k.Operator)
	s.mu.Lock()
	delete(s.entries, k)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Drained(serviceID domain.ServiceID, operator string, rpcType domain.RPCType) bool {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.entries[Key{ServiceID: serviceID, Operator: operator, RPCType: rpcType}]; ok && e.Until.After(now) {
		return true
	}
	e, ok := s.entries[Key{ServiceID: serviceID, Operator: operator}]
	return ok && e.Until.After(now)
}

func (s *MemoryStore) Active(_ context.Context, serviceID domain.ServiceID) []Entry {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Entry
	for _, e := range s.entries {
		if e.ServiceID == serviceID && e.Until.After(now) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Operator != out[j].Operator {
			return out[i].Operator < out[j].Operator
		}
		return out[i].RPCType < out[j].RPCType
	})
	return out
}
```
`Drained` callers pass an already-lowercased operator (the chokepoint lowercases once per URL); document that.

- [ ] **Step 4: Run** `go test ./drain/ -race -count=1` → PASS. Revert-check: make `Drained` skip the unscoped lookup → `UnscopedDrainCoversEveryRPCType` fails. Restore.
- [ ] **Step 5: Commit** `feat(drain): operator drain store, memory-backed`

---

### Task 2: `drain.RedisStore`

**Files:** Create `drain/redis.go`, `drain/redis_test.go`. Consumes: `featureflag.RedisClient`-shaped interface (define `drain.RedisClient` locally with `Get/Set/Del/Keys/Scan`-free minimal set: `Set(ctx, key, value, expiration) *redis.StatusCmd`, `Del(ctx, keys...) *redis.IntCmd`, `Keys(ctx, pattern) *redis.StringSliceCmd`, `MGet(ctx, keys...) *redis.SliceCmd`). Produces: `NewRedisStore(client RedisClient, opts ...RedisOption) *RedisStore`, `WithCacheTTL(d)`, `(*RedisStore).Start(ctx)` (refresh loop via `safego.GoCtx`), and `ErrPropagation` wrapping a Redis write failure while the local write succeeded.

- [ ] **Step 1: Failing tests** with an in-memory fake `RedisClient` (look at `featureflag/redis_test.go` for the fake it uses and copy its shape — a map + methods returning `redis.NewStatusResult` etc.): `Set` writes `sage:drain:<service>:<operator>:<rpc-or-all>` with JSON `{until, reason}` and expiration `until-now`; `Drained` reads local; `Release` deletes both; a `Set` whose Redis write fails returns `ErrPropagation` AND still drains locally; `Start` + a fake that returns a key set by "another replica" → after one refresh `Drained` is true.
- [ ] **Step 2: Run** → undefined.
- [ ] **Step 3: Implement** — embed a `*MemoryStore`; `Set`: local first, then Redis `Set` with expiry; on error return `fmt.Errorf("%w: %v", ErrPropagation, err)`. `Active`: local. `refresh(ctx)`: `Keys("sage:drain:*")` then `MGet`, parse, rebuild the local map atomically (replace, not merge, so releases elsewhere propagate). `Start` ticks every cache TTL (default 5s) with `safego.Run` per tick.
- [ ] **Step 4: Run** `-race` → PASS. Revert-check: skip the local write in `Set` → the propagation test fails.
- [ ] **Step 5: Commit** `feat(drain): Redis-backed store with local cache and propagation error`

---

### Task 3: Chokepoint — drains in `AvailableEndpoints`, and `SetBlockedDomains`

**Files:** Modify `protocol/shannon/relayer.go` (struct field `drains drain.Store`, `AvailableEndpoints` skip); create `protocol/shannon/drain.go` (`SetDrains`, `operatorOf` memo — reuse the public-suffix helper `domain.EndpointAddr.Operator()` uses; check `domain/endpoint.go` for the exported function and call it on the URL host), `protocol/shannon/drain_test.go`; add `SetBlockedDomains(entries []config.BlockedDomain) error` (validate, build, atomic swap — make `blockedDomains` an `atomic.Pointer[domainBlocklist]`, nil-safe reads) with a test.

- [ ] **Step 1: Failing tests** — find the existing harness that tests `AvailableEndpoints` against a fake session with `blocked_domains` (`protocol/shannon/domain_blocklist_test.go` or `relayer_test.go`); build a session with endpoints at `a.pocket.example` (two hosts) and `b.other.example`; drain `pocket.example` for `json_rpc` → `AvailableEndpoints(json_rpc)` returns only `b.other.example`'s endpoints; `AvailableEndpoints(websocket)` still returns all; release → all back. `SetBlockedDomains` test: swap in a list blocking `other.example` → next call excludes it; an invalid entry (empty domain) returns an error and leaves the old list.
- [ ] **Step 2: Run** → undefined.
- [ ] **Step 3: Implement.** In the loop next to `IsBlocked`: `if d := p.drains; d != nil && d.Drained(serviceID, operatorOf(url), rpcType) { continue }`. `operatorOf` memoizes host→eTLD+1 in a `sync.Map` keyed by URL (the same pattern `matchKey` uses). `SetDrains(store drain.Store)` is called at wire time.
- [ ] **Step 4: Run** `-race` → PASS. Revert-checks: remove the `continue` → drain test fails; make `SetBlockedDomains` not swap → its test fails.
- [ ] **Step 5: Commit** `feat(shannon): honour operator drains at AvailableEndpoints; swappable blocklist`

---

### Task 4: Config, metric, routes for the drain

**Files:** `config/config.go` (`AdminConfig.MaxDrain time.Duration \`yaml:"max_drain"\`` doc-commented, `EffectiveMaxDrain()` zero→24h; fixtures), `metrics/drain.go` (+test; `DrainLister{ ActiveDrains(serviceID string) []DrainEntry }`, `DrainEntry{Domain, RPCType string}`, `NewDrainCollector(lister, services)` → gauge `sage_drained_operators{service_id,domain,rpc_type}` with `rpc_type="all"` when empty), `router/admin_drain.go` (+test), `router/admin.go` (`AdminAPI` gains `drains drain.Store`, `sessions` (the endpoint provider — `protocol.EndpointProvider`) for `matched_endpoints`, and `maxDrain time.Duration`; `NewAdminAPI` signature grows accordingly — update every call site), docs.

Routes and body exactly per spec §3.4. Rules in the handler: `domain` required (400), lowercase it; `duration` parsed with `time.ParseDuration`, `> maxDrain` → 400 naming the ceiling; `rpc_type` validated against `domain.AllRPCTypes()` (400 otherwise); `duration == 0` → `Release` and `released: true`; "last operator" refusal: compute the set of operators across `sessions.AvailableEndpoints(ctx, service, rpcType-or-each)` and refuse (409) when every live endpoint belongs to the target operator; `dry_run` skips `Set`. `matched_endpoints` = count of live endpoints whose operator matches. A `drain.ErrPropagation` from `Set` → 200 with `propagation_error` filled.

- [ ] **Step 1: Failing tests** — memory store + a fake `EndpointProvider` with two operators: set 30m → 200, `applied`, `drained_until` ≈ now+30m, `matched_endpoints` correct; GET lists it; over-ceiling → 400; last-operator → 409; DELETE releases; dry-run leaves store empty. Metric collector test mirrors `metrics/methodblock_test.go`.
- [ ] **Step 2–4:** implement, run `-race`, revert-check the ceiling and the last-operator refusal.
- [ ] **Step 5:** `make docs`; commit `feat(admin): operator drain routes, ceiling and gauge`.

---

### Task 5: Chain-state reset

**Files:** `qos/plugin.go` (`StateResetter`), `qos/blockconsensus.go` (`Reset()`: drop observations, zero `perceived`, zero `externalFloor`, restart grace), `qos/endpointstore.go` (`Clear()`), the three plugins (`ResetState()` calling both; EVM also clears archival marks — they live in the store, so `Clear` covers it), `router/admin_state.go` (+test; route `POST /admin/chain-state/clear/{serviceID}` per spec §4), docs.

- [ ] Tests: `BlockConsensus`: observe heights → `PerceivedBlock() > 0`; `Reset()` → 0; `EndpointStore`: set two, `Clear`, `Count()==0`; each plugin: `UpdateBlockHeight` then `ResetState()` → `PerceivedBlockHeight()==0` and `SelectEndpoints` passes everything; route: plugin with the interface → `{"reset":true}`, noop plugin → `{"reset":false,...}`, unknown service → 404.
- [ ] Revert-check: make `ResetState` a no-op in EVM → its test fails.
- [ ] `make docs`; commit `feat(admin): chain-state reset`.

---

### Task 6: Runtime seams for reload

**Files:** `cmd/sagegw/wire.go` (`cfg` snapshot: `App` gains `Config atomic.Pointer[config.Config]`; `newRetryFn`/`newTimeoutFn`/the method-blocks knob reads load from it), `methodblock/store.go` (`SetTTL(d)`, `SetEscalation(n)` under the write lock; the middleware already calls `Mark`, which reads them), `featureflag` (nothing new — `Set`/`SetForService`/`Delete` exist), `healthcheck` (`SetConfiguredChecks` exists), `protocol/shannon` (`SetBlockedDomains` from Task 3).

- [ ] Tests: `newRetryFn` reads the NEW snapshot after `App.Config.Store(newCfg)` (table: hedge_delay 0 → 300ms); `methodblock` setters change subsequent `Mark` behaviour (TTL 0 disables).
- [ ] Commit `refactor(wire): config snapshot behind an atomic pointer; method-block setters`.

---

### Task 7: Reload orchestration + route + SIGHUP

**Files:** create `cmd/sagegw/reload.go` (+test), `router/admin_reload.go` (+test), modify `cmd/sagegw/main.go` (SIGHUP → same function), `router/admin.go` (`Reloader` interface `Reload(ctx) (ReloadResult, error)` injected; nil → 501).

`ReloadResult{Applied, NeedsRestart, Ignored, Inert, Warnings []string}`. `reload.go`: `func (a *App) Reload(ctx) (ReloadResult, error)`: `config.LoadFromFile(a.ConfigPath)` (store the path on `App` at build); run the same validations boot runs (`config.ValidateAdmin`, `shannon.ValidateBlockedDomains`, plugin `Config.Validate` per service, chain-order validation — factor boot's validation into one `validateForBoot(cfg) error` used by both if it is not already one function); diff sections with `reflect.DeepEqual` on the typed sub-structs; apply per the spec §5 table; serialise with a mutex. `needs_restart` names the top-level yaml key that changed (e.g. `gateway_config.services`, `redis`, `router`, `admin_config.addr`).

- [ ] Tests: write a temp config from `config/testdata/path_config.yaml`, build an App in mock mode (see `cmd/sagegw/wire_test.go` `mockConfig`), edit `hedge_delay` → `Reload` reports `applied` contains the defaults key and `newRetryFn` returns the new value; edit a service's `chain_id` → `needs_restart` names it; write invalid YAML → error, nothing applied; remove a flag override → flag returns to default. Route test: 200 with the JSON; nil reloader → 501.
- [ ] Revert-check: skip the `Config.Store` → the hedge_delay test fails.
- [ ] `make docs`; commit `feat(admin): config reload with an honest diff; SIGHUP`.

---

### Task 8: `traffic` sampler

**Files:** create `traffic/doc.go`, `traffic/sampler.go`, `traffic/sampler_test.go`.

**Interfaces:**
```go
func New(opts ...Option) *Sampler   // WithRate(n int), WithWindow(d), WithMaxFingerprints(n)
func (s *Sampler) Observe(serviceID domain.ServiceID, payloads []domain.Payload)
type MethodStats struct { Sampled, Distinct int; DistinctRatio float64 }
type Fingerprint struct { Method string; Count int; Share float64; Sample string }
type Summary struct {
    ServiceID string; WindowStart, WindowEnd time.Time
    Sampled, Distinct, Overflow int; DistinctRatio, Top1Share float64
    PerMethod map[string]MethodStats
}
func (s *Sampler) Summary(serviceID domain.ServiceID, previous bool) (Summary, bool)
func (s *Sampler) Top(serviceID domain.ServiceID, previous bool, n int) []Fingerprint
func (s *Sampler) Services() []domain.ServiceID
```
Fingerprint per spec §6 (fnv64 of method + "\x00" + compacted params with `id` removed; use `gjson` to pull `method`/`params` and `json.Compact` on the params raw); non-JSON-RPC: `httpMethod + path + compactedBody`. `Method` in `Fingerprint`/`PerMethod` is the plugin-independent raw method — **bounded** by only appearing in the admin JSON, never a metric label. Windows roll on the first `Observe` after `WindowEnd`. `Sample` is the first 200 bytes of the payload for that fingerprint.

- [ ] Tests: rate 1 (sample everything): 100 identical `eth_getBalance` payloads differing only in `id` → Distinct 1, Top1Share 1.0; 100 distinct addresses → Distinct 100, ratio 1.0; per-method stats; maxFingerprints 10 with 50 distinct → Distinct 10, Overflow 40; window roll → previous holds the old counts; rate 100 → about 1% sampled over 10k (tolerance); concurrent `Observe` under `-race`.
- [ ] Commit `feat(traffic): request-shape sampler`.

---

### Task 9: Sampler hook, flag, gauges, routes

**Files:** `featureflag/defaults.go` (`FlagRequestSampler = "request_sampler"`, default true), `relay/middleware/observe.go` (`Observe(flags, queue, repSvc, sampler *traffic.Sampler)` — nil-safe; call `sampler.Observe(ctx.ServiceID, ctx.Payloads)` when the flag is on, before the queue submit), `metrics/traffic.go` (+test; collector reading `Summary(svc, previous=true)` → `sage_request_sample_distinct_ratio{service_id}`, `sage_request_sample_top_share{service_id}`), `router/admin_traffic.go` (+test; routes per spec §6, `top` capped at 100, `window` default `previous`), `router/admin.go` (`AdminAPI` gains `sampler *traffic.Sampler`), config `observation_pipeline.request_sample_rate`? — **no**: keep defaults as constants this pass (YAGNI), note in spec §8 if wanted.

- [ ] Tests: Observe middleware records into the sampler when the flag is on and not when off; collector output for one service; route JSON shape for a service with data and 404 for unknown.
- [ ] `make docs`; commit `feat(admin): request-shape sampler wired, gauges and routes`.

---

### Task 10: Admin UI panels

**Files:** `router/admin_ui.html`. Add tabs: Drain (list active, form domain/duration/rpc_type/reason, dry-run checkbox, release buttons), Reset (button on the reputation tab: "Clear chain state"), Reload (button + pretty-printed result), Traffic (per-service summary table + top fingerprints). Follow the existing `loadX()` + `api()` pattern; no external fetches (CSP). Test: the existing `admin_ui_test.go` (if any) still passes; verify in a browser against `bench/mock-config.yaml` per the previous pass's method, and record what was clicked in the report.

- [ ] Commit `feat(admin-ui): drain, reset, reload and traffic panels`.

---

### Task 11: Wiring, docs, gate

**Files:** `cmd/sagegw/wire.go` (construct the drain store — Redis when `redisClient != nil`, `Start(ctx)` — `proto.SetDrains`; `App.ConfigPath`; sampler construction and injection into Observe + Admin; collectors registered; `NewAdminAPI` with every new dependency), `ARCHITECTURE.md` (admin section: list the new routes in prose, not counts; flag table row `request_sampler`).

- [ ] Full gate. Boot check against `local/beta-config.yaml` if present: `GET /admin/reputation/drain/pnf-anvil` → `[]`; `POST` drain `pocket.network` → 409 last operator; `POST` drain `purroofgroup.com` 5m → 200 and `sage_drained_operators` shows it; `POST /admin/chain-state/clear/pnf-anvil` → `{"reset":true}`; `POST /admin/reload` → 200 with `applied`/`needs_restart` arrays; `GET /admin/request-sample/pnf-anvil` after 200 relays → summary. Record exact commands.
- [ ] Commit `feat: wire the admin pass`.

---

### Task 12: Spec status + memory

- [ ] Spec status line → implemented (branch, no counts). Controller updates the memory files.

---

## Self-review against the spec

§3 drain semantics (ceiling refusal, release on 0, dry-run, last-operator refusal, lowercased operator, unscoped matches all) → Tasks 1, 4. §3.2 stores → Tasks 1, 2. §3.3 chokepoint + operatorOf memo → Task 3. §3.4 routes/gauge → Task 4. §4 → Task 5. §5 seams/diff/serialisation/SIGHUP → Tasks 6, 7. §6 sampler/hook/gauges/routes → Tasks 8, 9. §7 UI/docs/beta → Tasks 10, 11. Type consistency: `drain.Store` (T1–T4, T11), `NewAdminAPI` grows in T4, T5, T7, T9 — each task updates every call site; `traffic.Sampler` API (T8, T9, T11).
