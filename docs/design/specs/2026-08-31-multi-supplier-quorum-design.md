# Multi-supplier quorum / consensus relays

*2026-08-31. Status: proposed / scoping. Prompted by Otto.*

## Goal

Serve one client request by sending it to **N suppliers in parallel** (ideally
from **N distinct providers**), then return either:

- **Mode A — collect-all:** every supplier's individual answer, in one envelope,
  and let the caller apply its own logic; or
- **Mode B — consensus:** the **majority** answer, since responses to the same
  request at a synced height should be byte-identical.

This is opt-in and paid: N suppliers = N paid relays per request. It is a
redundancy/trust feature, not the default path.

## What already exists (and what this reuses)

- **`crossvalidation`** already hashes response digests and computes a majority
  / outlier set — but *passively, in the background*, over responses that
  naturally flow to different suppliers, feeding reputation. This feature is the
  **active, per-request, client-facing** version. The digest + majority logic is
  reused for the vote; the fan-out is new.
- **Hedge** already fans one request to a second supplier and prefers a
  different **operator** (`EndpointAddr.Operator()` = registrable domain, so all
  `*.nodefleet.net` hosts are one provider). It is race-to-first, not
  compare-and-vote, but the operator-diversity and detached-arm machinery are
  the model for the fan-out.
- **`Protocol.SendRelay`** signs and sends one relay to one endpoint — the arm
  primitive.
- Scoring already grades every attempt, so N arms naturally produce N
  reputation signals.

## Shape

A new **`MWConsensus`** middleware, gated by a `consensus` feature flag, that —
when triggered for a request — **takes over the select→send tail**: it selects
N endpoints itself, fans out N signed relays, aggregates, and returns *without*
running the normal single `SelectEndpoint`→`Score`→`SendRelay`. When not
triggered it is a pass-through. It is mutually exclusive with retry/hedge for
that request (you do not hedge a quorum).

Placement: at the retry/hedge tier (outer), so it owns selection and sending
for its arms; it holds the reputation service (to pick N across operators) and
the relayer (to send each arm), like `SendRelay` does.

### 1. Trigger — headers only, on every existing endpoint (decided)

**No special route.** Quorum is activated by headers on any existing endpoint,
so it works for every service transparently and composes with the existing
`Target-Service-Id` routing — a client adds a header to a request it already
sends, no URL migration:

- `Target-Quorum-Count: 4` — number of suppliers to fan out to (N).
- `Target-Quorum-Mode: consensus | collect` — which aggregation.
- Optional config `services[].quorum: { count, mode, allowed_methods }` as a
  default when the headers are absent, so a service can be always-quorum.

The **header is also the contract for the response shape**, which is why a
special path is unnecessary: consensus mode returns the *normal* response shape
(the majority answer — fully transparent), and collect mode returns the envelope
only because the caller set `Target-Quorum-Mode: collect`. Echo the fan-out back
with response headers `X-Quorum-Count` / `X-Quorum-Achieved` so even consensus
mode signals the cost.

Bounds: **default N = 3, cap = 9, and N is forced odd** (config `quorum.default`
/ `quorum.max`). Odd N is deliberate — `floor(N/2)+1` is then always a clean
majority (2 of 3, 3 of 5, 5 of 9) with no ties. N is also clamped to the number
of distinct operators actually available.

**Trust boundary:** because any request can now trigger N paid relays via a
header, the *edge* (taiji) owns who may set these headers — it strips them from
untrusted clients and can meter/bill on them. SAGE flag-gates per service and
clamps N, but the abuse boundary is the edge, the same trust model as any
privileged header. This is what a special path would otherwise be for (isolated
auth/billing), and the edge already provides it.

### 2. Selection — N across distinct providers

Pick the top-N endpoints by reputation spanning **N distinct operators**
(registrable domains), so the N answers are genuinely independent. Reuse the
selector + `ExcludeOperators` iteratively, or add `reputation.SelectN(service,
eps, rpcType, n)` that returns the best endpoint per operator until N or
operators run out. If fewer than N operators exist, fan out to as many as there
are and record the achieved fan-out (a 2-of-2 is weaker than 3-of-4, and the
response should say so).

### 3. Fan-out

N clones of the context, each with its endpoint pre-selected, each sent via
`Protocol.SendRelay` in its own goroutine (`safego.Call`, panic→error). Unlike
hedge, the arms are **not** detached — the request waits for them:

- **Consensus mode (early-return, decided):** return as soon as `floor(N/2)+1`
  arms share a digest — the majority is already decided, so waiting for the
  stragglers only adds latency. The stragglers are left to finish detached and
  self-score (they still feed reputation/cross-validation); the client is not
  made to wait for them. Odd N guarantees this threshold is reached or the arms
  are exhausted with a genuine no-majority.
- **Collect mode:** wait for all arms, or the relay timeout, then return whatever
  arrived.

Each arm is bounded by the per-attempt timeout machinery already in place.

### 4. Aggregation

- **Collect:** an envelope, e.g.
  `{"quorum": {"count": 4, "responses": [{"supplier": "...", "operator": "...",
  "http_status": 200, "body": <raw>}, ...]}}`. Raw bodies pass through; the
  caller decides.
- **Consensus:** canonicalise each response, hash it, group by hash. If a group
  reaches `floor(N/2)+1`, return its body (the normal response shape). **If no
  group reaches a majority once the arms are exhausted, fall back to collect
  mode** — return the envelope of all answers. This makes the disagreement
  *evident* to the caller (the response shape itself changes to the multi-answer
  envelope, with an `X-Quorum-Majority: false` header) rather than hiding it
  behind a hard error or a silently-picked arm.

### 5. Canonicalisation — the hard part

Byte-equality is too strict and height-sensitivity is real:

- **Insignificant differences:** JSON key order, whitespace, and the JSON-RPC
  `id` echo must be normalised out before hashing (parse → drop `id` →
  canonical-encode `result` → hash). REST/CometBFT bodies: canonical-JSON when
  the body is JSON, raw hash otherwise.
- **Height sensitivity (the real limiter):** for **immutable** queries
  (`eth_getTransactionReceipt`, a historical block by hash/number, a finalized
  balance) responses at any synced height match — consensus is meaningful.
  For **latest-height** queries (`eth_blockNumber`, `eth_getBalance` at
  `latest`, a pending nonce) suppliers one block apart *legitimately* differ, so
  a naive digest vote frequently has no majority or picks a stale value. Options:
  (a) restrict consensus to an **allowed-methods** list of immutable queries and
  reject/collect the rest; (b) a **height-tolerant** comparison for known
  latest-height methods (accept answers within ±k blocks, return the highest);
  (c) leave it to the caller (document that consensus on latest-height is racy).
  **Decided: start with (a), an explicit immutable-method allowlist** for
  consensus mode; a consensus request for a non-allowlisted (height-sensitive)
  method is answered in collect mode instead (evident, not silently wrong).
  Collect mode itself takes any method. Height-tolerant comparison (b) is a
  later refinement if wanted.

### 6. Reputation & cross-validation

Each arm scores normally. A supplier that is the **lone dissenter** in a quorum
is a strong outlier signal — feed the per-request majority/outlier straight into
the existing `crossvalidation` path (a much higher-confidence sample than the
background version, because the N were queried simultaneously).

### 7. Cost & safety

- N paid relays per request — the caller is opting into that. Surface it: a
  metric `sage_quorum_relays_total{service, mode}` and the achieved fan-out in
  the response so cost is never silent.
- A malformed or oversized `Target-Quorum-Count` is clamped, never trusted.
- Consensus with no majority must fail **legibly** (a defined error), not return
  a random arm.

## Decisions (settled 2026-09-01)

1. **Trigger:** headers only, on every existing endpoint; edge owns the trust
   boundary. No special route.
2. **N:** default 3, cap 9, forced odd (clean majority, no ties).
3. **Height sensitivity:** immutable-method allowlist for consensus mode;
   height-sensitive methods answered in collect mode. Height-tolerant compare is
   a later refinement.
4. **No majority:** fall back to collect mode (evident disagreement), not a hard
   error or a silently-picked arm.
5. **Consensus early-return:** yes — return at `floor(N/2)+1`, stragglers finish
   detached and self-score.

Still to pin during build: the exact collect-envelope JSON shape and the initial
immutable-method allowlist contents.

## Not in scope

Weighted voting, cross-region provider selection beyond the operator (domain)
split, and turning this into the default path. Consensus reuses the existing
digest logic; it does not change background cross-validation.
