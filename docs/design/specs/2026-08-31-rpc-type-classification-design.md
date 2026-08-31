# RPC-type classification for multi-surface chains

*2026-08-31. Status: proposed. Prompted by the mainnet 1% canary.*

## The problem

A chain often exposes more than one RPC surface on the same host. TRON serves
EVM-style JSON-RPC (`eth_blockNumber` at `/` or `/jsonrpc`) **and** its native
HTTP API (`/wallet/getnowblock`, `/walletsolidity/*`). A Cosmos chain serves
JSON-RPC (on EVM-compatible chains), the Cosmos REST gateway (`/cosmos/…`,
`/ibc/…`, and chain-specific namespaces like Pocket's `/pokt/…` and
`/poktroll/…`), and CometBFT (`/status`, `/block`). One `Target-Service-Id`
covers all of them; the surface is chosen by the request.

SAGE decides the surface in `detectRPCType` (`relay/middleware/parse.go`). It
recognises WebSocket upgrades, gRPC by content-type, a fixed CometBFT path
table, a fixed Cosmos-REST prefix table (`/cosmos/`, `/ibc/`), and a JSON-RPC
envelope in the body — and then **defaults everything else to JSON-RPC**.

That default is an EVM assumption. On a chain whose REST namespace is not one
of the two hardcoded prefixes, a REST request falls through to JSON-RPC and is
routed to JSON-RPC suppliers that do not serve it.

### Evidence (canary, 2026-08-31, `9d71015`)

Proven side by side against the `<service>.api.pocket.network` PATH twins:

| request | SAGE | PATH |
| --- | --- | --- |
| tron `/v1` `eth_blockNumber` | 200 (10/10) | 200 |
| tron `/v1/wallet/getnowblock` | `-32001` / 405 | 200, real block |
| pocket `/v1/cosmos/staking/v1beta1/validators` | 200 | 200 |
| pocket `/v1/poktroll/session/params` | 501 Not Implemented | 200 |

`eth_` JSON-RPC works everywhere. The native REST namespaces
(`/wallet/`, `/poktroll/`, `/pokt/`) are misrouted. On the canary this was
~17% of tron responses (`sage_relay_total{service_id="tron",status="405"}`).

It is a **pre-existing gap**, not a regression: the `rpc_type_fallbacks` fix
earlier in the day only stopped a *different* 405 source (per-supplier fallback
contaminating tron's JSON-RPC pool with REST-only suppliers). tron's reputation
keys are now 100% `json_rpc`, which is correct — and is also why a REST request
finds no REST supplier today (see Caveats).

## Why not a per-chain path table

Extending the hardcoded Cosmos table to tron's `/wallet/` and Pocket's
`/poktroll/` chases every chain's namespace forever: every Cosmos chain can add
a custom REST module, and every non-EVM chain has its own native API. A table
is a maintenance treadmill and drifts out of date silently — exactly the
failure the two existing tables already invite.

## Proposal

Flip the default. The surface is decided in this order:

1. **Self-identifying signals win first** — unambiguous on any chain:
   - `Upgrade: websocket` → WebSocket.
   - gRPC content-type → gRPC.
   - a JSON-RPC envelope in the body (`"jsonrpc"` present, or a bare batch
     array) → JSON-RPC.
2. **CometBFT path table** stays — it is a genuinely distinct surface with a
   small, stable set of well-known paths (`/status`, `/block`, …).
3. **Then key on the service's declared `rpc_types` plus the path.** A request
   addressed by a path *other than a JSON-RPC entry point* (`/`, `/jsonrpc`)
   on a service that declares `rest` → **REST**.
4. **Default JSON-RPC** only for services that do not declare `rest` — i.e.
   EVM, whose JSON-RPC is always at the root anyway.

Step 3 is the whole fix. It keys on `ServiceConfig.RPCTypes`, which every
service already sets (tron `["rest","json_rpc"]`, pocket
`["json_rpc","rest","comet_bft","websocket"]`), and on the path — no per-chain
enumeration. "Not a JSON-RPC entry point, on a REST-capable service, is REST"
is true for `/wallet/`, `/poktroll/`, `/pokt/`, and any future namespace.

### Ownership

Classification is chain semantics, and the architecture rule is that chain
semantics belong to the QoS plugin, not to a global table or to `config/`. The
principled shape is for the plugin to classify from the request — the Cosmos
plugin knows its surfaces, the generic/noop plugin applies the rule above —
with the global `detectRPCType` as the fallback. The minimal shipping version
is step 3 in `detectRPCType` keyed on the service config, which produces the
same behaviour without touching the `Plugin` interface; the plugin-owned
version is the follow-up if a chain ever needs to classify by something other
than path (a body shape, a method name).

## Caveats

- **Classification is half of it.** A REST-classified request must then reach a
  REST-staked supplier. Where a chain has none in the current session — tron's
  keys are all `json_rpc` today — a REST request finds an empty pool and fails
  legibly. That is a supplier-staking reality PATH lives with too, not
  something SAGE routes around; `rpc_type_fallbacks` covers the case where a
  fallback surface *is* stakeable.
- **Parity testing is per chain.** Each REST-capable service is validated
  against its `x.api.pocket.network` twin: standard Cosmos REST already matches
  (`/cosmos/staking` → 200 both), the native namespaces are the delta.
- **CometBFT vs REST ordering.** A Cosmos chain's `/status` is CometBFT, caught
  by step 2 before step 3 can call it REST. The CometBFT table stays ahead of
  the REST default for that reason.

## Not in scope

Rewriting a client's path (PATH answers tron JSON-RPC at both `/` and
`/jsonrpc`; SAGE forwards the client's path verbatim and both work, so no
rewrite is needed). `blocked_suppliers` and `endpoint_policy` are separate,
already-queued items.
