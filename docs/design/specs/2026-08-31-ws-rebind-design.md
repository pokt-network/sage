# WebSocket session rebind

Date: 2026-08-31. Status: implemented the same day (bridge: `WithEndpointLost`, `WithRebindLimit`; relayer: `resolveEndpoint`, `untriedFirst`; registry: `ReplayFrames`, `TranslateClientFrame`, `TranslateEndpointFrame`; metric `sage_websocket_rebinds_total`). Builds on the subscription registry
(`qos.SubscriptionRegistry`, commit `62556c2`) and the liveness check
(`311706f`). PATH's equivalent is #522's rebind; this is not a port — SAGE's
bridge is generic and its signing is per supplier, so the split is different.

## Problem

A bridge is one supplier for its lifetime. When that supplier's socket dies
(close frame, write failure, or the new 60 s unresponsive verdict) the client
is sent 1012 and must reconnect, re-subscribe and lose whatever arrived in
between. Three healthy suppliers behind the pool do the client no good.

## Shape

1. **Endpoint loss is a hook, not a shutdown.** `websockets.Bridge` gains
   `WithEndpointLost(handler)`. On an endpoint-side read error (never a
   client-side one) with a handler set and rebinds left, the bridge calls the
   handler instead of `Shutdown`. The handler returns a new endpoint
   connection, a new `MessageProcessor` (signing is per supplier, so the
   processor cannot be reused), and the raw frames to replay. The bridge
   swaps both under a lock, starts a fresh endpoint read loop, signs and
   writes the replays, and carries on. Client frames that arrive during the
   swap wait on the lock; nothing is dropped. A handler error, or the cap
   (`WithRebindLimit`, default 3), ends the bridge with 1012 exactly as
   today.

2. **Selection lives in the relayer.** `WSRelayer` installs the handler per
   bridge. It records a major signal against the lost endpoint, lists the
   service's WebSocket endpoints, excludes every endpoint this bridge has
   already used (operator-aware, like retry), falls back to the full list if
   that empties it (a blip on the only host is still worth one reconnect),
   selects with the same tier cascade as `Open`, resolves the URL, dials
   with the same relay-miner headers, and builds a processor that shares the
   bridge's subscription registry. Load counters move from the old endpoint
   to the new one; per-frame reputation signals follow the current endpoint.

3. **The registry translates.** Replaying a subscribe to a new supplier gets
   a new subscription id (EVM, Solana) and an ack the client already had.
   `SubscriptionRegistry` becomes a translator on both directions:
   - `ReplayFrames()` returns each live subscription's original request with
     a fresh gateway-owned id (`"sage-replay-N"`) that cannot collide with a
     client id, and arms the registry to expect those acks.
   - `TranslateEndpointFrame(data) (out, forward)`: a replay ack is consumed
     (mapped new id → client's id, not forwarded); a notification carrying a
     new id is rewritten to the client's id; anything else passes.
   - `TranslateClientFrame(data) out`: an unsubscribe naming the client's id
     is rewritten to the supplier's id.
   Rewrites are byte splices at spans the classifier reports
   (`params.subscription`, `params[0]`, or `id` for CometBFT, where the
   request id is the subscription id and the replay id is what events then
   carry).

4. **Session rollover is a rebind.** (Changed the same day, after beta
   showed the relay miner closing its socket at the session boundary and
   the bridge rebinding into the new session only to be closed by its own
   watcher.) `watchSessionExpiry` follows the CURRENT session's end height,
   which the rebind handler moves forward; at the boundary it asks the
   bridge to replace its endpoint with `ErrBridgeSessionExpired`, which
   resolves a fresh session and does not count toward the rebind limit. A
   rebind that fails to advance the session (a stale cache) falls back to
   the close with 1012. Connections now outlive sessions, which is exactly
   why the stall watchdog (5) ships alongside.

## Not in this change

- ~~The stall watchdog.~~ Shipped alongside: `WithStallDetector` polls the
  registry every 5 s; live subscriptions with no data (and no subscribe
  ack) for 60 s count as an endpoint loss and take the rebind path.
  `sage_websocket_stalls_total`.
- Also added: `POST /admin/websocket/rebind/{service}` — every live bridge
  of the service replaces its supplier. A drill, and the companion to
  drain (which only affects new selections).
- Rebinding on the client's request, or on reputation changes.

## Checks

Unit: registry replay/translate per chain; bridge loses endpoint → handler
supplies a second echo server → client frames keep flowing, client sees no
close, replay arrives at the new endpoint, ack is not forwarded, notification
id is rewritten; handler error → 1012; cap → 1012. Beta: normal subscription
unchanged (0 rebinds); rebind itself is not reproducible on beta (one live
host) — the bridge test is the proof.
