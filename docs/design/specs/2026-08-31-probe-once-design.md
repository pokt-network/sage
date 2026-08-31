# Probe once, apply everywhere

Date: 2026-08-31. Status: implemented the same day. The `Leader` hook is the single seam a sharded assignment (leader and followers each probing a share of the backends, by rendezvous hash over live members) would replace; nothing else changes for that.

## Problem

`healthcheck.LeaderElector` elects a leader (Redis `SET NX`, 30 s TTL,
hand-over in ~6 s — verified with two replicas), and nothing reads
`IsLeader()`. Every replica runs every health check. A health check is a
paid relay against the app's stake, so a fleet of N replicas burns N× the
relays for one replica's worth of knowledge: on beta ~15 probe relays per
30 s cycle per replica, ~43k/day each.

Gating probes on the leader alone would blind the followers: reputation,
block heights and chain-id assertions are local state fed by those probes,
and `reputation.Storage` is write-behind only — nothing reads it back (and
with two replicas its HASH is already last-writer-wins).

## Shape

**Probe once, apply everywhere.** The unit shared is the probe *result*, not
the score. Each replica's reputation stays "my own traffic + the fleet's
probes", so there is no score merge and no ownership question; followers
end up with exactly what they would have had from probing, minus the relay.

1. `Executor.sendCheck` splits into `probe` (the `SendRelay`) and
   `applyResult` (everything after it: transport grading, `ExtractData`,
   block-height fan-out to siblings, the `RecordSignalOnce` signal, the
   observation). Leader and follower share `applyResult` byte for byte.

2. `runOnce` returns early on a follower (`Leader.IsLeader() == false`).
   Without Redis the elector is always leader — unchanged behaviour.

3. The leader publishes each `ProbeResult` to a Redis Stream
   (`sage:probes`, `XADD MAXLEN ~ 10000`): service, probe endpoint, its
   siblings, check name and RPC type, latency, and either the response
   (status + body, body capped at 64 KiB) or the transport error's grading
   (the `heuristic.AnalyzeTransportError` reason and severity — the error
   itself does not survive serialisation, its verdict does).

4. Followers `XREAD` the stream (blocking, batched) and run each result
   through `applyResult`. `ExtractData` runs locally — the plugins are the
   same binary. A replica that has just booted replays the last two cycles
   (`XRANGE` from `now − 2·interval`) as its baseline before blocking on new
   entries, so it is not blind for a cycle.

5. Failover: the new leader starts publishing; followers never cared who
   wrote. Two leaders during a TTL overlap publish duplicates; a follower
   applying one probe twice is one extra attempt on a key, not a fault.

6. `reputation.Storage` write-behind is left as it is and documented as
   last-writer-wins external state; making it leader-only is a separate
   decision.

## Interfaces

In `healthcheck`: `ProbeSink` (`Publish(ctx, ProbeResult) error`) and
`ProbeSource` (`Run(ctx, apply func(ProbeResult))`), both optional on the
executor; `Leader` (`IsLeader() bool`). `RedisProbeStream` implements sink
and source over a `RedisClient` subset (`XAdd`, `XRead`, `XRange`). Tests
use in-memory fakes; the Redis implementation is exercised against the
Docker Redis harness with two replicas.

## Metrics

`sage_health_check_results_total{service_id, source}` — `probe` for a
result this replica produced, `stream` for one it applied from the leader.
On a healthy fleet the leader shows `probe`, everyone else `stream`, and
the ratio is the relay saving made visible.

## Checks

Unit: follower runs no relays; leader publishes one result per check;
a follower applying a streamed result records the same signal and block
height a probe would; codec round-trip; a boot replays the recent window.
Two replicas on Docker Redis: follower's reputation rows and perceived
heights move with zero `probe` results of its own; kill the leader → the
other's `probe` count starts within ~6 s and the stream continues.
