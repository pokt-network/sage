# Operations

Running SAGE in production: what to expose, what happens when a dependency
disappears, and what to do when routing goes wrong.

For the key-by-key config surface see [configuration.md](configuration.md); for
routes see [admin-api.md](admin-api.md); for metrics see [metrics.md](metrics.md).

## Ports

SAGE binds three listeners, and the split is a security boundary, not a
convenience.

| Listener | Config key | Default | Who should reach it |
|---|---|---|---|
| Relay | `router_config.port` | `3069` | clients, through your edge |
| Admin | `admin_config.addr` | `localhost:9091` | operators only |
| Prometheus | `metrics_config.prometheus_addr` | `:9090` | your scraper |
| pprof | `metrics_config.pprof_addr` | **off** | nobody, normally |

**The admin API is a control plane.** Anyone who reaches it can toggle
feature flags, reset reputation, clear circuit breakers, drain operators,
rebind WebSocket clients and reload the config. Enabling `shadow_mode` through
it stops the gateway answering client requests at all. It defaults to loopback
with no token; binding it anywhere else **requires** `admin_config.auth_token`
(or the `SAGE_ADMIN_TOKEN` environment variable, which overrides it) and the
gateway refuses to start otherwise. Send it as `Authorization: Bearer <token>`.
Put TLS in front of it all the same.

**pprof is off unless you set an address.** `/debug/pprof` hands out heap
dumps, and this process holds `gateway_private_key_hex` and every entry in
`owned_apps_private_keys_hex`. A heap dump is those keys. If you turn it on to
chase a leak, bind it to loopback and turn it off again.

## Deployment

```bash
make sage_build                          # → bin/sagegw, CGO disabled
./bin/sagegw -config /etc/sage/config.yaml
```

Config comes from `-config <path>` or the `GATEWAY_CONFIG` environment
variable. `GATEWAY_CONFIG` holds the YAML **content**, not a path to it, which
suits a config delivered as a secret rather than a mounted file.

There is a container image (`make docker_build` locally; `release.yml`
publishes `ghcr.io/pokt-network/sage:<tag>`, `:latest` and `:<sha7>` on a version tag;
the *Image* workflow (`gh workflow run image.yml --ref <branch>`) publishes
`ghcr.io/pokt-network/sage:<branch>-<sha7>` and `:<sha7>` from any branch on
demand without touching `latest`), and a mock backend for running without a full node or
suppliers at all:

```bash
./bin/sagegw -config bench/mock-config.yaml
```

The mock serves canned responses in-process. Use it for load tests and for
confirming the gateway's own behaviour without spending relays.

### Kubernetes

The image (`Dockerfile`, non-root, static binary) takes the config as a file
or as the `GATEWAY_CONFIG` environment variable. Prefer the file: `POST
/admin/reload` and `SIGHUP` re-read it, and there is nothing to re-read from
an environment variable.

```yaml
containers:
  - name: sagegw
    image: sage:latest
    args: ["-config", "/etc/sage/config.yaml"]
    ports:
      - {name: relay,   containerPort: 3069}
      - {name: admin,   containerPort: 9091}
      - {name: metrics, containerPort: 9090}
    env:
      - name: SAGE_ADMIN_TOKEN
        valueFrom: {secretKeyRef: {name: sage-admin, key: token}}
    volumeMounts:
      - {name: config, mountPath: /etc/sage, readOnly: true}
    livenessProbe:
      httpGet: {path: /livez, port: relay}
      periodSeconds: 10
    readinessProbe:
      httpGet: {path: /healthz, port: relay}
      periodSeconds: 5
    lifecycle:
      preStop: {exec: {command: ["sleep", "5"]}}
volumes:
  - name: config
    secret: {secretName: sage-config}   # it holds signing keys
```

- **Liveness is `/livez`, readiness is `/healthz`** (or `/health`; the two
  are the same check, `/healthz` is PATH's spelling). `/healthz` answers 503
  until the protocol layer has a session, and again whenever the full node
  is unreachable — right for taking a pod out of the Service, wrong for
  restarting it, which is what a liveness probe on it would do during a
  full-node outage. `/livez` is 200 whenever the process serves.
- The relay port is the only one to expose through the Service. Reach the
  admin port through `kubectl port-forward` or an internal Service with the
  token; scrape metrics from the pod.
- `admin_config.addr` must be `0.0.0.0:9091` (not the loopback default)
  for anything outside the container to reach it, and then the token is
  mandatory.
- Shutdown is graceful (`SIGTERM`, 10 s): in-flight relays finish, WebSocket
  clients get 1012 and reconnect. The `preStop` sleep lets the endpoint
  controller stop routing new connections first.
- Several replicas behind one Redis share drains, feature flags and health
  probes (only the elected leader sends probe relays; the others apply its
  results from the `sage:probes` stream). Reputation, method blocks and
  circuit breakers are per replica by design.
- `SIGHUP` on the container (`kubectl exec … kill -HUP 1`) or `POST
  /admin/reload` applies a changed config file without a restart; the
  response names what took effect and what needs a restart.

### Read the startup log

SAGE warns rather than fails on config it does not implement, so the startup
log is where you find out that a key you set is doing nothing:

- `config key ignored: SAGE does not implement this setting, and it has no effect`
  — the key has no field at all. It parsed, it does nothing.
- `feature flag ignored: SAGE has no such flag` — a name in `feature_flags`
  that is not in `featureflag.DefaultFlags`.
- `middleware is registered but not in the chain, so it will not run`.
- `admin API is reachable from outside this host` / `pprof is reachable from
  outside this host` — fix these before anything else.
- `Redis connection failed, running in local-only mode` — see below.

None of these stop the process. All of them mean the gateway you are running is
not the gateway you configured.

A key with a *declared* field but no code reading it produces **no warning at
all** — see the "Parsed but not implemented" list in
[configuration.md](configuration.md) before assuming a setting took effect.

## Degraded modes

SAGE is built so that losing a dependency narrows behaviour instead of stopping
it. Know which of these you are in before diagnosing a routing problem.

**Redis unreachable.** Reputation, feature flags and circuit-breaker state stop
being shared between instances; each falls back to its own local view. The
gateway keeps serving. Practically: each instance re-learns which endpoints are
bad from its own traffic, flags set through the admin API apply only to the
instance that received the call, and a circuit break opened on one instance
does not protect the others. Flag reads degrade to defaults for one cache TTL
rather than stalling the relay path.

**Pool collapse.** When every endpoint for a service scores below the minimum
threshold, selection returns the *least bad* one rather than nothing. Returning
nothing would turn a ranking system into a total outage on a service whose
suppliers are all still reachable. Watch for it — reputation is telling you
something is wrong with the whole pool, and it is serving anyway.

**Circuit breaks.** A broken domain is out of the pool for a while, and repeat
offenders are held out longer. `sage_circuit_breaker_state` reports what is
broken right now, computed at scrape time; a domain that recovers stops being
reported rather than lingering.

## Runbook

### Latency is up

1. `sage_relay_latency_seconds` — which service, which quantile.
2. `sage_retry_total` and `sage_hedge_total` — are requests being retried or
   raced? A hedge that fires constantly means `hedge_delay` is below the
   service's normal latency, and you are sending every request twice.
3. `sage_endpoint_reputation_score` — if scores are broadly healthy, the
   problem is upstream of selection.

### A service is returning errors

1. `GET /admin/circuit-breaker/{serviceID}` — is anything broken out?
2. `GET /admin/reputation/{serviceID}` — scores by key. Remember these are
   **backend URLs** by default, not endpoint addresses: several staked
   suppliers commonly share one.
3. `GET /admin/timeline/{serviceID}` — the event history that says *why* a
   score moved: the signal, the reason code, and the score before and after.
   This is the endpoint that answers "why is this endpoint not getting
   traffic". It is per-instance and bounded, so ask the instance that saw the
   problem, soon after.

### An endpoint was penalised for something since fixed

`POST /admin/reputation/reset/{serviceID}/{endpoint}` returns it to the initial
score across every RPC type. Otherwise it rehabilitates on its own through
probation traffic, which is slower but requires no intervention.

### A domain is broken out and you believe it is healthy

`POST /admin/circuit-breaker/clear/{serviceID}`. Escalation history deliberately
survives the clear: a domain let back in and immediately failing again is still
treated as a repeat offender. If it breaks again straight away, believe the
breaker.

### Turning behaviour off under load

Feature flags are the runtime switch, global or per-service:

```bash
curl -X PUT localhost:9091/admin/flags/{flag} -d '{"enabled":false}'
curl -X PUT localhost:9091/admin/flags/{flag}/{serviceID} -d '{"enabled":false}'
```

Per-service beats global. With Redis, the change reaches other instances within
their flag cache TTL; without it, only the instance you called.

`GET /admin/config` shows what the process is actually running — defaults
applied, flags as they stand now — which is not the same as the YAML on disk.

## What to alert on

- `sage_relay_total{status=~"5.."}` rising — supplier-side failures.
- `sum(sage_circuit_breaker_state)` above zero for a sustained period — a
  domain is not recovering.
- `service_id="__unknown__"` on any metric — traffic arriving for services this
  gateway does not serve. The label collapses on purpose so a hostile
  `Target-Service-Id` header cannot mint unbounded time series, but a spike
  means something is misrouted or probing you.
- `sage_degraded_total` rising — requests are being served from a degraded
  path.
