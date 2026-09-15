# CometBFT verdicts, the EVM face of Cosmos chains, and height-aware routing

**Status:** shipped to the mainnet canary in five images on 2026-09-13
(`b81e31b`, `d18ef60`, `5bae566`, `ed10baf`, `cdc82d9`), on branch
`feat/rpc-type-source-metrics`, not merged to `main`.

This note records what the canary showed once SAGE could see its own
verdicts, what was decided about each finding, and what is still open. It is
the state of the work as of the evening of 2026-09-13; the register in
`docs/path-compat.md` carries the one-line versions.

## What made the findings visible

`sage_heuristic_verdicts_total{service_id, rpc_type, reason, attribution}`
(`b81e31b`) counts, once per client relay attempt, what the heuristic
concluded about the upstream answer. Before it, only two slices of that were
visible: `sage_retry_total` saw what retried and `sage_reputation_attempts_total`
saw what scoring kept, and scoring drops client-attributed outcomes before it
counts them. An answer that was passed through, or graded the client's,
appeared in neither. Every finding below came out of the first hour of that
counter plus one ten-minute `debug_log` window on kava and persistence.

The window itself needed two things the canary did not have: the pods ran at
`logger_config.level: error` from a config file that is a sealed secret, and
the level had no runtime seam. `SAGE_LOG_LEVEL` (startup, announced as a
warning) and `GET`/`PUT /admin/log-level` (live, per process, not persisted)
were added in `d18ef60` for that. The canary pods now run at `info` through
the admin route; a restart drops them back to the file's `error` until the
file is changed or the environment variable is set.

## Finding 1: a CometBFT node's own error was scored against the supplier

CometBFT wraps every handler error as JSON-RPC `-32603` with `message` fixed
at `"Internal error"` and the reason in `data`: `tx (…) not found`,
`transaction indexing is disabled`, `height N is not available, lowest
height is M`. The heuristic read `message` only, so on a CometBFT service it
learned nothing from the body, and it graded any `-32603` that matched none
of its supplier wordings as `internal_error`: retry, major penalty,
attribution unknown. On akash and shentu that scored the comet_bft operator
(one operator's pair of hosts, which holds every comet_bft stake on shentu and
persistence) to 0 for answering client misses correctly, and paid for every
retry, each of which received the same body and was then delivered to the
client anyway (shentu: 1.43 paid relays per client request).

PATH passes a generic `-32603` through. It was right; the fix (`3ac89dc`)
matches it: wordings are matched against `message` and `data` together, so a
proxy wrapping `connection refused` in `data` still grades as the supplier's;
anything else at `-32603` is the node's answer, passed through, no retry, no
penalty, attribution blockchain. The reason label stays `internal_error`.
After the roll: retry resolutions on akash and shentu empty, comet_bft
`major_error` gone, that operator's scores climbing with no new penalties, shentu at
1.03 relays per client request.

The rule the user stated, which this note exists to preserve: *if the client
sent garbage the node returns garbage, and the supplier is not penalised for
it.*

## Finding 2: persistence's whole comet_bft face is one pruned host

The persistence capture was nine relays, all to one host (rm02), six of
them `block` at heights 27k–98k on a chain past 25M, answered `height N is
not available, lowest height is 25052001`. On-chain, persistence's and
shentu's comet_bft stakes are that one operator's only (rm02 229 addresses, rm01 42);
akash has some 220 hosts of a second operator besides. So it is neither a bad supplier nor
a lagging node: an archival query to a pruned face.

Three shapes were tried in one day, and the third is what stands:

1. `9b25fab` retried the pruned answer on another operator without penalty
   (the indicator table already graded that wording the chain's; a `-32603`
   envelope returned from Tier 2 and never reached it, and CometBFT's wording
   puts the number between the words, so `height is not available` never
   matched — `lowest height is` was added). On the canary every such retry
   exhausted on akash, shentu and persistence, zero recovered in fifteen
   minutes, akash with four other operators' hosts in session: the other
   operators prune too. The three services paid 31–42% more relays for it.
2. `70315e8` made it pass through again, under its own reason
   (`height_not_available`) so it stays countable.
3. `cdc82d9` made selection learn. The plugin reads the lowest held height
   out of the answer in `ExtractData`, on either face, and keeps it **per
   host** (rm02's 229 addresses teach one entry), for an hour, bounded,
   cleared by the chain-state reset. `SelectEndpoints` reads the requested
   height off the request — CometBFT `block`, `block_results`, `commit`,
   `validators`, `consensus_params`, `header`, `abci_query` with object or
   positional params, `blockchain` by `minHeight`, the GET forms with
   `?height=`, and the REST `blocks/{height}`, `validatorsets/{height}`,
   `txs/block/{height}` routes — and excludes hosts known pruned below it, in
   every tier, like the EVM plugin's archival filter. Latest, zero or no
   height means no filter. When nothing in the pool holds the height the
   filter empties, the shared selector returns the full list, the query is
   sent once and the node's answer is delivered; when any host holds it, it
   is found deterministically. Zero extra relays either way.

Not covered: the REST `x-cosmos-block-height` header, and positive evidence
(a host that served an old height is not marked capable; only pruned hosts
are marked). The EVM plugin's archival marks are still keyed per supplier
address, not per host; aligning them is a follow-up.

## Finding 3: two thirds of kava's `json_rpc` stakes front a CometBFT node

On kava `json_rpc` means EVM. Every supplier stakes JSON_RPC there, and
one operator's hosts (1324 of the first 2000 stakes; one of its hosts alone
162 addresses on one URL) point that URL at a CometBFT RPC. An EVM call such as
`eth_blockNumber` reaches a CometBFT node about two thirds of the time and
gets `{"code":-32601,"message":"Method not found"}`, which is exactly
CometBFT's reply to an unknown method. One host of another operator stakes one
path-less URL for all five types and answered the same request ok and
`-32601` alternately. The supplier-side fix is that operator restaking kava
JSON_RPC to an EVM endpoint; the addresses and counts are with ops.

PATH's cosmos QoS buckets every EVM method as `eth_other` and counts a
`-32601` envelope as ok, so PATH clients on kava have been getting
method-not-found on most EVM calls with no signal. SAGE named it
`method_not_found` on the first hour of the verdict counter, graded it the
client's, did not retry, and could not mark the host, because the cosmos
plugin named no EVM methods and the method-block store skips the `_other`
bucket by design.

Three changes:

- `449ab70` — the cosmos plugin names catalogued EVM methods through the EVM
  plugin's catalogue (`evm.KnownMethod`), so a `json_rpc` host that refuses
  an `eth_` method is marked for it and traffic moves to the hosts that
  serve EVM. Fifteen minutes after the roll the marks named exactly the
  predicted hosts (five of that operator's hosts, plus a third operator's
  kava JSON-RPC host) and kava's `json_rpc` success share went
  from 13% to 46%.
- `5bae566` — a `-32601` on a **catalogued** method is retried once on
  another operator, attribution still client, nothing scored; the heuristic
  middleware consults the QoS registry for the catalogue, and an
  uncatalogued name keeps the analyzer's no-retry so a bogus method cannot
  bounce across the pool. PATH passes the body to the client. 60% of those
  retries recovered.
- `7be33ca` — client-attributed marks live 30 minutes
  (`method_blocks.client_ttl`) instead of the 5 minutes sized for a timeout
  mark; at 5 minutes the pool re-learned each host and method every five
  minutes, one paid failed relay each, and kava's relays per client request
  had gone from 1.31 to 1.64.

Client-attributed marks still never escalate to a host-wide block, and the
block key is (service, host, method) without the RPC type; a client sending
an `eth_` body under a `comet_bft` header would mark the host across faces.
If that shows up, the RPC type joins the key.

SAGE has no `evm_chain_id` for cosmos services, so it cannot probe
`eth_chainId` on the `json_rpc` face and filter the pool up front. That is
the steady-state answer once the method blocks have shown the shape.

## The RPC-type model this all respects

Nothing above changes how a request is typed or pooled. On a chain that
fronts several surfaces (kava, sei, pocket): the `RPC-Type` header wins; gRPC
media type is `grpc`; a Cosmos SDK path — `/cosmos/...`, `/ibc/...`, and any
chain's own module routes such as `/pokt-network/poktroll/...` — is `rest`
by the generic rule (a path on a rest-declaring service that is neither a
JSON-RPC entry point nor a CometBFT path), with no per-chain table; a
CometBFT GET path or a JSON-RPC body carrying a CometBFT method is
`comet_bft` when the service declares it; any other JSON-RPC body is
`json_rpc`. `rpc_type_fallbacks` remains the pool-level bridge for sessions
with no `comet_bft` staker. The changes here only alter verdicts and
per-host memory.

## Also shipped

- `e43b1fc` success verdicts export `attribution="none"`; internally a
  success carries `AttrClient` as "no action needed", which read as a client
  error beside the real ones.
- `ed10baf` the cross-validation sweep's outlier line moves to debug: it
  re-reports the same outliers every 30 seconds, digests of height-dependent
  answers differ legitimately, nothing acts on it, and at `info` it was 33
  lines a second.

## Open

- Outreach to the operator whose kava JSON_RPC stakes front CometBFT
  (supplier side; fixes PATH too).
- EVM archival marks keyed per host, like the cosmos pruned memory.
- `evm_chain_id` for cosmos services and an `eth_chainId` probe on the
  `json_rpc` face.
- RPC type in the method-block key, if a cross-face mark is ever observed.
- sei: `http_408` and `transport_timeout` on `json_rpc`, `server_error`,
  `transport_error` on `rest` — supplier side, unexamined.
- The paired SAGE-vs-PATH pull (same services, same hour, both canaries):
  client status, paid relays per client request, retries, exhaustion. The
  user's standing rule: judge from that data, expecting some findings to be
  SAGE-only, some shared.
- Merge of `feat/rpc-type-source-metrics` to `main`.

## Addendum, same evening: runtime control without the file

The debug window and the external-source findings both ran into the same
wall: the config file is a sealed secret, and the first live seams (log
level in `d18ef60`, external sources in `92fa15b`) were per pod and undone
by the next roll. The user's instruction was to make every such change
possible without a deploy, and to make it stick.

Package `override` is a flat string map in Redis (memory without it), under
`sage:overrides:`, with a poller that hands a key space to an apply function
whenever it changes. Four seams now live on it:

- `log_level` — `PUT`/`DELETE /admin/log-level`; cmd/sagegw watches the key
  and moves the process's `slog.LevelVar`; a restarted process starts at it.
- `external_sources/<service>` — `PUT`/`DELETE /admin/external-sources/{service}`;
  the manager keeps the file's sources underneath, an empty list means
  "stop polling", DELETE clears the override and returns to the file.
- `tuning/<knob>[/<service>]` — the tuning store writes through and reloads;
  readers that keep their own state (method-block TTLs and escalation, the
  observation sample rate, each plugin's sync allowance) re-pull through a
  change hook. Five knobs were added for it.
- `config` — `PUT /admin/config` takes the whole YAML, validates it as at
  startup, applies it through the reload seams (retry, hedge, timeout, flags,
  health checks, blocked domains, method blocks; service blocks are reported
  as needing a restart), persists it, and every replica applies it;
  `DELETE /admin/config` returns to the file; `POST /admin/reload` answers 409
  while an upload is in force so the file cannot silently undo it.

Every admin response says `persisted: true|false`, so an operator on a Redis-
less gateway knows the change is this pod only. A file reload still
re-applies the file's method-block knobs over a tuning override until the
next tuning change; noted, not fixed.

## Addendum: the paired SAGE-vs-PATH hour, 13:51–14:51Z on `1440b96`

Both canaries, same seven services, same hour; SAGE at ~2% of PATH's volume.
One operator's timeout wave that hit both gateways ended around 13:55, so the
first minutes carry its tail on both sides. Inside the hour: osmosis
`retry.max_retries` 3 and the nine retired external sources (14:20). Not yet:
the 10 s request deadline and the osmosis retry budget (14:47).

Client non-200 share, SAGE | PATH: akash 0.00% | 0.71%; kava 2.15% | 5.28%;
osmosis 6.00% | 5.70%; persistence 0.00% | 5.36%; pocket 0.00% | 1.92%;
sei 5.36% | 10.15%; shentu 0.35% | 0.91%. SAGE better on six, osmosis at
parity — its residual 500s were clients hanging up mid-retry, which
`8dd24b7` reports as 499 from 14:51Z.

Client-serving relays per client request, SAGE | PATH: akash 1.11 | 1.04;
kava 1.45 | 1.15; osmosis 1.29 | 1.13; persistence 1.03 | 1.01; pocket 1.23 |
1.02; sei 1.43 | 3.04 (PATH hedges sei); shentu 1.03 | 1.01. Counting probes
and health checks PATH spends more than SAGE on persistence, pocket, sei and
shentu, because PATH probes at 0.7–1.8 relays/s per service regardless of
traffic.

Exhaustion per request, SAGE | PATH: sei 0.098 | 0.089; kava 0.103 | 0.027;
osmosis 0.008 | 0.053; pocket 0.018 | 0.018; persistence 0 | 0.046; shentu
0 | 0.008; akash 0 | 0.006. Kava's is half `method_not_found` (the
retry-once path still costs one relay per newly met host and method) and a
quarter `timeout`; pocket's `timeout` exhaustion matches PATH's transport
figure and points at slow pocket suppliers, not examined.

Where SAGE spends more relays it is retries that recover half the time on
kava and sei; where PATH spends more it is hedging (sei) and probing.

## Addendum, evening: five more on the canary (`2318987`, live 22:25Z)

- Family marks (`041bee9`): a `-32601` on a catalogued `eth_` method marks
  the host for the whole EVM catalogue; seven kava hosts held 53 methods
  each after one visit, `method_not_found` exhaustion gone.
- `sage_client_latency_seconds` (`be348e1`): the caller's wait. First read,
  SAGE | PATH p50/p95/p99: osmosis 0.170/0.595/0.984 | 0.077/0.241/0.714;
  kava 0.079/0.435/1.923 | 0.043/0.203/0.353. The p99 gap on kava is retry
  chains; the p50 gap is not — SAGE's own per-relay p50 on osmosis is
  0.098 s, so about 70 ms sit inside SAGE between router and upstream on
  the median request. Open.
- EVM archival marks per host (`9f32c27`).
- Relay-miner statuses graded by status (`c73510e`): `upstream_5xx`,
  `upstream_4xx`, `upstream_429`, `upstream_413`; `transport_error` fell to
  near zero. Base's suppliers were sick for both gateways all afternoon
  (SAGE 4.2/s miner 5xx, PATH 22% errors); the new rows named it.
- Reputation keys and method-block hosts follow the URL dialed per RPC type
  (`2318987`): on osmosis the old `…-json…|rest` keys froze at the roll and
  the two operators' dedicated REST hosts (`…-rest…|rest`) carry the
  traffic.

Also live through the override store, no deploy: `timeout.relay_timeout`
10 s global (nothing needed a higher block: relays over 10 s were 0.02% of
traffic, 504 fell), osmosis `retry.max_retries` 3 and `retry.max_latency`
1500 ms, nine dead external sources retired. Ops keeps the register in
pnf-ops `organizations/pnf/apps/sage/RUNTIME-OVERRIDES.md`.

Open now: the ~70 ms in-process median gap; operator outreach on kava;
`evm_chain_id` for the Cosmos EVM face; RPC type in the method-block key
if a cross-face mark ever shows; the merge.

## Addendum 2026-09-14: the latency dig, closed

Three images (`d4453b9` per-stage timing, `c3da9a8` flag snapshot, `36fb1c1`
send_relay phases and the hedge accounting fix) answered where SAGE's
client latency goes.

- In-process overhead: 30 ms per request canary-wide before, 7 ms after,
  6.5 of which is `parse` reading the request body. The 23 ms that went was
  the feature-flag store's per-key Redis cache, whose 5 s TTL expired
  between requests on every service below 0.2 req/s, one or two round
  trips per flag-gated stage per relay. Flags are now served from a polled
  snapshot; propagation pod to pod is under a second.
- Inside `send_relay`: signing is ~0.3 ms per attempt everywhere; verify is
  ~1 ms except osmosis REST at 3.7 ms per attempt (body deserialisation);
  prepare is nothing. The round trip to the miner is ~96% of the upstream
  call. SAGE compute is not the gap.
- What remains against PATH on osmosis: SAGE relay p50 0.074 s vs PATH
  0.044 s (same suppliers), and SAGE client-minus-relay 64 ms vs PATH 3 ms.
  The first is host choice: SAGE picks uniformly inside a score tier and
  latency has reporting power only (`docs/scoring.md` §7.2), while PATH's
  selection bands exclude slow hosts. The second is retry serialisation at
  1.88 attempts per request, mostly miner 502s now graded `upstream_5xx`.
  Both are policy, not overhead, and both are now measurable per stage.

Left as measured: the phase counters are recorded on successful attempts
only, so their sum trails `send_relay` by the failed attempts' time
(osmosis 70 ms/req, arb-one 6 ms); the gap is itself the cost of failed
attempts and is readable as such.

## Addendum 2026-09-14, afternoon: eight from the ops reading

What the ops session found reading the canary after the tie-break image
(`363820a`), each fixed in its own commit, one image:

- Unmeasured hosts drew a full tier-mean share in the tie-break while their
  every attempt failed (the EWMA reads successes only, so they never became
  measured). Now half a share (`2 × mean` in the 1/latency weight): enough
  to measure a new host within a minute at canary volume.
- `DELETE /admin/flags/{flag}` existed only per service; the global PUT had
  no inverse. Added; per-service overrides survive it, and the memory store
  now agrees with the Redis store on that.
- The reputation reset pushed its target through the key function once per
  RPC type, so a target copied from the listing (`https://host|rest`) left
  four phantom keys at the initial score on pod 75mk4. The reset now matches
  recorded keys only: a listing key resets that face, a URL, a host or an
  endpoint address resets every face of that host; no match is a 404 and
  creates nothing. The response names the keys touched.
- Reputation state is per pod, so a reset had to be repeated on each pod's
  admin port. A reset is now announced in the override store
  (`reputation_reset/<service>/<target>`) and every replica applies
  announcements newer than its own start; older ones are what storage
  already hydrated.
- A node's 5xx (`http_5xx`) was critical with a breaker vote per event. Now
  major, no vote: one 5xx is a weak statement about a host. The breaker
  keeps its votes for connect failures, HTML error pages and empty bodies.
- The osmosis REST 5xx stream, on both gateways, was mostly
  `/cosmwasm/wasm/v1/contract/{addr}/smart/{query}` and
  `/cosmos/tx/v1beta1/txs/block/{height}`: routes a gRPC-gateway node
  answers 500 to by design when the query cannot be served. Each one was
  retried and scored against the host that had answered correctly.
  `qos.VerdictRefiner` lets the plugin re-attribute a verdict from the
  route; the cosmos plugin turns a node or miner 5xx there into
  `query_5xx`, the chain's answer, delivered, nobody scored. The rule is
  the route, not the chain — every Cosmos chain runs the same gateway.
- `relay_request` named the endpoint's primary host on REST attempts and
  carried no verb or path; `relay_response` carried no error. Both now
  name the URL dialed for the RPC type, and the request line carries
  `rpc_type`, `http_method`, `path`, `normalized_method`; the response
  line carries `error`. A debug capture is now comparable with PATH's.

Watch on the next image: `sage_heuristic_verdicts_total{reason="query_5xx"}`
should take what `http_5xx` and `upstream_5xx` on osmosis carried; osmosis
attempts per request should fall with it; the reset route answers with
`keys` and `persisted: true` and the other pod logs "reputation reset
applied from another replica".
