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
(the kalorius pair, which holds every comet_bft stake on shentu and
persistence) to 0 for answering client misses correctly, and paid for every
retry, each of which received the same body and was then delivered to the
client anyway (shentu: 1.43 paid relays per client request).

PATH passes a generic `-32603` through. It was right; the fix (`3ac89dc`)
matches it: wordings are matched against `message` and `data` together, so a
proxy wrapping `connection refused` in `data` still grades as the supplier's;
anything else at `-32603` is the node's answer, passed through, no retry, no
penalty, attribution blockchain. The reason label stays `internal_error`.
After the roll: retry resolutions on akash and shentu empty, comet_bft
`major_error` gone, kalorius scores climbing with no new penalties, shentu at
1.03 relays per client request.

The rule the user stated, which this note exists to preserve: *if the client
sent garbage the node returns garbage, and the supplier is not penalised for
it.*

## Finding 2: persistence's whole comet_bft face is one pruned host

The persistence capture was nine relays, all to `rm02.kalorius.tech`, six of
them `block` at heights 27k–98k on a chain past 25M, answered `height N is
not available, lowest height is 25052001`. On-chain, persistence's and
shentu's comet_bft stakes are kalorius only (rm02 229 addresses, rm01 42);
akash has some 220 rpcgate hosts besides. So it is neither a bad supplier nor
a lagging node: an archival query to a pruned face.

Three shapes were tried in one day, and the third is what stands:

1. `9b25fab` retried the pruned answer on another operator without penalty
   (the indicator table already graded that wording the chain's; a `-32603`
   envelope returned from Tier 2 and never reached it, and CometBFT's wording
   puts the number between the words, so `height is not available` never
   matched — `lowest height is` was added). On the canary every such retry
   exhausted on akash, shentu and persistence, zero recovered in fifteen
   minutes, akash with four non-kalorius hosts in session: the other
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
nodefleet's hosts (1324 of the first 2000 stakes; `pkp-og` alone 162
addresses on one URL) point that URL at a CometBFT RPC. An EVM call such as
`eth_blockNumber` reaches a CometBFT node about two thirds of the time and
gets `{"code":-32601,"message":"Method not found"}`, which is exactly
CometBFT's reply to an unknown method. `s023.rpcgate.xyz` stakes one
path-less URL for all five types and answered the same request ok and
`-32601` alternately. The supplier-side fix is nodefleet restaking kava
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
  predicted hosts (nodefleet non-custodial, nr, igniter, pkp-og, pkp-og-2,
  plus kleomedes' kava-json host) and kava's `json_rpc` success share went
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

- nodefleet outreach on kava (supplier side; fixes PATH too).
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
