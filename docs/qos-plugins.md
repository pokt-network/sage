# Adding a chain

Chain-specific knowledge lives in a QoS plugin. The gateway core knows nothing
about `eth_blockNumber`, CometBFT status responses, or Solana slots — a plugin
does, and it is the only thing that does.

Read `qos/evm/` first. It is the fullest plugin in the tree, so it shows which
extensions a mature chain ends up wanting. Do not copy it wholesale; a new chain
should start with two methods and grow.

## The core interface is two methods

```go
type Plugin interface {
    ParseRequest(ctx context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error)
    SelectEndpoints(endpoints domain.EndpointAddrList, payloads []domain.Payload) (domain.EndpointAddrList, error)
}
```

That is the whole obligation. `ParseRequest` validates and extracts payloads —
use the `body` argument, never `req.Body`, which has already been consumed.
`SelectEndpoints` adds *chain-specific* filtering: block height, archival, sync
state. Generic reputation and tiering are infrastructure and already applied;
a plugin that scored its own endpoints would be competing with the reputation
package rather than informing it.

## Everything else is an optional extension

Capability is added by implementing an interface, never by widening `Plugin`.
Callers type-assert and skip the feature when it is absent.

| Interface | What it buys |
|---|---|
| `BlockHeightTracker` | per-endpoint height, perceived chain head |
| `ArchivalDetector` | route historical-state requests to archival nodes |
| `HealthChecker` | supply the plugin's own health check payloads |
| `DataExtractor` | pull height / chain ID / sync state out of responses |
| `CoalescenceClassifier` | mark methods safe to deduplicate in flight |
| `CachePolicy` | per-method response cache TTLs |
| `MethodNormalizer` | name a payload's method from a bounded catalogue, for method-aware state and metric labels |
| `ExternalFloorSetter` | take a trusted outside height (`services[].external_block_sources`) as a floor under the perceived head |
| `EndpointHeightLister` | list the latest height each endpoint reported, for `GET /admin/chain-state/{service}` |
| `StateResetter` | discard learned chain state (block consensus, per-endpoint heights, archival marks) via the admin chain-state reset route, without a restart |
| `SubscriptionClassifier` | read WebSocket frames and say which open, close or feed a subscription, so `qos.SubscriptionRegistry` can track what is live on a bridge (the knowledge a rebind and a stall watchdog need); the id lives in different places per chain (EVM: hex result / `params.subscription`; Solana: integer result / `<x>Notification`; CometBFT: the request id itself) |

A new method on the core `Plugin` interface is a compile error in every existing
plugin, and forces four chains to stub out a feature one of them needed. That is
why the split exists — respect it.

## Do not re-implement the shared machinery

| Need | Use |
|---|---|
| Per-endpoint state | `qos.EndpointStore[T]` |
| A chain head from disagreeing endpoints | `qos/blockconsensus.go` |
| Endpoint filtering helpers | `qos/selector.go` |

These exist because EVM, Cosmos and Solana each had their own copy once.

## Chain semantics are yours, not config's

Config carries per-service values opaquely — `chain_id` is just a string to it.
Your plugin owns the format, the validation, and the comparison, through its own
`Config.Validate` called at wire time (so a bad value is a startup failure).

The reason is that no rule generalizes. EVM reports hex from `eth_chainId`, and
`"0x1"` and `"0x01"` are the same chain, so comparison must be **numeric**.
CometBFT reports a name from `/status` like `cosmoshub-4`, which compares
**exactly**. Teaching `config/` either rule would be teaching it the wrong one
for the other chain.

When an endpoint reports a chain ID that disagrees, return `qos.ErrWrongChain`
rather than a generic extraction error. The two mean different things: a
malformed response is an endpoint having a bad moment, while a wrong chain ID is
a healthy endpoint confidently serving somebody else's chain. They escalate
differently — see `healthcheck.checkSignal`.

## Steps

1. **Package.** `qos/<chain>/`, with a `doc.go` explaining what is specific
   about this chain. The exported-comment lint will require it.
2. **`Config` + `Validate`.** Whatever the chain needs, validated at wire time.
3. **`Plugin`.** The two methods. Resist adding extensions until something needs
   them.
4. **Service type.** Add a `domain.ServiceType` constant if the chain is a new
   family, and a case in the `switch` in `cmd/sagegw.Build` that registers the
   plugin per configured service.
5. **Tests** next to the code. Canonical run is `-short -race`.

Chains that need no chain-specific handling at all use `qos/noop`, which relays
and scores but understands nothing about the payload. That is the honest choice
for a service you cannot yet model — better than a plugin that pretends to
validate.

## Inference is cached, not permanent

If you detect a property of an endpoint rather than being told it — archival
support being the standard case — give the determination a TTL. An endpoint that
answers a historical query today may prune tomorrow, and treating one probe as
permanent truth routes archival traffic to a node that has since dropped the
data. See `archivalTTL` in `qos/evm`.
