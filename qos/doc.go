// Package qos is the plugin system that carries chain-specific knowledge, plus
// the machinery those plugins share.
//
// The gateway itself knows nothing about eth_blockNumber, CometBFT status
// responses, or Solana slots. Everything a chain understands about its own
// requests and responses lives behind [Plugin], registered per service on a
// [Registry] and looked up by domain.ServiceID.
//
// # A small core, extended by interface
//
// [Plugin] has two methods — parse a request, filter the endpoint list. That
// is the whole obligation for a new chain. Every further capability is an
// optional extension interface in plugin.go: [BlockHeightTracker],
// [ArchivalDetector], [HealthChecker], [DataExtractor], [CachePolicy],
// [CoalescenceClassifier], and the rest. A plugin gains a capability by
// implementing the interface; callers type-assert for it and skip the feature
// when absent.
//
// Add capability by adding an interface, never by widening [Plugin]. A new
// method on the core interface is a compile error in every existing plugin and
// forces four chains to stub out a feature one of them needed.
//
// # Shared machinery
//
// A new chain should not be reimplementing storage or consensus. This package
// holds the pieces the chain plugins have in common: [EndpointStore] is a
// generic per-endpoint data store, blockconsensus.go derives a perceived chain
// head from disagreeing endpoints, and selector.go holds the filtering
// helpers. Reputation and tiering are deliberately *not* here — those are
// generic infrastructure applied to every service, and a plugin that scored
// its own endpoints would be competing with the reputation package rather than
// informing it.
//
// # Chain semantics belong here, not in config
//
// Config carries per-service values opaquely; this package owns what they
// mean. An EVM chain_id is hex and compares numerically, a CometBFT chain_id
// is a name that compares exactly, and neither rule generalizes — so each
// plugin validates its own config at wire time. [ErrWrongChain] exists for the
// same reason: an endpoint confidently serving somebody else's chain is a
// different failure from an endpoint returning garbage, and the two escalate
// differently.
package qos
