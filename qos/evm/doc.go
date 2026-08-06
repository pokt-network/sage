// Package evm is the QoS plugin for EVM JSON-RPC chains — Ethereum and every
// network that speaks its RPC surface.
//
// It implements qos.Plugin plus most of the optional extension interfaces:
// block height tracking and parsing, archival detection, health checks,
// response data extraction, per-method cache policy, and coalescence
// classification. It is the fullest plugin in the tree, and therefore the one
// to read first when writing a new chain — not to copy wholesale, but to see
// which extensions a mature plugin ends up wanting.
//
// Two EVM-specific rules are worth knowing before changing anything here.
//
// Chain IDs are hex strings that must compare *numerically*. "0x1" and "0x01"
// are the same chain, and a string comparison would wrongly eject an endpoint
// as serving the wrong network. This is exactly why chain-ID semantics live in
// the plugin rather than in config — CometBFT's IDs are names that compare
// exactly, and neither rule generalizes.
//
// Archival status is inferred, not declared, so it is cached with a TTL
// (archivalTTL). An endpoint that answers a historical-state query today may
// prune tomorrow; treating one probe as permanent truth routes archival
// traffic to a node that has since dropped the data.
package evm
