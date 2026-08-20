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
//
// The inference has three states, not two. An endpoint that has never been
// asked for historical state has told us nothing, and is not the same as one
// that answered and said it does not retain it — so selection excludes only a
// fresh negative. Requiring proof of archival before serving an archival
// request excluded every unobserved endpoint, which at any moment is most of
// them, and the tier cascade then handed back the unfiltered list anyway.
//
// Observations come from client traffic that happened to name a historical
// block (see observeArchival), which is a free probe and the only kind the
// plugin gets: health checks ask for the head. That is also why the block
// parameter, not the method, is the gate — eth_getBalance(addr, "latest") is
// among the most common calls on the network and a pruned node answers it
// perfectly.
package evm
