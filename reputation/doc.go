// Package reputation scores endpoints from observed behaviour and picks which
// one a relay should go to.
//
// [Service] is the contract: record a [Signal] about how an endpoint behaved,
// query a score, or ask for a selection. Scores live behind a [Storage] —
// in-memory or Redis-backed, so instances can share what they have learned —
// and writes are queued rather than synchronous, because the relay path should
// not wait on a round trip to record that a relay went fine.
//
// # What a score is attached to
//
// A score is keyed by (identity, RPC type), and "identity" is configurable —
// see key.go. Both halves matter.
//
// The identity defaults to the backend *URL*, not the domain.EndpointAddr.
// An endpoint address is a (supplier, URL) pair, a supplier is a staked
// registration rather than a machine, and one backend routinely sits behind
// several registrations. Scoring per pair makes every registration on a shared
// backend prove independently that the backend is broken — the pool learns one
// fact N times, at the cost of N times the failed relays. Coarser is not
// automatically better: per-domain and per-supplier each blend backends that
// fail independently. Per-URL is the granularity at which the thing being
// scored is one machine.
//
// The RPC type is *always* part of the key, at every granularity. A Shannon
// supplier stakes one service across several RPC types, and the relay miner
// behind a single staked URL routes each type to a different backend — so a
// dead WebSocket backend says nothing about that supplier's REST backend.
// Blending them ejects an endpoint from traffic it was serving correctly.
//
// # Selection
//
// [TieredSelector] classifies endpoints into tiers by score and cascades:
// best populated tier wins, and the pick *within* that tier is uniformly
// random rather than always-the-maximum. Deterministic best-picking herds
// every instance onto one endpoint and starves the pool of the traffic it
// needs to discover that anything else recovered. Probation routing prepends a
// low-scoring endpoint to a small share of requests for the same reason: an
// endpoint that never gets traffic can never prove it is healthy again.
//
// Two guards shape the result. The pool-collapse guard returns the least-bad
// endpoint when *every* endpoint scores below the minimum, because the
// alternative — returning nothing — turns a ranking system into a total outage
// on a service whose suppliers are all still reachable. Reputation exists to
// rank a pool, not to empty it. The operator concentration cap (see
// concentration.go) separately bounds any one operator's share of selections
// within the winning tier; it is opt-in at wire time behind a feature flag.
//
// [Service.SelectSpread] is the variant for long-lived connections: tier
// cascade with weighted-random inside the top tier, biased away from endpoints
// already carrying load. Concentrating every WebSocket bridge on the single
// highest-scoring endpoint is how a healthy supplier becomes an outage.
//
// # Timeline
//
// Every scoring event is also appended to a per-endpoint [TimelineEvent] ring,
// exposed through the admin API, which is what makes "why is this endpoint not
// getting traffic" answerable after the fact. Events store structured fields
// and render their human-readable form on read, so the relay path never pays
// for formatting a string an operator may never look at.
package reputation
