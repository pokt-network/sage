// Package traffic tells repeated traffic from diverse traffic, per service.
//
// Every quality signal SAGE has — latency, error rate, reputation — rewards
// the fastest answer. A cache-fronted operator answers a repeated request
// without ever touching a node, and by every one of those signals that looks
// identical to a fast, correct supplier. The only place the difference shows
// up is the shape of the traffic itself: the same request, over and over,
// versus genuinely distinct ones. [Sampler] reads that shape.
//
// # Fingerprinting
//
// Each sampled payload is reduced to a fingerprint. What decides the shape is
// the RPCType, not merely whether a method was parsed: a JSON-RPC-shaped type
// (json_rpc, and comet_bft when it carries a method — see rpctype.go)
// fingerprints as fnv64 of the method name, a NUL byte, and the "params"
// member with whitespace compacted out — "id" plays no part because it is
// never read. Everything else — REST, gRPC, comet_bft without a method,
// unknown — fingerprints on its HTTP verb, path, and compacted body instead,
// even when Method() is non-empty: a gRPC payload's method comes from the URL
// path while its body is protobuf, so treating it as JSON-RPC-shaped would
// find no "params" and collapse every distinct call to one fingerprint. Two
// requests that would hit a cache identically collapse to one fingerprint;
// two that would not, don't.
//
// The body-based fingerprint sees at most the first hashBytes (4 KiB) of the
// body: with no "params" member to reduce the payload to, an untrusted client
// would otherwise choose how much hashing each sampled relay does. Two such
// bodies identical in their first 4 KiB therefore share a fingerprint.
//
// The raw method name is bounded to the admin-facing JSON this package
// produces (per-method stats, top fingerprints) and MUST NEVER be attached to
// a metric label — a service can be pointed at by a client who controls the
// method string, and an unbounded label value is a cardinality attack on the
// metrics backend.
//
// # Windows and bounds
//
// Each service keeps a current and a previous fixed-length window. A window
// rolls into "previous" on the first SAMPLED Observe call made after its end
// time; there is no background ticker. A service that stops receiving traffic
// therefore keeps its windows exactly as they were, which is why
// [Sampler.PreviousWindow] — the gauge lister — disowns a previous window that
// ended more than two window lengths ago. An absent metric says "no recent
// traffic"; a stale one would keep describing traffic that stopped.
//
// Within a window, fingerprints are tracked up
// to maxFingerprints; a distinct fingerprint arriving after that cap is hit
// increments an overflow counter instead of being stored, so a service with
// unbounded cardinality costs a fixed amount of memory rather than an
// unbounded amount.
//
// Per-method stats are bounded the same way, on the same knob: a raw method
// string is itself client-controlled, so a client that keeps inventing method
// names is exactly the attack the fingerprint cap defends against, one level
// up. Once a window has tracked maxFingerprints distinct methods, a new one
// folds into a shared "_other" bucket instead of growing the table, and
// MethodOverflow counts the distinct method names that landed there.
//
// # Sampling
//
// Observe runs on every relay, so the unsampled path — the overwhelming
// majority of calls — must cost as close to nothing as a counter increment.
// Each service keeps its own atomic counter, outside the mutex that guards
// its windows: a request is sampled when the counter lands on a multiple of
// the configured rate, and one that is not sampled never takes a lock. A rate
// of 1 samples everything. Fingerprinting happens before the lock is taken,
// so sampled relays do not queue behind each other's hashing.
package traffic
