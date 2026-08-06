// Package circuitbreaker removes a failing supplier domain from the routable
// pool for a while, so the gateway stops spending relays on a host it already
// knows is broken.
//
// It breaks on *hostname*, not on endpoint. When a backend is down it is down
// for every supplier registration fronting it, and breaking per endpoint would
// make the pool rediscover the same outage once per registration. State is
// Redis-backed so instances share it, with a short local cache in front; as
// everywhere in SAGE, Redis is optional and its absence degrades to local-only
// rather than failing.
//
// # Rate, not first error
//
// The trigger is a failure *rate* over a sliding window, gated by a minimum
// failure count — not the first error. First-error breaking is
// volume-sensitive rather than quality-sensitive: the busiest operator reaches
// its first error soonest after every TTL expiry, so the healthiest
// high-traffic host is the one broken most often, and a domain fronting many
// endpoints takes a large share of the pool down with it. The minimum-failures
// floor exists because one failure on a quiet domain is a 100% failure rate.
//
// # Escalation has memory
//
// Repeat offenders are held out longer, up to a cap. That escalation state
// outlives the break itself (defaultEscalationMemory) on purpose: without
// memory across expiry, a domain that breaks again immediately after being let
// back in looks like a first offence forever.
//
// The same history is what makes batch fan-out safe. One bad upstream moment
// produces one MarkBroken per sub-relay, and escalating on each would drive a
// transient blip straight to the maximum TTL.
//
// # It is opt-in, per response
//
// A circuit break is a much heavier action than a retry, and the two are
// decided separately — see the heuristic package. Nothing here should be
// wired to trigger on "the relay failed"; it triggers on a response that
// specifically asked to circuit-break.
package circuitbreaker
