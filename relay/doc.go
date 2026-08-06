// Package relay is the middleware-chain core of the gateway: the [Handler]
// abstraction every relay concern is written against, the per-request
// [Context] that flows through it, and the registry and ordering rules that
// let a chain be composed from configuration rather than from code.
//
// The model is deliberately http.Handler-shaped. A [Handler] processes one
// relay; a [Middleware] wraps a Handler to add one concern; [Chain] composes
// them so the first in the list is outermost and runs first. Each middleware
// lives in its own file under relay/middleware and does one thing, which is
// what makes the chain reorderable at all.
//
// # Composition is data, not code
//
// A chain is built by name. A module registers a factory on a
// [MiddlewareRegistry] at wire time, an operator names it in
// gateway_config.middleware_chain, and [MiddlewareRegistry.BuildChain]
// assembles the two;
// omitting the config key falls back to [DefaultChainOrder]. This is the seam
// for extending the gateway without touching protocol code.
//
// Order still matters, so it is checked rather than trusted. Invariants that
// must hold — endpoint selection sitting inside retry so rotation actually
// rotates, for one — are declared as rules in chain_order.go and validated at
// startup. A name the ordering rules do not know is allowed anywhere in the
// chain, which also means it gets no protection: a middleware that must
// precede another needs its own rule, not a hopeful position in the default
// order.
//
// # Context is per-request-tree, not per-request
//
// [Context] is a flat struct of typed fields, each written by exactly one
// middleware and read by others. There is no generic values bag; new state
// means a new field. Before adding one, note that [Context.Clone] is a
// *shallow* copy — hedge racers and batch sub-relays each run on a clone, so
// every pointer, slice, and map field is shared across the whole request tree.
// Scalars are safe to write; anything else needs an atomic or must be treated
// as read-only. The race detector only catches the mistake if a test actually
// fans out.
package relay
