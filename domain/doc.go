// Package domain holds the core types every other package speaks in:
// services, endpoints, payloads, RPC types, and typed relay errors.
//
// It has no dependencies on the rest of SAGE, and must keep it that way. Every
// other package imports domain; if domain imported back, the graph would have
// no root and "where does this type belong" would stop having an answer.
// Anything that needs configuration, logging, storage, or a network is by
// definition not a domain type.
//
// Two conventions here are load-bearing elsewhere:
//
// An [EndpointAddr] is a "supplier-url" pair, not a machine. A Shannon supplier
// is a staked registration, and one backend URL routinely sits behind several
// of them — which is why reputation scores per URL by default rather than per
// address, and why [EndpointAddr.Supplier] and [EndpointAddr.URL] exist as
// separate accessors.
//
// An [ErrorKind] is what retry, circuit-breaking, and reputation switch on.
// They classify by kind, never by matching on error strings, so a reworded
// upstream message cannot silently change routing behaviour.
package domain
