// Package drain lets an operator bench one supplier operator for one service,
// for a bounded time, without touching the endpoints it happens to be
// serving right now.
//
// A drain is keyed on the operator's registrable domain (eTLD+1) — the same
// value domain.EndpointAddr.Operator() returns — not on endpoint addresses.
// Endpoint addresses rotate every session; a drain keyed on one would expire
// silently the moment the session rolls over, quietly un-benching an operator
// an admin believed was still off. PATH issue #526 is exactly this bug: a
// block keyed on a session-scoped address survived only as long as the
// session did. Keying on the operator instead makes the drain durable across
// rotation: an endpoint newly rotated in at a drained operator is benched the
// moment it appears, matched against the operator's domain rather than an
// address the drain never saw.
package drain
