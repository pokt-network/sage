// Package blocklist makes the operator domain ban (gateway_config.blocked_domains)
// dynamic: an admin can ban a domain, for every RPC type or a few, without a
// redeploy, and the ban reaches every replica and outlives a restart.
//
// The relationship to the two neighbours it is easy to confuse with:
//
//   - The config list (and SAGE_BLOCKED_DOMAINS) is the base. It is compiled
//     into the Shannon protocol at start and re-applied on a config reload.
//     Entries set here are ADDED to it; they cannot remove a config entry —
//     that is the file's, and a reload's, job.
//   - A drain (package drain) is per service, has a TTL and is meant for an
//     operator having a bad afternoon. A blocked domain is global, permanent
//     until released, and meant for an operator that is gone.
//
// A [Manager] owns the union: the base from config, the admin entries from a
// [Backend], and one apply function — the protocol's SetBlockedDomains — that
// the union is handed to whenever either half changes. It is the swap point a
// reload writes through, so config and admin never race for the same slot.
//
// The Redis backend is one HASH, sage:blocked_domains, field = domain. Every
// replica re-reads it on a short interval and re-applies when it changed, the
// way drains propagate. A write that reaches the local protocol but not Redis
// is reported as [ErrPropagation]: the ban is real on the replica that took
// the request and the caller is told the fleet does not have it.
package blocklist
