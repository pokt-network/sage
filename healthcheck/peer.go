package healthcheck

import (
	"context"
	"slices"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
)

// Reading another SAGE instance's probe results, read-only
// (active_health_checks.peer_probe_stream). Two instances serving the same
// services otherwise probe the same backends twice: every probe is a paid
// relay, and on 2026-09-14 the canary and the free-RPC instance each sent
// 1540 per 120 s cycle for the same suppliers. The instance that reads applies
// the other's results as its own and skips a check the other ran recently;
// anything the other does not cover (a service it does not serve, a backend
// outside its session, a result gone stale because it stopped publishing)
// this instance still probes itself. There is nothing to switch over: the
// fallback is the freshness window.

// SetPeerSource installs a read-only feed of another instance's probe results,
// and maxAge, resolved per service at read time so the tuning knob takes
// effect on the next cycle: how long one result stands in for this
// instance's own check, zero meaning the check's own interval. A nil maxAge
// is always zero. Wire time only; Start runs the feed on every replica, and
// the leader's schedule consults what it delivered.
func (e *Executor) SetPeerSource(s ProbeSource, maxAge func(domain.ServiceID) time.Duration) {
	e.peerSource = s
	e.peerMaxAge = maxAge
	e.peerSeen = make(map[probeKey]time.Time)
}

// backendKey names the backend an endpoint belongs to, exactly as
// groupByBackend keys its groups, so a peer's result and this instance's
// schedule agree on what "the same check" is.
func (e *Executor) backendKey(ep domain.EndpointAddr) string {
	if e.dedupByBackendURL.Load() {
		if url, err := ep.URL(); err == nil && url != "" {
			return url
		}
	}
	return string(ep)
}

// applyPeerResult lands one of the other instance's results on this
// instance's own registrations of the same backend, and records the check as
// covered.
//
// The translation is the point. The other instance's session is its own: its
// Siblings are the registrations IT holds, and block heights land per
// registration. Applied as sent, a backend this instance reaches through
// different registrations would get no height, and a check skipped on the
// strength of the result would leave those registrations height-less and
// filtered out of selection. So the result is re-pointed at this instance's
// registrations with the same backend URL. A backend this instance does not
// reach is not its business; a transport failure on a registration it does
// not hold may be the other session's problem rather than the backend's, so
// neither is applied, and neither counts as covered.
func (e *Executor) applyPeerResult(ctx context.Context, r ProbeResult) {
	if _, ok := e.sessions.ConfiguredServices()[r.ServiceID]; !ok {
		return
	}
	rpcType := r.RPCType
	if rpcType == "" {
		rpcType = domain.RPCTypeJSONRPC
	}
	eps, err := e.endpoints.AvailableEndpoints(ctx, r.ServiceID, rpcType)
	if err != nil {
		return
	}
	key := e.backendKey(r.Endpoint)
	var local domain.EndpointAddrList
	for _, ep := range eps {
		if e.backendKey(ep) == key {
			local = append(local, ep)
		}
	}
	if len(local) == 0 {
		return
	}
	held := slices.Contains(local, r.Endpoint)
	if r.TransportError != "" && !held {
		return
	}
	if !held {
		r.Endpoint = local[0]
	}
	r.Siblings = local
	r.Source = ResultSourcePeer
	e.applyResult(ctx, r)

	at := r.ProbedAt
	if at.IsZero() {
		at = e.now()
	}
	pk := probeKey{service: r.ServiceID, backend: key, check: r.Check}
	e.peerMu.Lock()
	if at.After(e.peerSeen[pk]) {
		e.peerSeen[pk] = at
	}
	e.peerMu.Unlock()
}

// coveredByPeer reports whether the other instance ran this check against
// this backend recently enough to stand in for this instance's own probe.
// The peer_probe_skip flag, per service, is the live off switch.
func (e *Executor) coveredByPeer(ctx context.Context, key probeKey, interval time.Duration, now time.Time) bool {
	if e.peerSource == nil {
		return false
	}
	if e.flags != nil && !e.flags.IsEnabled(ctx, featureflag.FlagPeerProbeSkip, key.service) {
		return false
	}
	var window time.Duration
	if e.peerMaxAge != nil {
		window = e.peerMaxAge(key.service)
	}
	if window <= 0 {
		window = interval
	}
	e.peerMu.Lock()
	at, ok := e.peerSeen[key]
	e.peerMu.Unlock()
	return ok && now.Sub(at) < window
}

// prunePeerSeen drops coverage older than maxBaselineAge, so a backend that
// left both sessions takes its entry with it rather than the map growing for
// the life of the process.
func (e *Executor) prunePeerSeen(now time.Time) {
	if e.peerSource == nil {
		return
	}
	cutoff := now.Add(-maxBaselineAge)
	e.peerMu.Lock()
	for k, at := range e.peerSeen {
		if at.Before(cutoff) {
			delete(e.peerSeen, k)
		}
	}
	e.peerMu.Unlock()
}
