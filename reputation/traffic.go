package reputation

import (
	"github.com/pokt-network/sage/domain"
)

// TrafficCounter reports how much client traffic has graded an endpoint.
//
// It exists so the health-check executor can decide whether a probe would tell
// it anything it is not already being told. Every client attempt records a
// reputation signal, so a backend carrying traffic is being graded
// continuously; a probe against it buys a second copy of a fact the score
// middleware already has.
//
// The count is cumulative and in signal units, not relays: a single relay
// records more than one signal on this build. Callers therefore want the
// difference between two readings over a known window, not the absolute value,
// and must set any threshold in the same signal units the counter returns.
//
// Optional, like StateLister: a Service that cannot answer simply does not
// implement it, and the caller falls back to probing everything.
type TrafficCounter interface {
	// TrafficSignals returns the number of client-traffic signals recorded
	// against the reputation key this endpoint and RPC type map to, and
	// whether that key is known at all. An unknown key reports (0, false)
	// rather than (0, true): "never seen" and "seen, no traffic" are the same
	// number but not the same fact, and a caller diffing readings must not
	// treat a key appearing for the first time as a window with no traffic.
	TrafficSignals(serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (uint64, bool)
}

var _ TrafficCounter = (*serviceImpl)(nil)

// TrafficSignals implements TrafficCounter. It reads the same shard cache the
// selector reads, under the same read lock, and allocates nothing.
func (s *serviceImpl) TrafficSignals(serviceID domain.ServiceID, ep domain.EndpointAddr, rpcType domain.RPCType) (uint64, bool) {
	key := s.key(ep, rpcType)
	sh := s.shard(key)
	sh.mu.RLock()
	st, ok := sh.cache[serviceID][key]
	sh.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return st.TrafficAttempts, true
}
