package shannon

import (
	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// rpcFallbackTable is the per-service rpc_type_fallbacks mapping: for a
// service and a requested RPC type, the type to relay through when a supplier
// has not staked the requested one. Built once at construction and read on
// the hot path; nil is a valid empty table.
type rpcFallbackTable map[domain.ServiceID]map[domain.RPCType]domain.RPCType

// buildRPCFallbacks lifts the config's string map into typed form. Values are
// validated at config load, so an unknown name cannot reach here.
func buildRPCFallbacks(services []config.ServiceConfig) rpcFallbackTable {
	var table rpcFallbackTable
	for _, svc := range services {
		if len(svc.RPCTypeFallbacks) == 0 {
			continue
		}
		if table == nil {
			table = make(rpcFallbackTable)
		}
		m := make(map[domain.RPCType]domain.RPCType, len(svc.RPCTypeFallbacks))
		for from, to := range svc.RPCTypeFallbacks {
			m[domain.RPCType(from)] = domain.RPCType(to)
		}
		table[domain.ServiceID(svc.ID)] = m
	}
	return table
}

// resolve returns the fallback type for the service and requested type, or
// "" when there is none.
func (t rpcFallbackTable) resolve(serviceID domain.ServiceID, rpcType domain.RPCType) domain.RPCType {
	if t == nil {
		return ""
	}
	return t[serviceID][rpcType]
}

// anyStakes reports whether at least one endpoint in the session staked the
// RPC type — the pool-level question rpc_type_fallbacks turns on.
func anyStakes(endpoints map[domain.EndpointAddr]*endpoint, rpcType domain.RPCType) bool {
	for _, ep := range endpoints {
		if _, err := ep.GetURL(rpcType); err == nil {
			return true
		}
	}
	return false
}

// endpointURL is the URL a relay of rpcType goes to on ep: the staked URL for
// that type, or — one hop, when the service maps it — the URL of the fallback
// type. The request itself is not changed.
//
// This is per endpoint, where selection (Protocol.endpoints) applies the
// mapping pool-level like PATH. Both are needed: selection must not add a
// REST-only supplier to a json_rpc pool that has json_rpc suppliers, while a
// relay addressed to an endpoint that lacks the type — one handed out under
// the pool fallback, or a cosmos health check, which probes json_rpc-staked
// suppliers with the CometBFT GET /status they serve from the same port —
// still has to reach it.
func (p *Protocol) endpointURL(serviceID domain.ServiceID, ep *endpoint, rpcType domain.RPCType) (string, error) {
	url, err := ep.GetURL(rpcType)
	if err == nil {
		return url, nil
	}
	if fallback := p.rpcFallbacks.resolve(serviceID, rpcType); fallback != "" {
		if fallbackURL, fbErr := ep.GetURL(fallback); fbErr == nil {
			return fallbackURL, nil
		}
	}
	return "", err
}

// SetRuledOut installs the reputation check the rpc_type fallback reads: it
// reports an endpoint whose recorded score for the RPC type has fallen below
// the floor selection considers at all. Call once at wire time.
func (p *Protocol) SetRuledOut(f func(domain.ServiceID, domain.EndpointAddr, domain.RPCType) bool) {
	p.ruledOut = f
}

// unusable reports whether a pool of the requested type cannot serve: empty
// after exclusions, or — when byReputation — every member ruled out by
// reputation.
func (p *Protocol) unusable(serviceID domain.ServiceID, rpcType domain.RPCType, pool domain.EndpointAddrList, byReputation bool) bool {
	if len(pool) == 0 {
		return true
	}
	if !byReputation || p.ruledOut == nil {
		return false
	}
	for _, ep := range pool {
		if !p.ruledOut(serviceID, ep, rpcType) {
			return false
		}
	}
	return true
}
