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

// endpointURL is the URL a relay of rpcType goes to on ep: the staked URL for
// that type, or — one hop, when the service maps it — the URL of the fallback
// type. The request itself is not changed; the fallback exists because relay
// miners commonly serve CometBFT's HTTP and JSON-RPC surfaces from one port.
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
