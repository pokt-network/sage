// Package shannon implements the Shannon protocol for SAGE.
package shannon

import (
	"fmt"

	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	sdk "github.com/pokt-network/shannon-sdk"

	"github.com/pokt-network/sage/domain"
)

// endpoint wraps a session endpoint with RPC-type URL mapping.
// Each endpoint belongs to a single supplier and may support multiple RPC types.
type endpoint struct {
	supplierAddr string
	// ownerAddr is the supplier's on-chain owner, from the session. Operators
	// rotate operator addresses and domains freely; the owner is the stake
	// they cannot move without unstaking, so it is what a ban and a per-owner
	// metric key on. Empty when the session did not carry it.
	ownerAddr string
	urls      map[domain.RPCType]string // rpc type → URL
	session   *sessiontypes.Session
}

// Supplier returns the supplier's operator address.
func (e *endpoint) Supplier() string {
	return e.supplierAddr
}

// Owner returns the supplier's owner address, or "" when unknown.
func (e *endpoint) Owner() string {
	return e.ownerAddr
}

// GetURL returns the URL for the given RPC type.
// Returns an error if the RPC type is not supported by this endpoint.
func (e *endpoint) GetURL(rpcType domain.RPCType) (string, error) {
	url, ok := e.urls[rpcType]
	if !ok || url == "" {
		return "", fmt.Errorf("endpoint %s does not support RPC type %s", e.supplierAddr, rpcType)
	}
	return url, nil
}

// urlPreference is the order PublicURL picks a URL in. Any fixed order would
// do; this one puts the transports most services stake first, so the address
// an operator sees is usually the one they think of as "the" endpoint.
var urlPreference = []domain.RPCType{
	domain.RPCTypeJSONRPC,
	domain.RPCTypeREST,
	domain.RPCTypeCometBFT,
	domain.RPCTypeGRPC,
	domain.RPCTypeWebSocket,
}

// PublicURL returns a stable representative URL for display and for Addr.
//
// Stability is the point, not which URL wins. Addr is built from this and is
// used as a reputation key, and ranging over e.urls returns Go's randomised map
// order — so a supplier staking several transports (all five, for Cosmos) got a
// different address on every process start, scattering its per-endpoint history
// across keys that look like different endpoints. Today's per-supplier
// granularity hides that; it would not survive switching to per-endpoint.
func (e *endpoint) PublicURL() string {
	for _, rpcType := range urlPreference {
		if url := e.urls[rpcType]; url != "" {
			return url
		}
	}
	// An RPC type this build does not know about: still has to be deterministic,
	// so take the lowest URL rather than whichever the map hands over first.
	lowest := ""
	for _, url := range e.urls {
		if url != "" && (lowest == "" || url < lowest) {
			lowest = url
		}
	}
	return lowest
}

// serviceID is the service of the session the endpoint came from, "" when
// it carries none.
func (e *endpoint) serviceID() domain.ServiceID {
	if e.session == nil || e.session.Header == nil {
		return ""
	}
	return domain.ServiceID(e.session.Header.ServiceId)
}

// Session returns the session this endpoint belongs to.
func (e *endpoint) Session() *sessiontypes.Session {
	return e.session
}

// Addr returns the unique address for this endpoint in "supplierAddr-url" format.
func (e *endpoint) Addr() domain.EndpointAddr {
	return domain.EndpointAddr(fmt.Sprintf("%s-%s", e.supplierAddr, e.PublicURL()))
}

// rpcTypeMapping maps poktroll sharedtypes.RPCType to domain.RPCType.
var rpcTypeMapping = map[sharedtypes.RPCType]domain.RPCType{
	sharedtypes.RPCType_JSON_RPC:  domain.RPCTypeJSONRPC,
	sharedtypes.RPCType_REST:      domain.RPCTypeREST,
	sharedtypes.RPCType_COMET_BFT: domain.RPCTypeCometBFT,
	sharedtypes.RPCType_WEBSOCKET: domain.RPCTypeWebSocket,
	sharedtypes.RPCType_GRPC:      domain.RPCTypeGRPC,
}

// endpointsFromSession extracts all endpoints from a session, grouped by supplier address.
// Returns a map from EndpointAddr to *endpoint for efficient lookup.
func endpointsFromSession(session *sessiontypes.Session) map[domain.EndpointAddr]*endpoint {
	sf := sdk.SessionFilter{
		Session: session,
	}

	allEndpoints, err := sf.AllEndpoints()
	if err != nil {
		return nil
	}

	owners := make(map[string]string, len(session.Suppliers))
	for _, s := range session.Suppliers {
		if s != nil {
			owners[s.OperatorAddress] = s.OwnerAddress
		}
	}

	result := make(map[domain.EndpointAddr]*endpoint)
	for _, supplierEndpoints := range allEndpoints {
		if len(supplierEndpoints) == 0 {
			continue
		}

		supplier := string(supplierEndpoints[0].Supplier())
		ep := &endpoint{
			supplierAddr: supplier,
			ownerAddr:    owners[supplier],
			urls:         make(map[domain.RPCType]string),
			session:      session,
		}

		for _, se := range supplierEndpoints {
			domainRPC, ok := rpcTypeMapping[se.RPCType()]
			if !ok {
				continue
			}
			ep.urls[domainRPC] = se.Endpoint().Url
		}

		result[ep.Addr()] = ep
	}

	return result
}
