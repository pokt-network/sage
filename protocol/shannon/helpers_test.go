package shannon

import (
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
)

// newTestLogger returns a slog.Logger that writes to stderr at debug level.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// buildMultiServiceSession builds service's session (app pokt1app, blocks
// 100-110) with one supplier staking endpoints, one URL per RPC type.
//
// It expresses the two stake shapes the single-URL fixtures cannot, and that
// the any-service index got wrong: call it once per service with the
// same supplier and URL for "one supplier, one URL, many services" (the
// endpoint address is the same in every session), or give the types
// different hosts for an operator that stakes one host per RPC type (the
// address names the JSON-RPC host; the other faces are dialed elsewhere).
func buildMultiServiceSession(service, supplier string, endpoints map[sharedtypes.RPCType]string) *sessiontypes.Session {
	var eps []*sharedtypes.SupplierEndpoint
	for _, rt := range slices.Sorted(maps.Keys(endpoints)) {
		eps = append(eps, &sharedtypes.SupplierEndpoint{Url: endpoints[rt], RpcType: rt})
	}
	id := service + "-session"
	return &sessiontypes.Session{
		SessionId: id,
		Header: &sessiontypes.SessionHeader{
			SessionId: id, ServiceId: service, ApplicationAddress: "pokt1app",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers: []*sharedtypes.Supplier{{
			OperatorAddress: supplier,
			OwnerAddress:    supplier + "-owner",
			Services:        []*sharedtypes.SupplierServiceConfig{{ServiceId: service, Endpoints: eps}},
		}},
	}
}

// splitHostFaces is the default fixture's stake: JSON-RPC at url, REST on
// another host, the shape of an operator that stakes one host per RPC type.
// Every session a test builds through buildTestSession has it, so code that
// takes the address's URL for the URL a face dials fails in any test, not only
// in one written for it.
func splitHostFaces(url string) map[sharedtypes.RPCType]string {
	return map[sharedtypes.RPCType]string{
		sharedtypes.RPCType_JSON_RPC: url,
		sharedtypes.RPCType_REST:     strings.Replace(url, "://", "://rest-", 1),
	}
}
