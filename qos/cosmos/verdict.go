package cosmos

import (
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
)

var _ qos.VerdictRefiner = (*Plugin)(nil)

// ReasonQuery5xx is the verdict reason for a 5xx on a REST route the node
// answers 5xx to by design when the query itself cannot be served: the
// answer is the chain's, delivered as is, nothing retried, nobody scored.
const ReasonQuery5xx = "query_5xx"

// query5xxPrefixes are the REST routes a gRPC-gateway node turns a query
// failure into a 5xx on. The prefix match is on the raw path (query string
// and variable segments included), because the variable segments are what
// the client got wrong.
//
//   - /cosmwasm/wasm/v1/contract/{addr}/smart/{query}: a smart query the
//     contract rejects ("query wasm contract failed") is a 500. On the
//     2026-09-14 canary this route was most of osmosis's REST 5xx stream
//     for both gateways, and SAGE scored every one against the host.
//   - /cosmos/tx/v1beta1/txs/block/{height}: a height the node does not
//     hold, or one past the head, is a 500 rather than a 404.
//
// Every Cosmos chain runs the same gateway, so these are routes, not
// chains; a chain module route that behaves the same way belongs here too.
var query5xxPrefixes = []string{
	"/cosmwasm/wasm/v1/contract/",
	"/cosmos/tx/v1beta1/txs/block/",
}

// RefineVerdict implements qos.VerdictRefiner: a 5xx verdict (the node's
// http_5xx, or the miner's upstream_5xx relaying it) on one of the routes
// above becomes the chain's answer to the client's query.
func (p *Plugin) RefineVerdict(payload domain.Payload, result heuristic.AnalysisResult) (heuristic.AnalysisResult, bool) {
	if result.Reason != "http_5xx" && result.Reason != "upstream_5xx" {
		return result, false
	}
	if payload.RPCType() != domain.RPCTypeREST || !isQuery5xxPath(payload.Path()) {
		return result, false
	}
	return heuristic.AnalysisResult{
		Attribution: heuristic.AttrBlockchain,
		Confidence:  0.7,
		Reason:      ReasonQuery5xx,
		Details:     "5xx on a route the node answers 5xx to for a query it cannot serve (" + result.Reason + "): delivered, not retried, nobody scored",
	}, true
}

func isQuery5xxPath(path string) bool {
	for _, prefix := range query5xxPrefixes {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		if prefix == "/cosmwasm/wasm/v1/contract/" && !strings.Contains(path, "/smart/") {
			// The contract's other routes (state, history, raw) do not
			// carry a client-authored query.
			return false
		}
		return true
	}
	return false
}
