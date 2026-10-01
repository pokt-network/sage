package cosmos

import (
	"net/http"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// restLatestBlockPath is the gRPC-gateway route for the newest block.
const restLatestBlockPath = "/cosmos/base/tendermint/v1beta1/blocks/latest"

// restCanaryName is the REST canary's health check name, and the method label
// its answers are counted under.
const restCanaryName = "rest_head_canary"

// headTime reads the time of the newest block an answer names, for the
// answers that name one: CometBFT status (sync_info.latest_block_time) and
// block with no height (block.header.time), over GET or JSON-RPC, and the
// REST latest-block route. Time, not height, because it needs no agreement
// on which height scale perceived is kept in: on a chain whose EVM face
// feeds consensus, CometBFT and EVM numbers agree only where the flag says
// so. ok is false for any other request or an answer without the field.
func headTime(payload domain.Payload, response []byte) (time.Time, bool) {
	var field string
	switch {
	case payload.Path() == restLatestBlockPath:
		field = "block.header.time"
		if !gjson.GetBytes(response, field).Exists() {
			field = "sdk_block.header.time"
		}
	case isCometCall(payload, "status"):
		field = "result.sync_info.latest_block_time"
	case isCometCall(payload, "block") && !namesHeight(payload.Bytes()):
		field = "result.block.header.time"
	default:
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, gjson.GetBytes(response, field).String())
	if err != nil || t.IsZero() {
		return time.Time{}, false
	}
	return t, true
}

// namesHeight reports whether a CometBFT JSON-RPC call names a height, by
// name ({"height":"5"}) or by position (["5"]): a block at a height is
// history, graded by its own old time it would read as stale.
func namesHeight(body []byte) bool {
	params := gjson.GetBytes(body, "params")
	h := params.Get("height")
	if params.IsArray() {
		h = params.Get("0")
	}
	return h.Exists() && h.String() != ""
}

// isCometCall reports whether a request is the CometBFT method name, as a GET
// on its path with no query (a query names a height) or as a JSON-RPC call.
func isCometCall(payload domain.Payload, name string) bool {
	if payload.HTTPMethod() == http.MethodGet {
		return payload.Path() == "/"+name
	}
	return payload.Method() == name
}

// restHeadCanary is the state canary's Cosmos form: the REST latest-block
// route, the same URL every time, graded by the block's time
// (featureflag.FlagStateCanary). A response cache keyed on the URL serves it
// as old as it serves everything else.
func restHeadCanary() domain.Payload {
	return domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP(restLatestBlockPath, http.MethodGet)
}
