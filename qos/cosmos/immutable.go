package cosmos

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// IsImmutable reports whether a request's answer is fixed once it exists, so a
// quorum may decide it by majority (qos.ImmutableClassifier).
//
// The same rule as the EVM plugin's: a block, header, commit, result set or
// validator set pinned to an explicit height, and anything named by hash. An
// absent or zero height means the latest, which honest nodes a block apart
// disagree on. Checked on beta (pocket-lego-testnet, 2026-09-16): CometBFT
// block and block_results and REST blocks and validatorsets at height 100
// gave one canonical digest over eight calls each, and status gave five.
//
// Deliberately not here: abci_query at a height, since a pruned node answers
// the same query with an error, and the vote would count pruning.
func (p *Plugin) IsImmutable(payload domain.Payload) bool {
	switch payload.RPCType() {
	case domain.RPCTypeREST:
		return isImmutableRESTPath(payload.Path())
	case domain.RPCTypeJSONRPC, domain.RPCTypeCometBFT:
		method, height, hash := cometCall(payload)
		switch method {
		case "block", "header", "commit", "block_results", "validators":
			return height > 0
		case "block_by_hash", "header_by_hash", "tx":
			return hash != ""
		}
	}
	return false
}

// cometCall reads a CometBFT call from either face: a JSON-RPC POST, whose
// params are an object or a positional array, or an HTTP GET whose path names
// the method and whose query carries the params.
func cometCall(payload domain.Payload) (method string, height uint64, hash string) {
	if payload.HTTPMethod() == "GET" || len(payload.Bytes()) == 0 {
		u, err := url.Parse(payload.Path())
		if err != nil {
			return "", 0, ""
		}
		q := u.Query()
		return strings.ToLower(strings.Trim(u.Path, "/")), positiveInt(q.Get("height")), strings.Trim(q.Get("hash"), `"`)
	}
	params := gjson.GetBytes(payload.Bytes(), "params")
	h, hs := params.Get("height"), params.Get("hash")
	if params.IsArray() {
		h, hs = params.Get("0"), params.Get("0")
	}
	return strings.ToLower(payload.Method()), positiveInt(h.String()), hs.String()
}

// positiveInt parses a decimal height, zero for anything else.
func positiveInt(s string) uint64 {
	n, err := strconv.ParseUint(strings.Trim(s, `"`), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// isImmutableRESTPath reports whether a Cosmos REST path names a height or a
// transaction hash rather than the latest state.
func isImmutableRESTPath(rawPath string) bool {
	u, err := url.Parse(rawPath)
	if err != nil {
		return false
	}
	seg := strings.Split(strings.Trim(u.Path, "/"), "/")
	switch {
	case len(seg) == 5 && strings.Join(seg[:4], "/") == "cosmos/base/tendermint/v1beta1" &&
		(seg[4] == "blocks" || seg[4] == "validatorsets"):
		return false // the latest, when no height follows
	case len(seg) == 6 && strings.Join(seg[:4], "/") == "cosmos/base/tendermint/v1beta1" &&
		(seg[4] == "blocks" || seg[4] == "validatorsets"):
		return positiveInt(seg[5]) > 0
	case len(seg) == 5 && strings.Join(seg[:4], "/") == "cosmos/tx/v1beta1/txs":
		return isHex(seg[4])
	case len(seg) == 6 && strings.Join(seg[:5], "/") == "cosmos/tx/v1beta1/txs/block":
		return positiveInt(seg[5]) > 0
	}
	return false
}
