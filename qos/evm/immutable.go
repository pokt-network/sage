package evm

import (
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// IsImmutable reports whether a request's answer is fixed once it exists, so a
// quorum may decide it by majority (qos.ImmutableClassifier).
//
// Deliberately short. A method belongs here only when every synced node must
// return the same bytes for it: by hash, or by an explicit block number. A
// block tag ("latest", "safe", ...) names a moving target, and two honest nodes
// a block apart disagree on it, so a vote there measures lag rather than
// correctness. Near-head numbers can still be reorged; a reorg shows up as no
// majority, which falls back to collect mode rather than a wrong answer.
func (p *Plugin) IsImmutable(payload domain.Payload) bool {
	params := gjson.GetBytes(payload.Bytes(), "params")
	switch payload.Method() {
	case "eth_chainId",
		"eth_getTransactionByHash",
		"eth_getTransactionReceipt",
		"eth_getBlockByHash":
		return true
	case "eth_getBlockByNumber":
		return isExplicitBlockNumber(params.Get("0"))
	case "eth_getLogs":
		return params.Get("0.blockHash").Type == gjson.String
	}
	return false
}

// isExplicitBlockNumber reports whether a block parameter is a hex number
// rather than a tag.
func isExplicitBlockNumber(param gjson.Result) bool {
	if param.Type != gjson.String {
		return false
	}
	_, err := parseHexUint64(param.String())
	return err == nil
}
