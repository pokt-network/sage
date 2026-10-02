package evm

import (
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// bytesResultMethods answer with DATA: hex-encoded bytes, "0x" followed by
// two hex digits per byte, "0x" for none. QUANTITY methods (eth_blockNumber,
// eth_getBalance, eth_estimateGas, …) are not here: "0x0" is their zero.
//
// On mainnet base (2026-10-02) one operator's hosts answered an eth_call that
// reverts on every other node ("execution reverted: TRANSFER_FROM_FAILED")
// with {"result":"0x0"}, on four suppliers, every time: a layer in front of
// the node rewriting the revert. A client's aggregator took it as a valid
// empty answer. No node encodes bytes that way.
var bytesResultMethods = map[string]bool{
	"eth_call":                    true,
	"eth_getCode":                 true,
	"eth_getStorageAt":            true,
	"eth_getRawTransactionByHash": true,
	"eth_getRawTransactionByBlockHashAndIndex": true,
}

// InvalidResult implements qos.ResultValidator: a DATA method's string result
// that is not "0x" followed by an even number of hex digits. An error answer,
// or a result that is not a string, is left to the analyzer.
func (p *Plugin) InvalidResult(payload domain.Payload, response []byte) (string, bool) {
	if !bytesResultMethods[payload.Method()] {
		return "", false
	}
	r := gjson.GetBytes(response, "result")
	if r.Type != gjson.String || validHexBytes(r.Str) {
		return "", false
	}
	shown := r.Str
	if len(shown) > 66 {
		shown = shown[:66] + "…"
	}
	return payload.Method() + " result " + shown + " is not hex bytes", true
}

var _ qos.ResultValidator = (*Plugin)(nil)

// validHexBytes reports whether s is "0x" followed by an even number of hex
// digits.
func validHexBytes(s string) bool {
	if len(s) < 2 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') || len(s)%2 != 0 {
		return false
	}
	for i := 2; i < len(s); i++ {
		switch c := s[i]; {
		case '0' <= c && c <= '9', 'a' <= c && c <= 'f', 'A' <= c && c <= 'F':
		default:
			return false
		}
	}
	return true
}
