package solana

import (
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
)

// parseRequest validates the pre-read request body and returns a single
// JSON-RPC Payload. The body must be a JSON object with a "method" field.
func parseRequest(body []byte) (domain.Payload, error) {
	if len(body) == 0 {
		return domain.Payload{}, fmt.Errorf("solana: empty request body")
	}

	method := gjson.GetBytes(body, "method")
	if !method.Exists() || method.String() == "" {
		return domain.Payload{}, fmt.Errorf("solana: missing or empty method field")
	}

	return domain.NewPayload(body, domain.RPCTypeJSONRPC, method.String()), nil
}

// extractBlockHeightFromResponse tries to extract a block height from a Solana
// JSON-RPC response. It checks two locations in order:
//  1. result.blockHeight  — returned by getEpochInfo
//  2. result.absoluteSlot — also returned by getEpochInfo; used as fallback
//
// Both values are plain JSON numbers (not hex strings).
func extractBlockHeightFromResponse(response []byte) (uint64, error) {
	// Try result.blockHeight first.
	bh := gjson.GetBytes(response, "result.blockHeight")
	if bh.Exists() && bh.Type == gjson.Number {
		v := bh.Uint()
		if v > 0 {
			return v, nil
		}
	}

	// Fall back to result.absoluteSlot.
	slot := gjson.GetBytes(response, "result.absoluteSlot")
	if slot.Exists() && slot.Type == gjson.Number {
		v := slot.Uint()
		if v > 0 {
			return v, nil
		}
	}

	return 0, fmt.Errorf("solana: no blockHeight or absoluteSlot in response")
}

// coalescableMethods is the set of read-only Solana methods that are safe
// to coalesce (de-duplicate in-flight requests with the same key).
var coalescableMethods = map[string]bool{
	"getSlot":            true,
	"getEpochInfo":       true,
	"getRecentBlockhash": true,
	"getBlockHeight":     true,
}
