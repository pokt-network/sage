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

// extractBlockHeightFromResponse extracts a block height from a Solana
// JSON-RPC response. The only accepted source is result.blockHeight, as
// returned by getEpochInfo — a plain JSON number, not a hex string.
//
// result.absoluteSlot is deliberately NOT a fallback. A slot is not a block
// height: Solana skips slots, so absoluteSlot runs ahead of blockHeight by the
// number of skipped slots (tens of millions on mainnet). Mixing the two into
// one perceived-height consensus makes every endpoint reporting blockHeight
// look catastrophically behind the ones reporting absoluteSlot, and the
// sync-allowance check then ejects whichever group is in the minority. An
// endpoint that cannot report blockHeight contributes no height observation
// at all, which is the safe outcome — ExtractData already treats a missing
// height as "this response carries no height", not as an error.
func extractBlockHeightFromResponse(response []byte) (uint64, error) {
	bh := gjson.GetBytes(response, "result.blockHeight")
	if bh.Exists() && bh.Type == gjson.Number {
		v := bh.Uint()
		if v > 0 {
			return v, nil
		}
	}

	return 0, fmt.Errorf("solana: no blockHeight in response")
}

// extractBlockHeightForMethod is extractBlockHeightFromResponse plus the one
// other response shape that really is a block height: getBlockHeight answers
// with a bare number rather than an object.
//
// Gated on the request method, and that gate is the whole point rather than an
// optimization. getSlot also answers with a bare number, and a slot is not a
// height — accepting any bare number would reopen exactly the poisoning hole
// the absoluteSlot comment above describes, just through a different door.
// Naming the method is what distinguishes "the caller asked for a height and
// got one" from "this response happens to contain a number".
//
// This matters because an operator can configure health checks
// (active_health_checks.local). A configured getBlockHeight probe would
// otherwise run, pass, and contribute no height at all: the endpoint stays
// selectable — SAGE reads an unknown height as unjudgeable rather than stale —
// but nothing it reports ever reaches the staleness filter, so a genuinely
// lagging endpoint serving no user traffic is never caught. PATH hit the
// sharper version of this, where the same gap benched the endpoint outright.
func extractBlockHeightForMethod(request, response []byte) (uint64, error) {
	if h, err := extractBlockHeightFromResponse(response); err == nil {
		return h, nil
	}

	if gjson.GetBytes(request, "method").String() != "getBlockHeight" {
		return 0, fmt.Errorf("solana: no blockHeight in response")
	}

	result := gjson.GetBytes(response, "result")
	if result.Type == gjson.Number {
		if v := result.Uint(); v > 0 {
			return v, nil
		}
	}
	return 0, fmt.Errorf("solana: no blockHeight in response")
}

// coalescableMethods is the set of read-only Solana methods that are safe
// to coalesce (de-duplicate in-flight requests with the same key).
var coalescableMethods = map[string]bool{
	"getSlot":            true,
	"getEpochInfo":       true,
	"getRecentBlockhash": true,
	"getBlockHeight":     true,
}
