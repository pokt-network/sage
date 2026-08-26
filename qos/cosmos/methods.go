package cosmos

import (
	"strings"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// knownRESTTemplates is the catalogued set of gRPC-gateway paths, with every
// variable segment written as :var. A request path is templated (see
// templatePath) and then looked up here; anything unlisted is MethodOther.
var knownRESTTemplates = map[string]bool{
	"/cosmos/base/tendermint/v1beta1/blocks/latest":        true,
	"/cosmos/base/tendermint/v1beta1/blocks/:var":          true,
	"/cosmos/base/tendermint/v1beta1/node_info":            true,
	"/cosmos/base/tendermint/v1beta1/syncing":              true,
	"/cosmos/base/tendermint/v1beta1/validatorsets/latest": true,
	"/cosmos/base/tendermint/v1beta1/validatorsets/:var":   true,
	"/cosmos/bank/v1beta1/balances/:var":                   true,
	"/cosmos/bank/v1beta1/balances/:var/by_denom":          true,
	"/cosmos/bank/v1beta1/supply":                          true,
	"/cosmos/auth/v1beta1/accounts/:var":                   true,
	"/cosmos/auth/v1beta1/account_info/:var":               true,
	"/cosmos/tx/v1beta1/txs":                               true,
	"/cosmos/tx/v1beta1/txs/:var":                          true,
	"/cosmos/tx/v1beta1/txs/block/:var":                    true,
	"/cosmos/tx/v1beta1/simulate":                          true,
	"/cosmos/staking/v1beta1/validators":                   true,
	"/cosmos/staking/v1beta1/validators/:var":              true,
	"/cosmos/staking/v1beta1/delegations/:var":             true,
	"/cosmos/distribution/v1beta1/delegators/:var/rewards": true,
	"/cosmos/gov/v1/proposals":                             true,
	"/cosmos/gov/v1/proposals/:var":                        true,
	"/cosmos/gov/v1beta1/proposals":                        true,
	"/cosmos/gov/v1beta1/proposals/:var":                   true,
	"/ibc/core/channel/v1/channels":                        true,
	"/ibc/core/client/v1/client_states":                    true,
	"/ibc/apps/transfer/v1/denom_traces":                   true,
	"/ibc/apps/transfer/v1/denom_traces/:var":              true,
}

// templatePath replaces every path segment that is a number, a hex string
// (optionally 0x-prefixed, at least 8 hex digits), or a bech32 address
// (a lowercase prefix, its '1' separator, and 36 or more data characters)
// with :var, and drops the query string. What survives is the route, which is
// ours to catalogue; what is removed is the client's, which is not.
func templatePath(path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	segs := strings.Split(path, "/")
	for i, s := range segs {
		if isVariableSegment(s) {
			segs[i] = ":var"
		}
	}
	return strings.Join(segs, "/")
}

// isVariableSegment reports whether a path segment is a request-specific
// identifier (numeric, hex, or bech32) rather than a fixed route component.
func isVariableSegment(s string) bool {
	if s == "" {
		return false
	}
	if isDigits(s) {
		return true
	}
	h := strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if len(h) >= 8 && isHex(h) {
		return true
	}
	// bech32: hrp + '1' + data, lowercase, long. The '1' is the first one in
	// the segment (the separator right after the human-readable prefix); a
	// real 20-byte Cosmos address has 38+ data characters after it.
	if i := strings.IndexByte(s, '1'); i > 0 && len(s)-i >= 37 && s == strings.ToLower(s) {
		return true
	}
	return false
}

// isDigits reports whether s consists only of ASCII digits.
func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isHex reports whether s consists only of ASCII hex digits.
func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// NormalizeMethod implements qos.MethodNormalizer. CometBFT JSON-RPC methods
// come from cometBFTMethods verbatim; REST paths are templated and matched
// against knownRESTTemplates.
//
// A CometBFT GET-path request (e.g. GET /status) carries no Method() —
// parseRequest leaves it empty and records the path instead — so it falls
// through to the path branch below, where it is recognised by trimming the
// leading slash and matching cometBFTMethods directly (no template catalogue
// needed: these paths have no variable segments).
func (p *Plugin) NormalizeMethod(payload domain.Payload) string {
	if m := payload.Method(); m != "" {
		if cometBFTMethods[m] {
			return m
		}
		return qos.MethodOther
	}
	path := payload.Path()
	if path == "" {
		return ""
	}
	tpl := templatePath(path)
	if knownRESTTemplates[tpl] {
		return tpl
	}
	if cometBFTPath := strings.TrimPrefix(tpl, "/"); cometBFTMethods[cometBFTPath] {
		return cometBFTPath // GET /status is the same method as {"method":"status"}
	}
	return qos.MethodOther
}
