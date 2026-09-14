package cosmos

import (
	"sort"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

func TestTemplatePath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/cosmos/tx/v1beta1/txs/block/12345", "/cosmos/tx/v1beta1/txs/block/:var"},
		{"/cosmos/bank/v1beta1/balances/cosmos1abcdefghijklmnopqrstuvwxyz0123456789", "/cosmos/bank/v1beta1/balances/:var"},
		{"/cosmos/tx/v1beta1/txs/0xDEADBEEF", "/cosmos/tx/v1beta1/txs/:var"},
		{"/cosmos/tx/v1beta1/txs/A1B2C3D4E5F60718293A4B5C6D7E8F90A1B2C3D4E5F60718293A4B5C6D7E8F90", "/cosmos/tx/v1beta1/txs/:var"},
		{"/cosmos/base/tendermint/v1beta1/blocks/latest", "/cosmos/base/tendermint/v1beta1/blocks/latest"},
		{"/status?height=5", "/status"},
	}
	for _, tc := range cases {
		if got := templatePath(tc.in); got != tc.want {
			t.Errorf("templatePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A -32601 marks the whole EVM family only for a method every EVM node
// serves. sei's EVM lacks net_listening, and when any EVM method counted
// that one refusal blocked every EVM method on healthy hosts (2026-09-14).
func TestMethodFamily_OnlyForMethodsEveryEVMServes(t *testing.T) {
	p := NewPlugin(nil, Config{})
	for _, m := range []string{"eth_blockNumber", "eth_chainId"} {
		if fam := p.MethodFamily(m); len(fam) == 0 {
			t.Errorf("MethodFamily(%q) is empty: a node refusing it has no EVM", m)
		}
	}
	for _, m := range []string{"net_listening", "web3_clientVersion", "debug_traceTransaction", "eth_getLogs", "status", "/cosmos/bank/v1beta1/params"} {
		if fam := p.MethodFamily(m); len(fam) != 0 {
			t.Errorf("MethodFamily(%q) = %d methods, want none: refusing it says nothing about the rest", m, len(fam))
		}
	}
}

func TestNormalizeMethod(t *testing.T) {
	p := NewPlugin(nil, Config{})
	jsonrpc := func(m string) domain.Payload { return domain.NewPayload([]byte(`{}`), domain.RPCTypeCometBFT, m) }
	rest := func(path string) domain.Payload {
		return domain.NewPayload(nil, domain.RPCTypeREST, "").WithHTTP(path, "GET")
	}
	cases := []struct {
		name string
		p    domain.Payload
		want string
	}{
		{"cometbft method", jsonrpc("block_results"), "block_results"},
		{"unknown cometbft method", jsonrpc("nope"), qos.MethodOther},
		// The EVM face of kava or sei: catalogued EVM methods are named so
		// per-host method blocks can learn which json_rpc stakers serve EVM.
		{"evm method on the json_rpc face", domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_blockNumber"), "eth_blockNumber"},
		{"evm debug method", domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "debug_traceTransaction"), "debug_traceTransaction"},
		{"uncatalogued evm-looking method", domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, "eth_madeUp"), qos.MethodOther},
		{"catalogued rest template", rest("/cosmos/tx/v1beta1/txs/block/77"), "/cosmos/tx/v1beta1/txs/block/:var"},
		{"unlisted rest path", rest("/osmosis/gamm/v1beta1/pools/1"), qos.MethodOther},
		{"no method, no path", domain.NewPayload(nil, domain.RPCTypeREST, ""), ""},
		// parseRequest sets Method() to "" for a CometBFT GET-path request
		// like /status (see parser.go's isCometBFTPath branch) and carries the
		// path via WithHTTP instead, so NormalizeMethod must derive the method
		// name from the path itself.
		{"cometbft GET path", domain.NewPayload(nil, domain.RPCTypeCometBFT, "").WithHTTP("/status", "GET"), "status"},
	}
	for _, tc := range cases {
		if got := p.NormalizeMethod(tc.p); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Golden: the REST template catalogue is a label value set. Growth must be a
// diff someone reads, not a runtime surprise.
func TestKnownRESTTemplates_Golden(t *testing.T) {
	names := make([]string, 0, len(knownRESTTemplates))
	for tpl := range knownRESTTemplates {
		names = append(names, tpl)
	}
	sort.Strings(names)
	got := strings.Join(names, "\n")
	if got != knownRESTTemplatesGolden {
		t.Fatalf("knownRESTTemplates changed; update knownRESTTemplatesGolden in this test if intended.\n--- got ---\n%s", got)
	}
}

const knownRESTTemplatesGolden = `/cosmos/auth/v1beta1/account_info/:var
/cosmos/auth/v1beta1/accounts/:var
/cosmos/bank/v1beta1/balances/:var
/cosmos/bank/v1beta1/balances/:var/by_denom
/cosmos/bank/v1beta1/supply
/cosmos/base/tendermint/v1beta1/blocks/:var
/cosmos/base/tendermint/v1beta1/blocks/latest
/cosmos/base/tendermint/v1beta1/node_info
/cosmos/base/tendermint/v1beta1/syncing
/cosmos/base/tendermint/v1beta1/validatorsets/:var
/cosmos/base/tendermint/v1beta1/validatorsets/latest
/cosmos/distribution/v1beta1/delegators/:var/rewards
/cosmos/gov/v1/proposals
/cosmos/gov/v1/proposals/:var
/cosmos/gov/v1beta1/proposals
/cosmos/gov/v1beta1/proposals/:var
/cosmos/staking/v1beta1/delegations/:var
/cosmos/staking/v1beta1/validators
/cosmos/staking/v1beta1/validators/:var
/cosmos/tx/v1beta1/simulate
/cosmos/tx/v1beta1/txs
/cosmos/tx/v1beta1/txs/:var
/cosmos/tx/v1beta1/txs/block/:var
/ibc/apps/transfer/v1/denom_traces
/ibc/apps/transfer/v1/denom_traces/:var
/ibc/core/channel/v1/channels
/ibc/core/client/v1/client_states`
