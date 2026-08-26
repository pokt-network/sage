package evm

import (
	"sort"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

func TestNormalizeMethod(t *testing.T) {
	p := NewPlugin(nil, Config{})
	cases := []struct{ method, want string }{
		{"eth_getLogs", "eth_getLogs"},
		{"eth_call", "eth_call"},
		{"debug_traceTransaction", "debug_traceTransaction"},
		{"eth_definitelyNotAMethod", qos.MethodOther},
		{"", ""},
	}
	for _, tc := range cases {
		got := p.NormalizeMethod(domain.NewPayload([]byte(`{}`), domain.RPCTypeJSONRPC, tc.method))
		if got != tc.want {
			t.Errorf("NormalizeMethod(%q) = %q, want %q", tc.method, got, tc.want)
		}
	}
}

// Every method the plugin reasons about elsewhere must be in the catalogue,
// or a coalescable/archival method would normalise to _other and share one
// block with every unknown method.
func TestKnownMethods_CoverOtherLists(t *testing.T) {
	for m := range coalescableMethods {
		if !knownMethods[m] {
			t.Errorf("coalescable %q missing from knownMethods", m)
		}
	}
	for m := range methodsWithBlockParam {
		if !knownMethods[m] {
			t.Errorf("block-param %q missing from knownMethods", m)
		}
	}
}

// Golden: the catalogue is a label value set. Growth must be a diff someone
// reads, not a runtime surprise.
func TestKnownMethods_Golden(t *testing.T) {
	names := make([]string, 0, len(knownMethods))
	for m := range knownMethods {
		names = append(names, m)
	}
	sort.Strings(names)
	got := strings.Join(names, "\n")
	if got != knownMethodsGolden {
		t.Fatalf("knownMethods changed; update knownMethodsGolden in this test if intended.\n--- got ---\n%s", got)
	}
}

const knownMethodsGolden = `debug_getRawReceipts
debug_traceBlockByHash
debug_traceBlockByNumber
debug_traceCall
debug_traceTransaction
eth_blobBaseFee
eth_blockNumber
eth_call
eth_chainId
eth_createAccessList
eth_estimateGas
eth_feeHistory
eth_gasPrice
eth_getBalance
eth_getBlockByHash
eth_getBlockByNumber
eth_getBlockReceipts
eth_getBlockTransactionCountByHash
eth_getBlockTransactionCountByNumber
eth_getCode
eth_getFilterChanges
eth_getFilterLogs
eth_getLogs
eth_getProof
eth_getStorageAt
eth_getTransactionByBlockHashAndIndex
eth_getTransactionByBlockNumberAndIndex
eth_getTransactionByHash
eth_getTransactionCount
eth_getTransactionReceipt
eth_getUncleByBlockHashAndIndex
eth_getUncleByBlockNumberAndIndex
eth_getUncleCountByBlockHash
eth_getUncleCountByBlockNumber
eth_maxPriorityFeePerGas
eth_newBlockFilter
eth_newFilter
eth_sendRawTransaction
eth_subscribe
eth_syncing
eth_uninstallFilter
eth_unsubscribe
net_listening
net_peerCount
net_version
trace_block
trace_call
trace_filter
trace_replayBlockTransactions
trace_replayTransaction
trace_transaction
web3_clientVersion
web3_sha3`
