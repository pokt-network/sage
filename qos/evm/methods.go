package evm

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// knownMethods is the EVM method catalogue: every JSON-RPC method the plugin
// will name in method-aware state and metric labels. A method not listed
// normalises to qos.MethodOther. The other per-method sets in this package
// (coalescableMethods, methodsWithBlockParam) are subsets, checked by test.
var knownMethods = map[string]bool{
	// eth namespace — standard read set
	"eth_blockNumber": true, "eth_chainId": true, "eth_gasPrice": true,
	"eth_maxPriorityFeePerGas": true, "eth_feeHistory": true, "eth_syncing": true,
	"eth_getBalance": true, "eth_getCode": true, "eth_getStorageAt": true,
	"eth_getTransactionCount": true, "eth_call": true, "eth_estimateGas": true,
	"eth_getBlockByNumber": true, "eth_getBlockByHash": true,
	"eth_getBlockTransactionCountByNumber": true, "eth_getBlockTransactionCountByHash": true,
	"eth_getUncleCountByBlockNumber": true, "eth_getUncleCountByBlockHash": true,
	"eth_getUncleByBlockNumberAndIndex": true, "eth_getUncleByBlockHashAndIndex": true,
	"eth_getTransactionByHash": true, "eth_getTransactionByBlockNumberAndIndex": true,
	"eth_getTransactionByBlockHashAndIndex": true, "eth_getTransactionReceipt": true,
	"eth_getBlockReceipts": true, "eth_getLogs": true, "eth_getProof": true,
	"eth_sendRawTransaction": true, "eth_createAccessList": true, "eth_blobBaseFee": true,
	"eth_newFilter": true, "eth_newBlockFilter": true, "eth_getFilterChanges": true,
	"eth_getFilterLogs": true, "eth_uninstallFilter": true, "eth_subscribe": true, "eth_unsubscribe": true,
	// net / web3
	"net_version": true, "net_listening": true, "net_peerCount": true,
	"web3_clientVersion": true, "web3_sha3": true,
	// debug / trace — the namespaces most often absent on a host
	"debug_traceTransaction": true, "debug_traceBlockByNumber": true, "debug_traceBlockByHash": true,
	"debug_traceCall": true, "debug_getRawReceipts": true,
	"trace_block": true, "trace_transaction": true, "trace_call": true, "trace_filter": true,
	"trace_replayTransaction": true, "trace_replayBlockTransactions": true,
}

// NormalizeMethod implements qos.MethodNormalizer.
func (p *Plugin) NormalizeMethod(payload domain.Payload) string {
	m := payload.Method()
	if m == "" {
		return ""
	}
	if knownMethods[m] {
		return m
	}
	return qos.MethodOther
}
