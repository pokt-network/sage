package solana

import (
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// knownMethods is the Solana method catalogue; see qos.MethodNormalizer.
// coalescableMethods is a subset, checked by test.
var knownMethods = map[string]bool{
	"getAccountInfo": true, "getBalance": true, "getBlock": true, "getBlockHeight": true,
	"getBlockProduction": true, "getBlockCommitment": true, "getBlocks": true, "getBlocksWithLimit": true,
	"getBlockTime": true, "getClusterNodes": true, "getEpochInfo": true, "getEpochSchedule": true,
	"getFeeForMessage": true, "getFirstAvailableBlock": true, "getGenesisHash": true, "getHealth": true,
	"getHighestSnapshotSlot": true, "getIdentity": true, "getInflationGovernor": true, "getInflationRate": true,
	"getInflationReward": true, "getLargestAccounts": true, "getLatestBlockhash": true, "getLeaderSchedule": true,
	"getMaxRetransmitSlot": true, "getMaxShredInsertSlot": true, "getMinimumBalanceForRentExemption": true,
	"getMultipleAccounts": true, "getProgramAccounts": true, "getRecentBlockhash": true,
	"getRecentPerformanceSamples": true, "getRecentPrioritizationFees": true, "getSignatureStatuses": true,
	"getSignaturesForAddress": true, "getSlot": true, "getSlotLeader": true, "getSlotLeaders": true,
	"getStakeMinimumDelegation": true, "getSupply": true, "getTokenAccountBalance": true,
	"getTokenAccountsByDelegate": true, "getTokenAccountsByOwner": true, "getTokenLargestAccounts": true,
	"getTokenSupply": true, "getTransaction": true, "getTransactionCount": true, "getVersion": true,
	"getVoteAccounts": true, "isBlockhashValid": true, "minimumLedgerSlot": true, "requestAirdrop": true,
	"sendTransaction": true, "simulateTransaction": true,
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
