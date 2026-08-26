package solana

import (
	"sort"
	"strings"
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

func TestNormalizeMethod(t *testing.T) {
	p := NewPlugin(nil, 0)
	cases := []struct{ method, want string }{
		{"getProgramAccounts", "getProgramAccounts"},
		{"getSlot", "getSlot"},
		{"getMultipleAccounts", "getMultipleAccounts"},
		{"getSomethingFake", qos.MethodOther},
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
// or a coalescable method would normalise to _other and share one block with
// every unknown method.
func TestKnownMethods_CoverOtherLists(t *testing.T) {
	for m := range coalescableMethods {
		if !knownMethods[m] {
			t.Errorf("coalescable %q missing from knownMethods", m)
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

const knownMethodsGolden = `getAccountInfo
getBalance
getBlock
getBlockCommitment
getBlockHeight
getBlockProduction
getBlockTime
getBlocks
getBlocksWithLimit
getClusterNodes
getEpochInfo
getEpochSchedule
getFeeForMessage
getFirstAvailableBlock
getGenesisHash
getHealth
getHighestSnapshotSlot
getIdentity
getInflationGovernor
getInflationRate
getInflationReward
getLargestAccounts
getLatestBlockhash
getLeaderSchedule
getMaxRetransmitSlot
getMaxShredInsertSlot
getMinimumBalanceForRentExemption
getMultipleAccounts
getProgramAccounts
getRecentBlockhash
getRecentPerformanceSamples
getRecentPrioritizationFees
getSignatureStatuses
getSignaturesForAddress
getSlot
getSlotLeader
getSlotLeaders
getStakeMinimumDelegation
getSupply
getTokenAccountBalance
getTokenAccountsByDelegate
getTokenAccountsByOwner
getTokenLargestAccounts
getTokenSupply
getTransaction
getTransactionCount
getVersion
getVoteAccounts
isBlockhashValid
minimumLedgerSlot
requestAirdrop
sendTransaction
simulateTransaction`
