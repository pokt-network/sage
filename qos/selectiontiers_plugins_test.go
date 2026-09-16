package qos_test

import (
	"testing"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/cosmos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/qos/jsonheight"
	"github.com/pokt-network/sage/qos/solana"
)

// heightPlugin is what the tier count needs from a plugin.
type heightPlugin interface {
	qos.Plugin
	qos.SelectionTierReporter
	UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64)
}

// Every plugin that filters on height reports the tier each selection
// settled on: 1 for an endpoint at the head, 3 for one far behind it with
// nothing better to pick. Wire only installs the counter on plugins that
// satisfy qos.SelectionTierReporter, so a plugin missing here would have no
// sage_qos_selection_tier_total series at all.
func TestHeightPluginsReportSelectionTiers(t *testing.T) {
	for name, p := range map[string]heightPlugin{
		"evm":        evm.NewPlugin(nil, evm.Config{SyncAllowance: 10}),
		"cosmos":     cosmos.NewPlugin(nil, cosmos.Config{SyncAllowance: 10}),
		"solana":     solana.NewPlugin(nil, 10),
		"jsonheight": jsonheight.NewPlugin(nil, jsonheight.EthBeacon, 10),
	} {
		t.Run(name, func(t *testing.T) {
			var tiers []int
			p.SetSelectionTierRecorder(func(tier int) { tiers = append(tiers, tier) })
			head := domain.EndpointAddr("pokt1a-https://head.example.com")
			behind := domain.EndpointAddr("pokt1b-https://behind.example.com")
			p.UpdateBlockHeight(head, 1000)
			p.UpdateBlockHeight(behind, 700)

			if _, err := p.SelectEndpoints(domain.EndpointAddrList{head}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := p.SelectEndpoints(domain.EndpointAddrList{behind}, nil); err != nil {
				t.Fatal(err)
			}
			if len(tiers) != 2 || tiers[0] != 1 || tiers[1] != 3 {
				t.Errorf("tiers = %v, want [1 3]", tiers)
			}
		})
	}
}
