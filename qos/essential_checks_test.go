package qos_test

import (
	"log/slog"
	"testing"

	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/cosmos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/qos/solana"
)

// Every plugin that tracks block height must mark the check carrying it
// essential, or traffic-informed probing can skip away the only source of a
// fact client traffic does not supply.
//
// The health-check executor's traffic threshold guarantees how MANY
// observations arrive, not what is in them. A plugin reads a height out of one
// specific method — eth_blockNumber for EVM, the CometBFT status response, a
// Solana getEpochInfo — and a client sends whatever it wants. A service under
// heavy eth_call traffic clears the gate by orders of magnitude while teaching
// the block consensus nothing, which is exactly the state the mainnet canary
// reached on 2026-09-03 with arb-one at 100% skip.
//
// This test is in an external package so it can see the plugins the qos
// package cannot import.
func TestPlugins_MarkTheirBlockHeightCheckEssential(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	plugins := map[string]qos.HealthChecker{
		"evm":    evm.NewPlugin(logger, evm.Config{}),
		"cosmos": cosmos.NewPlugin(logger, cosmos.Config{}),
		"solana": solana.NewPlugin(logger, 0),
	}

	for name, plugin := range plugins {
		t.Run(name, func(t *testing.T) {
			checks := plugin.HealthChecks()
			if len(checks) == 0 {
				t.Fatal("no health checks declared")
			}
			names := make([]string, 0, len(checks))
			essential := 0
			for _, c := range checks {
				names = append(names, c.Name)
				if c.Essential {
					essential++
				}
			}
			if essential == 0 {
				t.Errorf("no essential check among %v: traffic-informed probing could skip every "+
					"probe for this service, and client traffic does not carry what these checks ask for", names)
			}
			if essential == len(checks) && len(checks) > 1 {
				t.Errorf("every check of %v is essential, so nothing can ever be skipped; "+
					"mark the minimum, since an essential check is a paid relay every cycle", names)
			}
		})
	}
}
