package main

import (
	"fmt"
	"slices"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/protocol/shannon"
	"github.com/pokt-network/sage/qos/cosmos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/relay"
	"github.com/pokt-network/sage/relay/middleware"
	"github.com/pokt-network/sage/reputation"
)

// validateConfig runs the checks that decide whether SAGE can run a config at
// all — the ones config.LoadFromFile cannot make, because they belong to the
// packages that consume the values rather than to the parser.
//
// It exists as one function because it has two callers with the same question.
// Build asks it at startup, where the answer is "refuse to start". Reload asks
// it before applying anything, where the answer is "refuse to change" — and a
// reload that accepted a file the binary would not boot with would leave the
// gateway one restart away from being down, with nothing having said so.
//
// Whole-config and fail-fast: the first problem is returned, because a config
// with two problems is fixed one at a time either way.
//
// Two of boot's checks are deliberately NOT here, because they cannot be made
// from the config alone: a middleware name the registry does not know (that
// needs the built registry, so BuildChain keeps it) and the Shannon signing
// keys (parsed by shannon.New against a live full node). Both belong to
// sections a reload reports as needing a restart anyway.
func validateConfig(cfg *config.Config) error {
	// A misspelled granularity must not fall through to the default: it would
	// silently change what scores are attached to, and nothing downstream could
	// tell the difference until an incident.
	keyGranularity := cfg.Gateway.Reputation.KeyGranularity
	if !reputation.ValidKeyGranularity(keyGranularity) {
		return fmt.Errorf(
			"reputation_config.key_granularity %q is not recognised (want one of: %s, %s, %s, %s)",
			keyGranularity,
			reputation.KeyPerURL, reputation.KeyPerEndpoint,
			reputation.KeyPerDomain, reputation.KeyPerSupplier,
		)
	}

	// Chain semantics belong to the QoS plugin, so each plugin validates its
	// own per-service settings (chain_id format, supported RPC types).
	for _, svc := range cfg.Gateway.AllServices() {
		if err := validateServiceQoS(svc); err != nil {
			return err
		}
	}

	// blocked_domains is compiled inside the Shannon protocol, where endpoints
	// are handed out. Validate it here too, so a malformed entry fails under
	// every backend rather than only the one that reads it.
	if err := shannon.ValidateBlockedDomains(cfg.Gateway.BlockedDomains); err != nil {
		return err
	}

	if _, err := middleware.ParseTrustedProxies(cfg.Router.TrustedProxies); err != nil {
		return fmt.Errorf("router_config.trusted_proxies: %w", err)
	}

	// The ordering invariants, not the registration check — that one needs the
	// registry and stays in Build, which runs it immediately afterwards. The
	// two are in the same relative order they had inside BuildChain.
	order := cfg.Gateway.EffectiveMiddlewareChain()
	if len(order) == 0 {
		order = relay.DefaultChainOrder()
	}
	if err := relay.ValidateChainOrder(order); err != nil {
		return fmt.Errorf("build middleware chain: %w", err)
	}

	// send_relay is the only middleware that actually relays. Without it the
	// chain parses, selects an endpoint, and hands the request to the
	// registry's terminal, which errors — a gateway that answers nothing.
	//
	// A pure statement about the configured order, so it is checked here
	// rather than after BuildChain: a reload has to be able to refuse the same
	// file, and the ordering rules above have already run, which is what kept
	// this check second when it lived in Build.
	if !slices.Contains(order, relay.MWSendRelay) {
		return fmt.Errorf("build middleware chain: %q is missing from the configured chain; "+
			"without it no request is ever relayed", relay.MWSendRelay)
	}

	return nil
}

// validateServiceQoS asks the service's QoS plugin whether its config is one
// it can serve. Plugins with nothing to validate (solana, the passthrough) say
// nothing.
func validateServiceQoS(svc config.ServiceConfig) error {
	var err error
	switch domain.ServiceType(svc.Type) {
	case domain.ServiceTypeEVM:
		err = evmConfigFor(svc).Validate()
	case domain.ServiceTypeCosmos:
		err = cosmosConfigFor(svc).Validate()
	}
	if err != nil {
		return fmt.Errorf("service %q: %w", svc.ID, err)
	}
	return nil
}

// unscoredChainWarning is what warnIfUnscoredChain reports. It names the fix
// as well as the fault: an operator reading it at 3am should not have to find
// out from ARCHITECTURE.md where in the chain score belongs.
const unscoredChainWarning = `middleware_chain names observe but not score: ` +
	`with feature flag scoring_v2 on, nothing records reputation signals — ` +
	`add "score" after "select_endpoint" or disable scoring_v2`

// warnIfUnscoredChain reports a pinned chain that would record no reputation
// at all, and the warning to log for it.
//
// Under scoring_v2, observe deliberately stops recording signals and score
// records them instead. A config written before score existed — a PATH config,
// or a SAGE one that pins gateway_config.middleware_chain — names observe and
// not score, so with the flag on (the default) nothing records anything: every
// endpoint keeps its starting score forever, selection degrades to round-robin
// over a pool that never learns, and no error surfaces because both
// middlewares are behaving exactly as designed.
//
// A warning rather than a startup error: dropping a middleware from the chain
// is a legitimate thing for a config to do, and refusing to boot on it would
// make an upgrade fail closed for a gateway that was running fine a minute
// earlier. A chain that names NEITHER observe nor score is not warned about —
// that reads as a deliberately minimal chain, not as a missed migration.
func warnIfUnscoredChain(names []string) (string, bool) {
	if !slices.Contains(names, relay.MWObserve) || slices.Contains(names, relay.MWScore) {
		return "", false
	}
	return unscoredChainWarning, true
}

// evmConfigFor builds the EVM plugin's config for a service. Shared with Build
// so the settings validated here are the settings the plugin is constructed
// with, rather than two lists that can drift.
func evmConfigFor(svc config.ServiceConfig) evm.Config {
	return evm.Config{
		SyncAllowance:   svc.SyncAllowance,
		ExpectedChainID: svc.ChainID,
	}
}

// cosmosConfigFor builds the Cosmos plugin's config for a service. See
// evmConfigFor.
func cosmosConfigFor(svc config.ServiceConfig) cosmos.Config {
	rpcTypes := make([]domain.RPCType, len(svc.RPCTypes))
	for i, rt := range svc.RPCTypes {
		rpcTypes[i] = domain.RPCType(rt)
	}
	return cosmos.Config{
		SyncAllowance:     svc.SyncAllowance,
		SupportedRPCTypes: rpcTypes,
		ExpectedChainID:   svc.ChainID,
	}
}
