// Package cosmos implements a QoS plugin for Cosmos SDK chains.
//
// Cosmos chains expose three RPC interfaces:
//   - REST (gRPC-gateway): GET/POST to paths like /cosmos/base/tendermint/v1beta1/blocks/latest
//   - CometBFT RPC: GET to paths like /status, /block, or POST with JSON-RPC method names
//   - JSON-RPC: standard JSON-RPC 2.0 over POST (rare, chain-specific)
//
// Block height is sourced from CometBFT /status responses (sync_info.latest_block_height),
// and, behind featureflag.FlagCosmosEVMHeight, from the EVM face's eth_blockNumber.
package cosmos

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
)

const (
	// staleSweepInterval is how often the endpoint store is swept for stale entries.
	staleSweepInterval = 5 * time.Minute

	// endpointStaleTTL is how long an endpoint can go unseen before being swept.
	endpointStaleTTL = 10 * time.Minute
)

// cosmosEndpoint holds per-endpoint state tracked by the Cosmos plugin.
type cosmosEndpoint struct {
	BlockHeight uint64
	RPCType     domain.RPCType
	ChainID     string
}

// Plugin is the Cosmos QoS plugin. It implements:
//   - qos.Plugin
//   - qos.BlockHeightTracker
//   - qos.HealthChecker
//   - qos.DataExtractor
//   - qos.ChainViewer
//   - qos.MethodNormalizer
//   - qos.ImmutableClassifier
//   - qos.StateResetter
//   - qos.SubscriptionClassifier
//   - qos.WebSocketProber
type Plugin struct {
	qos.SelectionTiers
	qos.HeightTracking

	logger            *slog.Logger
	supportedRPCTypes []domain.RPCType
	expectedChainID   string
	evmHeight         func() bool
	stateCanary       func() bool

	store *qos.EndpointStore[cosmosEndpoint]
	// pruned remembers, per host, the lowest height the node holds; see pruned.go.
	pruned *prunedMemory
}

// Config carries the per-service settings a Cosmos plugin needs.
//
// Mirrors evm.Config, and for the same reason: how to run a check belongs in
// code, the values it asserts belong in config. Zero values are sensible
// defaults, per CLAUDE.md.
type Config struct {
	// SyncAllowance is how many blocks behind the perceived chain head an
	// endpoint may fall and still serve traffic.
	SyncAllowance uint64

	// SupportedRPCTypes are the RPC types this service instance accepts. Empty
	// means all three (REST, CometBFT, JSON-RPC).
	SupportedRPCTypes []domain.RPCType

	// ExpectedChainID is the network name this service must serve, as CometBFT
	// /status reports it under node_info.network (e.g. "cosmoshub-4"). Empty
	// disables the assertion.
	ExpectedChainID string

	// StateCanary reports, when asked, whether the state canary runs
	// (featureflag.FlagStateCanary): the REST latest-block canary always, and
	// the EVM canary (qos/evm/canary.go) too where EVMHeight holds. Nil means
	// never.
	StateCanary func() bool

	// EVMHeight reports, at the moment it is asked, whether this chain's EVM
	// face reports the Cosmos height (featureflag.FlagCosmosEVMHeight). When
	// it does, the plugin probes eth_blockNumber on json_rpc stakes and reads
	// the height out of eth_blockNumber answers. Nil means never.
	EVMHeight func() bool
}

// Validate reports whether the config is usable, and is called at wire time.
//
// It catches far less than evm.Config.Validate, and the asymmetry is worth
// being explicit about: a CometBFT network is an opaque name, so there is no
// format to check against. "cosmoshub-5" is indistinguishable from
// "cosmoshub-4" to anything but the chain itself, which means a typo cannot be
// caught here — it will surface as every endpoint of the service being ejected,
// with the expected and reported names side by side in the warning.
//
// Surrounding whitespace is the one mistake that is both catchable and
// invisible in YAML, so it is rejected rather than trimmed: trimming would be a
// guess about intent, and the whole point of this field is to mean exactly what
// it says.
func (c Config) Validate() error {
	if c.ExpectedChainID == "" {
		return nil
	}
	if strings.TrimSpace(c.ExpectedChainID) != c.ExpectedChainID {
		return fmt.Errorf("expected_chain_id: %q has surrounding whitespace", c.ExpectedChainID)
	}
	return nil
}

// Compile-time interface checks.
var (
	_ qos.Plugin             = (*Plugin)(nil)
	_ qos.BlockHeightTracker = (*Plugin)(nil)
	_ qos.HealthChecker      = (*Plugin)(nil)
	_ qos.DataExtractor      = (*Plugin)(nil)
	_ qos.ChainViewer        = (*Plugin)(nil)
	_ qos.HeightObserver     = (*Plugin)(nil)
	_ qos.StateResetter      = (*Plugin)(nil)
	_ qos.RPCTypeClassifier  = (*Plugin)(nil)
)

// NewPlugin creates a Cosmos QoS plugin for a single service.
func NewPlugin(logger *slog.Logger, cfg Config) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	supportedRPCTypes := cfg.SupportedRPCTypes
	if len(supportedRPCTypes) == 0 {
		supportedRPCTypes = []domain.RPCType{
			domain.RPCTypeREST,
			domain.RPCTypeCometBFT,
			domain.RPCTypeJSONRPC,
		}
	}
	p := &Plugin{
		logger:            logger,
		supportedRPCTypes: supportedRPCTypes,
		expectedChainID:   cfg.ExpectedChainID,
		evmHeight:         cfg.EVMHeight,
		stateCanary:       cfg.StateCanary,
		store:             qos.NewEndpointStore[cosmosEndpoint](logger),
		pruned:            newPrunedMemory(),
	}
	p.Consensus = qos.NewBlockConsensus(logger, cfg.SyncAllowance)
	p.SetSyncAllowance(cfg.SyncAllowance)
	return p
}

// --- qos.Plugin --- //

// ParseRequest inspects the request and returns a single-element Payload slice.
// The RPC type is auto-detected from the request path and body.
func (p *Plugin) ParseRequest(_ context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	payload, err := parseRequest(req, body, rpcType, p.supportedRPCTypes)
	if err != nil {
		return nil, err
	}

	// Reject RPC types the configured service does not support.
	if !slices.Contains(p.supportedRPCTypes, payload.RPCType()) {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   fmt.Sprintf("cosmos: RPC type %q not supported by this service", payload.RPCType()),
			Retryable: false,
		}
	}

	return []domain.Payload{payload}, nil
}

// ClassifyRPCType implements qos.RPCTypeClassifier: the type a request is
// relayed as, decided by which CometBFT face it addresses and which types the
// service declares. See classifyRPCType.
func (p *Plugin) ClassifyRPCType(req *http.Request, body []byte, detected domain.RPCType) domain.RPCType {
	return classifyRPCType(req, body, detected, p.supportedRPCTypes)
}

// SelectEndpoints filters the supplied endpoint list by:
//  1. Block height — endpoints must be within syncAllowance of the perceived head.
//  2. RPC type compatibility — endpoints must support the requested RPC type.
//
// It uses the tiered degradation logic in qos.SelectWithKnownHeights.
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, payloads []domain.Payload) (domain.EndpointAddrList, error) {
	if len(endpoints) == 0 {
		return nil, nil
	}

	perceived := p.Consensus.PerceivedBlock()

	// Determine requested RPC type from the first payload (if any).
	var requestedRPCType domain.RPCType
	if len(payloads) > 0 {
		requestedRPCType = payloads[0].RPCType()
	}

	// Block height filter factory (parameterised by sync allowance multiplier).
	getHeight := qos.HeightGetter(p.store, func(ep cosmosEndpoint) uint64 { return ep.BlockHeight }, p.Consensus.Projection())

	makeBlockFilter := func(allowance uint64) qos.FilterFunc {
		return qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, allowance))
	}

	// RPC type filter — only applied when we have an explicit type to match.
	var rpcTypeFilter qos.FilterFunc
	if requestedRPCType != "" && requestedRPCType != domain.RPCTypeUnknown {
		rpcTypeFilter = func(addr domain.EndpointAddr) error {
			ep, ok := p.store.Get(addr)
			if !ok {
				// Unknown endpoint — let through.
				return nil
			}
			if ep.RPCType != "" && ep.RPCType != requestedRPCType {
				return &domain.RelayError{
					Kind:      domain.ErrCapability,
					Message:   fmt.Sprintf("cosmos: endpoint RPC type %q does not match requested %q", ep.RPCType, requestedRPCType),
					Retryable: true,
				}
			}
			return nil
		}
	}

	baseFilters := []qos.FilterFunc{makeBlockFilter(p.SyncAllowance())}
	relaxedFilters := []qos.FilterFunc{makeBlockFilter(p.SyncAllowance() * 2)}
	nonBlockFilters := []qos.FilterFunc{}
	if rpcTypeFilter != nil {
		baseFilters = append(baseFilters, rpcTypeFilter)
		relaxedFilters = append(relaxedFilters, rpcTypeFilter)
		nonBlockFilters = append(nonBlockFilters, rpcTypeFilter)
	}

	// Height-aware routing (pruned.go): a request that names a specific
	// height skips hosts known to have pruned below it. Applied in every
	// tier, like the EVM plugin's archival filter: a pruned host is no better
	// a choice when the pool is degraded. If it empties every tier the
	// selector falls back to the full list and the query is sent once, so
	// the client gets the node's answer and nothing is retried.
	heightFiltered := false
	if len(payloads) > 0 {
		if height, ok := requestedHeight(payloads[0]); ok {
			heightFiltered = true
			heightFilter := func(addr domain.EndpointAddr) error {
				lowest, known := p.pruned.lowest(addr.Domain())
				if !known || lowest <= height {
					return nil
				}
				return &domain.RelayError{
					Kind:      domain.ErrCapability,
					Message:   fmt.Sprintf("cosmos: host pruned below height %d (lowest %d)", height, lowest),
					Retryable: true,
				}
			}
			baseFilters = append(baseFilters, heightFilter)
			relaxedFilters = append(relaxedFilters, heightFilter)
			nonBlockFilters = append(nonBlockFilters, heightFilter)
		}
	}

	ranker := qos.LeastStaleFallback(getHeight, perceived)
	result := qos.SelectWithKnownHeights(endpoints, getHeight, baseFilters, relaxedFilters, nonBlockFilters, ranker)
	p.ReportTier(result.Tier)

	if result.Degraded {
		// A pool with nothing that holds the requested height is the
		// ordinary outcome of an archival query on a pruned face, once per
		// such query; it is not the pool being unhealthy. Debug for that
		// case, Warn for the rest.
		if heightFiltered && result.Tier == 3 {
			p.logger.Debug("cosmos: no endpoint holds the requested height; sending once for the node's answer",
				"endpoint_count", len(result.Endpoints),
			)
		} else {
			p.logger.Warn("cosmos: endpoint selection degraded",
				"tier", result.Tier,
				"endpoint_count", len(result.Endpoints),
			)
		}
	}

	return result.Endpoints, nil
}

// --- qos.BlockHeightTracker --- //

// UpdateBlockHeight records a new block height observation for an endpoint and
// feeds it into the consensus computation.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	p.store.ObserveHeight(endpoint, func(ep *cosmosEndpoint) {
		ep.BlockHeight = height
	})
	p.Consensus.AddObservation(endpoint, height)
}

// StartSync starts background goroutines for the plugin (stale endpoint sweeping).
func (p *Plugin) StartSync(ctx context.Context) {
	safego.GoCtx(ctx, p.logger, "qos.cosmos.sweep", p.sweepLoop)
}

func (p *Plugin) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(staleSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed := p.store.SweepStale(endpointStaleTTL)
			if len(removed) > 0 {
				p.logger.Info("cosmos: swept stale endpoints", "count", len(removed))
			}
		}
	}
}

// --- qos.HealthChecker --- //

// HealthChecks returns health check payloads for the given endpoint.
// The Cosmos plugin always issues a CometBFT /status check to obtain block height.
// Behind featureflag.FlagCosmosEVMHeight it adds an eth_blockNumber check on
// the EVM face, for chains whose json_rpc stakes cannot answer /status.
//
// Always the CometBFT HTTP GET /status. A supplier staked for json_rpc only
// still receives it, through the service's rpc_type_fallbacks mapping
// (config.ServiceConfig): relay miners serve both surfaces from one port, so
// the GET works there. There used to be a JSON-RPC variant here selected on a
// store field nothing wrote, so it never ran; the fallback is the live
// version of that idea.
func (p *Plugin) HealthChecks() []qos.HealthCheck {
	checks := []qos.HealthCheck{
		{
			Name:    "comet_bft_status",
			Payload: cometBFTStatusPayload(),
			// The status response is where both the height and the chain id
			// come from; an abci_query from a client carries neither.
			Essential: true,
		},
		{
			// The REST face is a different backend behind the relay miner,
			// and a score is keyed per RPC type, so the /status probe says
			// nothing about it. Without this a REST key knocked to 0 by an
			// outage had no way back but traffic it no longer received
			// (mainnet 2026-09-15: osmosis, juno and a dozen other Cosmos REST
			// faces carried traffic with no probe at all). It runs only
			// against REST-staked suppliers, and is graded on its status: the
			// answer carries no height, so it is not Essential.
			Name:    "rest_syncing",
			Payload: restSyncingPayload(),
		},
	}
	// The EVM face, where its block number is the Cosmos height: on a chain
	// whose json_rpc stakes are EVM nodes, /status reaches none of them.
	if p.evmHeights() {
		checks = append(checks, qos.HealthCheck{
			Name:    "evm_block_number",
			Payload: evmBlockNumberPayload(),
			// Not Essential: on kava half the json_rpc stakes front a
			// CometBFT node with no EVM, and their "method not found" is an
			// answer about the host, not a failure (ExtractData). Essential
			// would grade that empty answer as a failed probe.
		})
	}
	if p.stateCanary != nil && p.stateCanary() {
		checks = append(checks, qos.HealthCheck{Name: restCanaryName, Payload: restHeadCanary(), GradesHead: true})
		if p.evmHeights() {
			checks = append(checks, evm.CanaryCheck(time.Now()))
		}
	}
	return checks
}

// HeadLag implements qos.HeadLagReader.
//
// The EVM face of a chain whose EVM block number is the Cosmos height
// (evmHeights) is read as the EVM plugin reads it: eth_blockNumber,
// eth_getBlockByNumber("latest") and the EVM state canary. On mainnet sei
// (2026-10-01) one owner carried 90% of first attempts on that face and
// nothing measured how old its answers were.
//
// Every other answer that names the newest block (CometBFT status, block with
// no height, the REST latest-block route, the REST canary) is graded by that
// block's time against the clock (headTime). The same owner held a third or
// more of first attempts on several Cosmos chains, unmeasured.
func (p *Plugin) HeadLag(payload domain.Payload, response []byte, at time.Time) (lag uint64, stale, ok bool) {
	if p.evmHeights() && payload.RPCType() == domain.RPCTypeJSONRPC {
		if lag, stale, ok := evm.HeadLag(payload, response, at, p.Consensus); ok {
			return lag, stale, ok
		}
	}
	ts, ok := headTime(payload, response)
	if !ok {
		return 0, false, false
	}
	return p.Consensus.StateLag(ts, at)
}

var _ qos.HeadLagReader = (*Plugin)(nil)

// --- qos.DataExtractor --- //

// ExtractData parses a relay response and returns structured data: the block
// height, and the chain identifier when the response carries one.
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*qos.ExtractedData, error) {
	if len(response) == 0 {
		return nil, fmt.Errorf("cosmos: empty response from %s", endpoint)
	}
	// Every Cosmos face answers JSON (CometBFT, the REST gateway, the EVM
	// face). A body that is not, a lone "OK" or newline from a default
	// backend behind a misrouted vhost, answers nothing: passed, it scored a
	// REST face's probe a success for answering no question.
	if !gjson.ValidBytes(response) {
		return nil, fmt.Errorf("cosmos: response from %s is not JSON", endpoint)
	}

	// An EVM face's eth_blockNumber answer, on a chain where that number is
	// the Cosmos height (featureflag.FlagCosmosEVMHeight).
	if p.evmHeights() && gjson.GetBytes(request, "method").String() == "eth_blockNumber" {
		// A json_rpc stake fronting a CometBFT node has no EVM face and says
		// so (-32601). That is what it serves, not a fault: no height, no
		// error. On mainnet kava (2026-10-01) two operators' json_rpc stakes
		// answer this way and carry its CometBFT JSON-RPC traffic.
		if gjson.GetBytes(response, "error.code").Int() == -32601 {
			return &qos.ExtractedData{}, nil
		}
		height, err := evm.ParseBlockNumber(response)
		if err != nil {
			return nil, fmt.Errorf("cosmos: evm eth_blockNumber from %s: %w", endpoint, err)
		}
		return &qos.ExtractedData{BlockHeight: &height}, nil
	}

	// Chain identity is checked before block height is recorded. An endpoint on
	// the wrong chain reports heights that are real for that chain, so feeding
	// them to consensus would let it skew the very number the height filters
	// compare against.
	// A pruned node's answer names the lowest height it holds; remember it
	// per host so SelectEndpoints stops sending old heights there. The
	// request is a client's, so the probe is free.
	if lowest, ok := prunedLowestHeight(response); ok {
		p.pruned.set(endpoint.Domain(), lowest)
		p.logger.Debug("cosmos: host reports pruned history", "endpoint", endpoint, "lowest_height", lowest)
	}

	chainID, hasChainID := parseChainID(response)
	if hasChainID {
		p.store.Update(endpoint, func(ep *cosmosEndpoint) {
			ep.ChainID = chainID
		})
		if err := p.assertChainID(endpoint, chainID); err != nil {
			return nil, err
		}
	}

	height, err := parseBlockHeight(response)
	if err != nil {
		// Not an error worth surfacing — the response may be for a method that
		// doesn't contain block height (e.g., abci_query).
		p.logger.Debug("cosmos: no block height in response", "endpoint", endpoint, "error", err)
		if hasChainID {
			return &qos.ExtractedData{ChainID: &chainID}, nil
		}
		return &qos.ExtractedData{}, nil
	}

	data := &qos.ExtractedData{BlockHeight: &height}
	if hasChainID {
		data.ChainID = &chainID
	}
	return data, nil
}

// assertChainID checks a reported network name against the service's configured
// chain_id, and reports qos.ErrWrongChain when they disagree.
//
// An exact comparison, deliberately. EVM must compare numerically because
// "0x531" and "0x0531" are the same chain written two ways; a CometBFT network
// is a name with no such freedom, so "cosmoshub-4" and "cosmoshub-04" are
// simply different chains and normalizing between them would invent a
// tolerance the chain itself does not have.
func (p *Plugin) assertChainID(endpoint domain.EndpointAddr, reported string) error {
	if p.expectedChainID == "" || reported == p.expectedChainID {
		return nil
	}

	p.logger.Warn("cosmos: endpoint reported unexpected chain id",
		"endpoint", endpoint,
		"expected_chain_id", p.expectedChainID,
		"reported_chain_id", reported,
	)
	return fmt.Errorf("%w: want %s, got %s", qos.ErrWrongChain, p.expectedChainID, reported)
}

// --- qos.StateResetter ---

// ResetState discards the block consensus and every per-endpoint observation
// (block height, chain ID) this plugin has learned. It is the admin
// chain-state reset: nothing else about the plugin's configuration changes,
// and the next health-check cycle and the next relays repopulate both from
// scratch.
func (p *Plugin) ResetState() {
	p.Consensus.Reset()
	p.store.Clear()
	p.pruned.reset()
}

// evmHeights reports whether this chain's EVM face reports the Cosmos height.
func (p *Plugin) evmHeights() bool { return p.evmHeight != nil && p.evmHeight() }

// AllStale reports whether every endpoint in eps is known to sit below the
// relaxed height bound (qos.StaleChecker).
func (p *Plugin) AllStale(eps domain.EndpointAddrList) bool {
	return p.AllStaleBy(eps, qos.HeightGetter(p.store, func(ep cosmosEndpoint) uint64 { return ep.BlockHeight }, p.Consensus.Projection()))
}

var _ qos.StaleChecker = (*Plugin)(nil)
