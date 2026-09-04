// Package cosmos implements a QoS plugin for Cosmos SDK chains.
//
// Cosmos chains expose three RPC interfaces:
//   - REST (gRPC-gateway): GET/POST to paths like /cosmos/base/tendermint/v1beta1/blocks/latest
//   - CometBFT RPC: GET to paths like /status, /block, or POST with JSON-RPC method names
//   - JSON-RPC: standard JSON-RPC 2.0 over POST (rare, chain-specific)
//
// Block height is sourced from CometBFT /status responses (sync_info.latest_block_height).
package cosmos

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/internal/safego"
	"github.com/pokt-network/sage/qos"
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
//   - qos.StateResetter
//   - qos.SubscriptionClassifier
type Plugin struct {
	logger            *slog.Logger
	syncAllowance     uint64
	supportedRPCTypes []domain.RPCType
	expectedChainID   string

	store     *qos.EndpointStore[cosmosEndpoint]
	consensus *qos.BlockConsensus
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
	return &Plugin{
		logger:            logger,
		syncAllowance:     cfg.SyncAllowance,
		supportedRPCTypes: supportedRPCTypes,
		expectedChainID:   cfg.ExpectedChainID,
		store:             qos.NewEndpointStore[cosmosEndpoint](logger),
		consensus:         qos.NewBlockConsensus(logger, cfg.SyncAllowance),
	}
}

// --- qos.Plugin --- //

// ParseRequest inspects the request and returns a single-element Payload slice.
// The RPC type is auto-detected from the request path and body.
func (p *Plugin) ParseRequest(_ context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	payload, err := parseRequest(req, body, rpcType)
	if err != nil {
		return nil, err
	}

	// Reject RPC types the configured service does not support.
	if !isRPCTypeSupported(payload.RPCType(), p.supportedRPCTypes) {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   fmt.Sprintf("cosmos: RPC type %q not supported by this service", payload.RPCType()),
			Retryable: false,
		}
	}

	return []domain.Payload{payload}, nil
}

// SelectEndpoints filters the supplied endpoint list by:
//  1. Block height — endpoints must be within syncAllowance of the perceived head.
//  2. RPC type compatibility — endpoints must support the requested RPC type.
//
// It uses the tiered degradation logic in qos.Select.
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, payloads []domain.Payload) (domain.EndpointAddrList, error) {
	if len(endpoints) == 0 {
		return nil, nil
	}

	perceived := p.consensus.PerceivedBlock()

	// Determine requested RPC type from the first payload (if any).
	var requestedRPCType domain.RPCType
	if len(payloads) > 0 {
		requestedRPCType = payloads[0].RPCType()
	}

	// Block height filter factory (parameterised by sync allowance multiplier).
	getHeight := qos.HeightGetter(p.store, func(ep cosmosEndpoint) uint64 { return ep.BlockHeight })

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

	baseFilters := []qos.FilterFunc{makeBlockFilter(p.syncAllowance)}
	relaxedFilters := []qos.FilterFunc{makeBlockFilter(p.syncAllowance * 2)}
	nonBlockFilters := []qos.FilterFunc{}
	if rpcTypeFilter != nil {
		baseFilters = append(baseFilters, rpcTypeFilter)
		relaxedFilters = append(relaxedFilters, rpcTypeFilter)
		nonBlockFilters = append(nonBlockFilters, rpcTypeFilter)
	}

	ranker := qos.LeastStaleFallback(getHeight, perceived)
	result := qos.SelectWithKnownHeights(endpoints, getHeight, baseFilters, relaxedFilters, nonBlockFilters, ranker)

	if result.Degraded {
		p.logger.Warn("cosmos: endpoint selection degraded",
			"tier", result.Tier,
			"endpoint_count", len(result.Endpoints),
		)
	}

	return result.Endpoints, nil
}

// --- qos.BlockHeightTracker --- //

// UpdateBlockHeight records a new block height observation for an endpoint and
// feeds it into the consensus computation.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	p.store.Update(endpoint, func(ep *cosmosEndpoint) {
		ep.BlockHeight = height
	})
	p.consensus.AddObservation(endpoint, height)
}

// EndpointHeights reports the latest height each endpoint supplied
// (qos.EndpointHeightLister).
func (p *Plugin) EndpointHeights() []qos.EndpointHeight { return p.consensus.EndpointHeights() }

// SetExternalFloor takes a trusted outside height as the floor the perceived
// head may not fall below (qos.ExternalFloorSetter).
func (p *Plugin) SetExternalFloor(height uint64) { p.consensus.SetExternalFloor(height) }

// PerceivedBlockHeight returns the current consensus-derived perceived block height.
func (p *Plugin) PerceivedBlockHeight() uint64 {
	return p.consensus.PerceivedBlock()
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

// --- qos.BlockHeightParser --- //

// ParseBlockHeight extracts a block height from a relay response.
// Handles both CometBFT sync_info format and Cosmos REST height format.
func (p *Plugin) ParseBlockHeight(response []byte) (uint64, error) {
	return parseBlockHeight(response)
}

// --- qos.HealthChecker --- //

// HealthChecks returns health check payloads for the given endpoint.
// The Cosmos plugin always issues a CometBFT /status check to obtain block height.
//
// Always the CometBFT HTTP GET /status. A supplier staked for json_rpc only
// still receives it, through the service's rpc_type_fallbacks mapping
// (config.ServiceConfig): relay miners serve both surfaces from one port, so
// the GET works there. There used to be a JSON-RPC variant here selected on a
// store field nothing wrote, so it never ran; the fallback is the live
// version of that idea.
func (p *Plugin) HealthChecks() []qos.HealthCheck {
	return []qos.HealthCheck{
		{
			Name:    "comet_bft_status",
			Payload: cometBFTStatusPayload(),
			// The status response is where both the height and the chain id
			// come from; an abci_query from a client carries neither.
			Essential: true,
		},
	}
}

// --- qos.ChainViewer --- //

// ChainView reports what this service currently believes about its chain, for
// the metrics exporter. Delegates to the consensus that owns the observations.
func (p *Plugin) ChainView() qos.ChainView { return p.consensus.ChainView() }

// LastHeightObservation reports when any of these endpoints last supplied a
// block height, so the executor can tell whether its height probe would learn
// anything the plugin does not already know.
func (p *Plugin) LastHeightObservation(endpoints domain.EndpointAddrList) (time.Time, bool) {
	return p.consensus.LastHeightObservation(endpoints)
}

// --- qos.DataExtractor --- //

// ExtractData parses a relay response and returns structured data: the block
// height, and the chain identifier when the response carries one.
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, _, response []byte) (*qos.ExtractedData, error) {
	if len(response) == 0 {
		return nil, fmt.Errorf("cosmos: empty response from %s", endpoint)
	}

	// Chain identity is checked before block height is recorded. An endpoint on
	// the wrong chain reports heights that are real for that chain, so feeding
	// them to consensus would let it skew the very number the height filters
	// compare against.
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

	if height > 0 {
		p.UpdateBlockHeight(endpoint, height)
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

// --- qos.LifecycleHooks --- //

// OnSessionChange is called when endpoints are added or removed from a session.
func (p *Plugin) OnSessionChange(_ domain.ServiceID, added, removed domain.EndpointAddrList) {
	p.store.Touch(added)
	for _, addr := range removed {
		p.logger.Debug("cosmos: endpoint removed from session", "endpoint", addr)
	}
}

// OnEndpointDiscovered is called when a new endpoint is seen for the first time.
func (p *Plugin) OnEndpointDiscovered(_ domain.ServiceID, endpoint domain.EndpointAddr) {
	p.logger.Debug("cosmos: endpoint discovered", "endpoint", endpoint)
	// Ensure the endpoint exists in the store with zero values so subsequent
	// updates via Update() always have a record to modify.
	p.store.Update(endpoint, func(_ *cosmosEndpoint) {})
}

// OnEndpointEvicted is called when an endpoint is removed from the known set.
func (p *Plugin) OnEndpointEvicted(_ domain.ServiceID, endpoint domain.EndpointAddr) {
	p.logger.Debug("cosmos: endpoint evicted", "endpoint", endpoint)
}

// --- qos.StateResetter ---

// ResetState discards the block consensus and every per-endpoint observation
// (block height, chain ID) this plugin has learned. It is the admin
// chain-state reset: nothing else about the plugin's configuration changes,
// and the next health-check cycle and the next relays repopulate both from
// scratch.
func (p *Plugin) ResetState() {
	p.consensus.Reset()
	p.store.Clear()
}
