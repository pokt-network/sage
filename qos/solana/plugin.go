// Package solana provides the Solana QoS plugin for SAGE.
//
// It implements block height tracking via getEpochInfo health checks and
// filters session endpoints that are too far behind the perceived chain tip.
package solana

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// solanaEndpoint holds per-endpoint state for a Solana session endpoint.
type solanaEndpoint struct {
	BlockHeight uint64
	Slot        uint64
}

// Plugin is the Solana QoS plugin.
type Plugin struct {
	logger        *slog.Logger
	syncAllowance uint64

	store     *qos.EndpointStore[solanaEndpoint]
	consensus *qos.BlockConsensus
}

// defaultSyncAllowance is the allowance used when the service does not
// configure one.
//
// Zero cannot be the fallback here, and not because it disables the check —
// it does the opposite. SelectEndpoints computes minHeight as
// perceived-syncAllowance without EVM's `syncAllowance > 0` guard, so zero
// makes the tier-1 filter a strict `height >= perceived` comparison. Perceived
// is the max of non-outlier observations, so by construction only the endpoint
// that reported last can satisfy it: everyone else's newest report is older
// than the one that just raised the bar. At ~400ms per Solana block that bar
// moves faster than health checks can refresh an endpoint, so the tier-1 pool
// collapses onto whichever endpoint is already carrying traffic — which is the
// only thing keeping its height current. An endpoint refreshed only by health
// checks trails permanently and is filtered out, which denies it the traffic
// that would have refreshed it. PATH hit exactly this in production on
// 2026-08-18: a solana pool on one operator while the alternatives sat at
// reputation 100, tier 1, zero cooldown, and received nothing.
//
// 1500 blocks is ~10 minutes of Solana, matching what PATH ships. The value is
// deliberately generous. It decides which endpoints are *selectable*, so
// lowering it is a routing change, not a check-strictness knob.
const defaultSyncAllowance = 1500

// NewPlugin creates a Solana Plugin. syncAllowance is the maximum number of
// blocks behind the perceived chain tip that an endpoint is allowed to be.
// Zero means unconfigured and falls back to defaultSyncAllowance; it does not
// disable the check.
func NewPlugin(logger *slog.Logger, syncAllowance uint64) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	if syncAllowance == 0 {
		syncAllowance = defaultSyncAllowance
	}
	return &Plugin{
		logger:        logger,
		syncAllowance: syncAllowance,
		store:         qos.NewEndpointStore[solanaEndpoint](logger),
		consensus:     qos.NewBlockConsensus(logger, syncAllowance),
	}
}

// --- qos.Plugin --- //

// ParseRequest validates the request body and extracts a single JSON-RPC payload.
func (p *Plugin) ParseRequest(_ context.Context, _ *http.Request, body []byte, _ domain.RPCType) ([]domain.Payload, error) {
	payload, err := parseRequest(body)
	if err != nil {
		return nil, &domain.RelayError{
			Kind:      domain.ErrValidation,
			Message:   err.Error(),
			Retryable: false,
		}
	}
	return []domain.Payload{payload}, nil
}

// SelectEndpoints filters session endpoints by block height.
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	perceived := p.consensus.PerceivedBlock()

	getHeight := qos.HeightGetter(p.store, func(ep solanaEndpoint) uint64 { return ep.BlockHeight })

	blockFilter := qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, p.syncAllowance))
	// Relaxed tier: twice the allowance.
	relaxedFilter := qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, p.syncAllowance*2))

	result := qos.Select(
		endpoints,
		[]qos.FilterFunc{blockFilter},
		[]qos.FilterFunc{relaxedFilter},
		nil, // tier 3: return all (no other filters)
		qos.LeastStaleFallback(getHeight, perceived),
	)

	return result.Endpoints, nil
}

// --- qos.BlockHeightTracker --- //

// UpdateBlockHeight records a block height observation from an endpoint.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	p.store.Update(endpoint, func(ep *solanaEndpoint) {
		ep.BlockHeight = height
	})
	p.consensus.AddObservation(endpoint, height)
}

// PerceivedBlockHeight returns the current consensus block height.
func (p *Plugin) PerceivedBlockHeight() uint64 {
	return p.consensus.PerceivedBlock()
}

// StartSync is a no-op for Solana; health checks drive updates externally.
func (p *Plugin) StartSync(_ context.Context) {}

// --- qos.BlockHeightParser --- //

// ParseBlockHeight extracts a block height from a Solana JSON-RPC response.
// It handles getEpochInfo responses (result.blockHeight only — see
// extractBlockHeightFromResponse for why absoluteSlot is not accepted).
func (p *Plugin) ParseBlockHeight(response []byte) (uint64, error) {
	return extractBlockHeightFromResponse(response)
}

// --- qos.HealthChecker --- //

// HealthChecks returns the health check payloads for the given endpoint.
// Solana health checks: getEpochInfo (for block height) and getHealth.
func (p *Plugin) HealthChecks(_ domain.EndpointAddr) []qos.HealthCheck {
	return []qos.HealthCheck{
		{
			Name:    "getEpochInfo",
			Payload: epochInfoPayload(),
			// The height source. getBlockHeight is accepted from configured
			// checks too, but this is the one the plugin guarantees itself.
			Essential: true,
		},
		{
			Name:    "getHealth",
			Payload: getHealthPayload(),
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

// ExtractData parses structured data from a Solana relay response.
// It extracts the block height from getEpochInfo and getBlockHeight responses;
// which shapes are accepted depends on the request, so the request is read
// rather than ignored (see extractBlockHeightForMethod).
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*qos.ExtractedData, error) {
	height, err := extractBlockHeightForMethod(request, response)
	if err != nil {
		// Not every response carries a block height — not an error worth surfacing.
		return &qos.ExtractedData{}, nil
	}

	_, err = qos.ValidateBlockHeight(height, p.consensus.PerceivedBlock(), p.syncAllowance)
	if err != nil {
		return nil, fmt.Errorf("solana: invalid block height from endpoint %s: %w", endpoint, err)
	}

	return &qos.ExtractedData{BlockHeight: &height}, nil
}

// --- qos.CoalescenceClassifier --- //

// IsCoalescable returns true for read-only methods that are safe to de-duplicate.
// Write methods (sendTransaction, simulateTransaction) must never be coalesced.
func (p *Plugin) IsCoalescable(method string) bool {
	return coalescableMethods[method]
}

// --- qos.LifecycleHooks --- //

// OnSessionChange sweeps stale endpoints that were removed from the session.
func (p *Plugin) OnSessionChange(_ domain.ServiceID, _, removed domain.EndpointAddrList) {
	for _, addr := range removed {
		// Evict removed endpoints so stale data doesn't pollute consensus.
		p.store.Update(addr, func(ep *solanaEndpoint) {
			ep.BlockHeight = 0
			ep.Slot = 0
		})
	}
}

// OnEndpointDiscovered is called when a new endpoint enters a session.
func (p *Plugin) OnEndpointDiscovered(_ domain.ServiceID, _ domain.EndpointAddr) {}

// OnEndpointEvicted is called when an endpoint is permanently evicted.
func (p *Plugin) OnEndpointEvicted(_ domain.ServiceID, _ domain.EndpointAddr) {}

// --- qos.StateResetter --- //

// ResetState discards the block consensus and every per-endpoint observation
// this plugin has learned. It is the admin chain-state reset: nothing else
// about the plugin's configuration changes, and the next health-check cycle
// and the next relays repopulate both from scratch.
func (p *Plugin) ResetState() {
	p.consensus.Reset()
	p.store.Clear()
}

// --- payload helpers --- //

func epochInfoPayload() domain.Payload {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getEpochInfo",
		"params":  []any{},
	})
	return domain.NewPayload(body, domain.RPCTypeJSONRPC, "getEpochInfo")
}

func getHealthPayload() domain.Payload {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getHealth",
		"params":  []any{},
	})
	return domain.NewPayload(body, domain.RPCTypeJSONRPC, "getHealth")
}

// Compile-time interface assertions.
var (
	_ qos.Plugin                = (*Plugin)(nil)
	_ qos.BlockHeightTracker    = (*Plugin)(nil)
	_ qos.BlockHeightParser     = (*Plugin)(nil)
	_ qos.HealthChecker         = (*Plugin)(nil)
	_ qos.DataExtractor         = (*Plugin)(nil)
	_ qos.ChainViewer           = (*Plugin)(nil)
	_ qos.HeightObserver        = (*Plugin)(nil)
	_ qos.CoalescenceClassifier = (*Plugin)(nil)
	_ qos.LifecycleHooks        = (*Plugin)(nil)
	_ qos.StateResetter         = (*Plugin)(nil)
)
