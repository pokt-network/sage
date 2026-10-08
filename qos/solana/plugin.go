// Package solana provides the Solana QoS plugin for SAGE.
//
// It implements block height tracking via getEpochInfo health checks and
// filters session endpoints that are too far behind the perceived chain tip.
package solana

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// solanaEndpoint holds per-endpoint state for a Solana session endpoint.
type solanaEndpoint struct {
	BlockHeight uint64
}

// Plugin is the Solana QoS plugin.
type Plugin struct {
	qos.SelectionTiers
	qos.HeightTracking

	logger *slog.Logger

	store *qos.EndpointStore[solanaEndpoint]
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
	p := &Plugin{
		logger: logger,
		store:  qos.NewEndpointStore[solanaEndpoint](),
	}
	p.Consensus = qos.NewBlockConsensus(logger, syncAllowance)
	p.SetSyncAllowance(syncAllowance)
	return p
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
	perceived := p.Consensus.PerceivedBlock()

	getHeight := qos.HeightGetter(p.store, func(ep solanaEndpoint) uint64 { return ep.BlockHeight }, p.Consensus.Projection())

	blockFilter := qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, p.SyncAllowance()))
	// Relaxed tier: twice the allowance.
	relaxedFilter := qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, p.SyncAllowance()*2))

	result := qos.SelectWithKnownHeights(
		endpoints,
		getHeight,
		[]qos.FilterFunc{blockFilter},
		[]qos.FilterFunc{relaxedFilter},
		nil, // tier 3: return all (no other filters)
		qos.LeastStaleFallback(getHeight, perceived),
	)

	p.ReportTier(result.Tier)
	return result.Endpoints, nil
}

// --- qos.BlockHeightTracker --- //

// UpdateBlockHeight records a block height observation from an endpoint.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	p.store.ObserveHeight(endpoint, func(ep *solanaEndpoint) {
		ep.BlockHeight = height
	})
	p.Consensus.AddObservation(endpoint, height)
}

// --- qos.HealthChecker --- //

// HealthChecks returns the health check payloads for the given endpoint.
// Solana health checks: getEpochInfo (for block height) and getHealth.
func (p *Plugin) HealthChecks() []qos.HealthCheck {
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

// --- qos.DataExtractor --- //

// ExtractData parses structured data from a Solana relay response.
// It extracts the block height from getEpochInfo and getBlockHeight responses;
// which shapes are accepted depends on the request, so the request is read
// rather than ignored (see extractBlockHeightForMethod).
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*qos.ExtractedData, error) {
	if unfinalized(request) {
		return &qos.ExtractedData{}, nil
	}
	height, err := extractBlockHeightForMethod(request, response)
	if err != nil {
		// Not every response carries a block height — not an error worth surfacing.
		return &qos.ExtractedData{}, nil
	}

	if err := qos.ValidateBlockHeight(height, p.Consensus.PerceivedBlock()); err != nil {
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

// --- qos.StateResetter --- //

// ResetState discards the block consensus and every per-endpoint observation
// this plugin has learned. It is the admin chain-state reset: nothing else
// about the plugin's configuration changes, and the next health-check cycle
// and the next relays repopulate both from scratch.
func (p *Plugin) ResetState() {
	p.Consensus.Reset()
	p.store.Clear()
}

// --- payload helpers --- //

func epochInfoPayload() domain.Payload {
	return domain.NewPayload([]byte(`{"id":1,"jsonrpc":"2.0","method":"getEpochInfo","params":[]}`), domain.RPCTypeJSONRPC, "getEpochInfo")
}

func getHealthPayload() domain.Payload {
	return domain.NewPayload([]byte(`{"id":1,"jsonrpc":"2.0","method":"getHealth","params":[]}`), domain.RPCTypeJSONRPC, "getHealth")
}

// Compile-time interface assertions.
var (
	_ qos.Plugin                = (*Plugin)(nil)
	_ qos.BlockHeightTracker    = (*Plugin)(nil)
	_ qos.HealthChecker         = (*Plugin)(nil)
	_ qos.DataExtractor         = (*Plugin)(nil)
	_ qos.ChainViewer           = (*Plugin)(nil)
	_ qos.HeightObserver        = (*Plugin)(nil)
	_ qos.CoalescenceClassifier = (*Plugin)(nil)
	_ qos.StateResetter         = (*Plugin)(nil)
)

// AllStale reports whether every endpoint in eps is known to sit below the
// relaxed height bound (qos.StaleChecker).
func (p *Plugin) AllStale(eps domain.EndpointAddrList) bool {
	return p.AllStaleBy(eps, qos.HeightGetter(p.store, func(ep solanaEndpoint) uint64 { return ep.BlockHeight }, p.Consensus.Projection()))
}

var _ qos.StaleChecker = (*Plugin)(nil)

// solanaBlockhashValidity is how many blocks past its own a blockhash stays
// valid: getLatestBlockhash answers lastValidBlockHeight = its block + 150.
const solanaBlockhashValidity = 150

// HeadLag reads the head a getEpochInfo, getBlockHeight or getLatestBlockhash
// answer names, on the block-height scale perceived is kept in, and measures
// it against the head expected now (qos.HeadLagReader). getSlot and
// context.slot are slots, a different scale, and are not read.
func (p *Plugin) HeadLag(payload domain.Payload, response []byte, at time.Time) (lag uint64, stale, ok bool) {
	if unfinalized(payload.Bytes()) {
		return 0, false, false
	}
	var head uint64
	switch payload.Method() {
	case "getEpochInfo":
		head = gjson.GetBytes(response, "result.blockHeight").Uint()
	case "getBlockHeight":
		head = gjson.GetBytes(response, "result").Uint()
	case "getLatestBlockhash":
		if v := gjson.GetBytes(response, "result.value.lastValidBlockHeight").Uint(); v > solanaBlockhashValidity {
			head = v - solanaBlockhashValidity
		}
	}
	if head == 0 {
		return 0, false, false
	}
	return p.Consensus.AnswerLag(head, at)
}

var _ qos.HeadLagReader = (*Plugin)(nil)

// unfinalized reports whether a request asks for a commitment below finalized.
// The height probe asks at the default, finalized, which runs ~30 blocks behind
// processed: a processed answer fed to consensus lifted perceived above every
// finalized one, and measured against a finalized head it is a different
// scale. Both are kept on finalized.
func unfinalized(request []byte) bool {
	c := gjson.GetBytes(request, "params.0.commitment").String()
	return c != "" && c != "finalized"
}
