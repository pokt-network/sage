// Package noop provides a passthrough QoS plugin for generic/unknown chain types.
//
// It accepts any request and returns all session endpoints unchanged.
// Used for chains such as near, sui, tron, and radix where no chain-specific
// QoS logic is required.
//
// If a non-zero syncAllowance is provided the plugin also implements
// BlockHeightTracker so external block sources can provide a floor height.
package noop

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// endpointState holds the minimal per-endpoint state tracked by NoOp.
type endpointState struct {
	blockHeight uint64
}

// Plugin is the passthrough NoOp QoS plugin.
type Plugin struct {
	logger        *slog.Logger
	syncAllowance uint64

	// store and consensus are only used when syncAllowance > 0.
	store     *qos.EndpointStore[endpointState]
	consensus *qos.BlockConsensus
}

// NewPlugin creates a NoOp Plugin. When syncAllowance is zero, block height
// tracking is disabled and all endpoints are always returned unchanged.
func NewPlugin(logger *slog.Logger, syncAllowance uint64) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &Plugin{
		logger:        logger,
		syncAllowance: syncAllowance,
		store:         qos.NewEndpointStore[endpointState](logger),
		consensus:     qos.NewBlockConsensus(logger, syncAllowance),
	}
}

// --- qos.Plugin --- //

// ParseRequest reads the request body and splits JSON-RPC batch arrays into
// individual Payloads so the Batch middleware can fan them out to different
// suppliers. Single requests and non-JSON bodies pass through as one Payload.
//
// Unlike the EVM parser, this does not validate JSON-RPC structure (no
// requirement for "jsonrpc" or "method" fields) — generic chains may use
// non-standard formats.
func (p *Plugin) ParseRequest(_ context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	// Generic chains may be REST-shaped, where the path is the request — carry
	// it (and the verb) through so the relay miner doesn't replay a bare POST
	// against the backend's root.
	path, httpMethod := "", ""
	if req != nil && req.URL != nil {
		path, httpMethod = req.URL.RequestURI(), req.Method
	}

	if len(body) == 0 {
		return []domain.Payload{domain.NewPayload(nil, rpcType, "").WithHTTP(path, httpMethod)}, nil
	}

	// Try to split JSON-RPC batch arrays.
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(trimmed, &items); err == nil && len(items) > 1 {
			payloads := make([]domain.Payload, len(items))
			for i, item := range items {
				payloads[i] = domain.NewPayload(item, rpcType, extractMethodBestEffort(item)).
					WithHTTP(path, httpMethod)
			}
			return payloads, nil
		}
		// Malformed array or single-element — fall through to single payload.
	}

	return []domain.Payload{
		domain.NewPayload(body, rpcType, extractMethodBestEffort(body)).WithHTTP(path, httpMethod),
	}, nil
}

// extractMethodBestEffort tries to read a "method" field from JSON. Returns ""
// if the body isn't JSON or doesn't have a method field — this is fine for
// generic chains where method names are unknown or absent.
func extractMethodBestEffort(body []byte) string {
	method := gjson.GetBytes(body, "method")
	if method.Type == gjson.String {
		return method.String()
	}
	return ""
}

// SelectEndpoints returns all provided endpoints unchanged.
// If syncAllowance > 0, endpoints too far behind perceived block height are
// filtered out using tiered degradation (same as other QoS plugins).
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	if p.syncAllowance == 0 {
		return endpoints, nil
	}

	perceived := p.consensus.PerceivedBlock()
	if perceived == 0 {
		// Cold start — no consensus yet, pass all through.
		return endpoints, nil
	}

	getHeight := func(addr domain.EndpointAddr) (uint64, bool) {
		data, ok := p.store.Get(addr)
		if !ok {
			return 0, false
		}
		return data.blockHeight, true
	}

	var minHeight uint64
	if perceived > p.syncAllowance {
		minHeight = perceived - p.syncAllowance
	}
	blockFilter := qos.BlockHeightFilter(getHeight, minHeight)

	var relaxedMin uint64
	if perceived > p.syncAllowance*2 {
		relaxedMin = perceived - p.syncAllowance*2
	}
	relaxedFilter := qos.BlockHeightFilter(getHeight, relaxedMin)

	result := qos.Select(
		endpoints,
		[]qos.FilterFunc{blockFilter},
		[]qos.FilterFunc{relaxedFilter},
		nil,
		// No least-stale fallback ranker: noop serves generic chains where a
		// single service may front heterogeneous backends, so block heights
		// aren't comparable across endpoints for staleness ranking.
		nil,
	)
	return result.Endpoints, nil
}

// --- qos.BlockHeightTracker --- //

// UpdateBlockHeight records a block height observation from an endpoint.
// Only meaningful when syncAllowance > 0.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	p.store.Update(endpoint, func(s *endpointState) {
		s.blockHeight = height
	})
	p.consensus.AddObservation(endpoint, height)
}

// PerceivedBlockHeight returns the current consensus block height.
func (p *Plugin) PerceivedBlockHeight() uint64 {
	return p.consensus.PerceivedBlock()
}

// StartSync is a no-op; external health checks drive updates.
func (p *Plugin) StartSync(_ context.Context) {}

// Compile-time interface assertions.
var (
	_ qos.Plugin             = (*Plugin)(nil)
	_ qos.BlockHeightTracker = (*Plugin)(nil)
)
