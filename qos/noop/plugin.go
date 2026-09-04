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

// Plugin is the passthrough QoS plugin: it relays and scores, and understands
// nothing about the payload.
//
// It tracks no block height, and that is a statement about what it can know
// rather than a gap. A height comes from a response, and reading one requires
// knowing which method returns it and where in the body it sits — which is
// exactly the chain knowledge this plugin is defined by not having. It carried
// a full block-height filter until 2026-09-04, fed by an UpdateBlockHeight
// nothing on the relay or probe path ever called, because both call sites are
// gated on a DataExtractor this plugin does not implement. So sync_allowance
// on a passthrough service read as live and decided nothing, for as long as
// the plugin has existed.
//
// A chain whose heights matter needs a plugin that can read them. TRON got one
// the same day, for exactly this reason.
type Plugin struct {
	logger *slog.Logger
}

// NewPlugin creates a passthrough Plugin.
//
// syncAllowance is accepted and ignored: it is meaningless without per-endpoint
// heights, the caller passes it uniformly for every plugin, and refusing it
// here would push a type switch into the wiring to say nothing useful.
func NewPlugin(logger *slog.Logger, _ uint64) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &Plugin{logger: logger}
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
// SelectEndpoints returns the candidates unchanged.
//
// There is nothing to filter on. Block height is the only thing the other
// plugins narrow a pool by, and this one has no way to learn it; reputation
// and the operator controls (bans, drains, blacklists) do their filtering
// before selection is reached, at the one place endpoints are handed out.
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	return endpoints, nil
}

// Compile-time interface assertions. Deliberately short: this plugin
// implements the core interface and none of the optional ones, which is what
// makes it the passthrough. A new assertion here should make somebody ask
// whether the capability can actually be honoured for a chain nothing is known
// about.
var _ qos.Plugin = (*Plugin)(nil)
