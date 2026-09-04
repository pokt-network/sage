// Package tron provides QoS for TRON, which answers two request framings on
// one service and needs both.
//
// TRON exposes an Ethereum-compatible JSON-RPC surface (eth_blockNumber,
// eth_chainId, eth_call) alongside its own REST API (/wallet/getnowblock and
// friends), and real traffic uses both: measured across the PATH fleet on
// 2026-09-04, 72% of TRON relays were JSON-RPC and 28% REST. Neither existing
// plugin serves that. The EVM plugin refuses a non-JSON-RPC request outright,
// with a non-retryable validation error, so it would have turned 28% of the
// traffic into errors in exchange for probing the other 72%. The passthrough
// takes everything and understands none of it: no health checks, no block
// heights, no chain view, and — because it implements no DataExtractor — a
// block-height filter that never receives a height and therefore never
// filters. TRON ran on the passthrough on both fleets, which is how the
// largest service by relay count came to have no QoS at all without anyone
// deciding it should.
//
// So this is the EVM plugin for the framing it understands and the passthrough
// for the framing it does not. Everything else — health checks, height
// extraction, consensus, archival, chain view — is EVM's, unmodified, because
// TRON's JSON-RPC surface really is Ethereum's and reimplementing it would be
// two copies to keep in step.
//
// The composition is only sound because an endpoint's identity does not change
// with RPC type: domain.EndpointAddr is supplier plus public URL, so a height
// learned from a JSON-RPC probe is stored against the same address a REST
// request selects, and height filtering covers both framings from one set of
// observations.
package tron

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/qos/noop"
)

// Config is the EVM plugin's config: TRON identifies its chain the way
// Ethereum does — a hex chain id, 0x2b6653dc on mainnet — so the validation
// and the comparison are the same, and a second type declaring the same two
// fields would be a copy waiting to drift.
type Config = evm.Config

// Plugin serves TRON. It IS the EVM plugin, with one method replaced.
type Plugin struct {
	*evm.Plugin

	// passthrough parses the framings EVM refuses. Held as a whole plugin
	// rather than reimplemented because the REST path has real behaviour worth
	// not duplicating — carrying the request URI and verb through, so the
	// relay miner replays the actual path instead of a bare POST at the
	// backend's root, and splitting JSON-RPC batch arrays. Only ParseRequest
	// is ever called on it; it holds no state this plugin reads.
	passthrough *noop.Plugin
}

// NewPlugin creates a TRON QoS plugin.
func NewPlugin(logger *slog.Logger, cfg Config) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &Plugin{
		Plugin: evm.NewPlugin(logger, cfg),
		// Sync allowance zero: this instance is a parser. Height filtering for
		// TRON is the embedded EVM plugin's, fed by its own observations.
		passthrough: noop.NewPlugin(logger, 0),
	}
}

// ParseRequest routes by framing: JSON-RPC and WebSocket to the EVM parser,
// which validates the envelope and extracts the method, and everything else to
// the passthrough, which carries the path and body through untouched.
//
// This is the whole of the divergence from EVM. A REST request parsed here
// still goes through EVM's SelectEndpoints, so it is height-filtered against
// heights the JSON-RPC probes gathered — which is the point of serving both
// framings from one plugin rather than two services.
func (p *Plugin) ParseRequest(ctx context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	if rpcType == domain.RPCTypeJSONRPC || rpcType == domain.RPCTypeWebSocket {
		return p.Plugin.ParseRequest(ctx, req, body, rpcType)
	}
	return p.passthrough.ParseRequest(ctx, req, body, rpcType)
}
