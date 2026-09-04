// Package jsonheight builds a QoS plugin for any chain that reports its block
// height in a JSON response, from a declaration of where.
//
// Most of what a QoS plugin does is chain-independent: filter a pool by how far
// behind an endpoint is, keep a consensus of what the head is, publish a chain
// view, run a probe on a schedule. What differs between chains is two facts —
// which request returns the height, and where in the response it sits. The EVM,
// Cosmos and Solana plugins each hand-roll the whole thing because each does
// more than that (chain-id assertion, archival demotion, several protocols on
// one service). A chain that needs only the two facts should not have to.
//
// This exists because four services on the mainnet canary — near, sui,
// eth-beacon and radix — were on the passthrough, which tracks no height at
// all, so their sync_allowance governed nothing and their selection was
// reputation alone. Nobody chose that; there was no cheap way to give a small
// chain real QoS, so every small chain got none.
//
// The declaration is Go, not config, and that is deliberate. A probe payload
// and a response path are chain semantics, and those live in a plugin where
// they can be read, tested and reviewed — not in YAML where a typo becomes a
// silent misconfiguration and a wrong path grades suppliers on a request they
// never agreed to serve.
package jsonheight

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// Chain is everything that differs between one of these chains and another.
type Chain struct {
	// Name identifies the chain in errors and logs.
	Name string
	// Probe is the health-check request that returns a height, sent over the
	// RPC type its payload carries. Its response is what HeightPath reads.
	Probe domain.Payload
	// CheckName names the probe in schedules and reputation reasons.
	CheckName string
	// HeightPath is the gjson path to the height in a response to Probe. A
	// string value is accepted as well as a number: several chains report
	// heights as decimal strings, and one that does is not lying.
	HeightPath string
	// RequestMethodPath, when set, is the gjson path at which a CLIENT request
	// names its method. It is what lets a height be read from client traffic
	// rather than from probes alone — the same request that asks for the head
	// answers with it. Empty means heights come from probes only.
	RequestMethodPath string
	// HeightMethod is the value RequestMethodPath must hold for a client
	// response to be read as a height. Ignored when RequestMethodPath is empty.
	HeightMethod string
}

// Plugin serves one chain declared by a Chain.
type Plugin struct {
	chain         Chain
	logger        *slog.Logger
	syncAllowance uint64

	store     *qos.EndpointStore[endpointState]
	consensus *qos.BlockConsensus
}

type endpointState struct {
	blockHeight uint64
}

// NewPlugin creates a plugin for the declared chain.
func NewPlugin(logger *slog.Logger, chain Chain, syncAllowance uint64) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	return &Plugin{
		chain:         chain,
		logger:        logger,
		syncAllowance: syncAllowance,
		store:         qos.NewEndpointStore[endpointState](logger),
		consensus:     qos.NewBlockConsensus(logger, syncAllowance),
	}
}

// --- qos.Plugin --- //

// ParseRequest carries the request through unchanged, including the path and
// verb for REST-shaped chains.
//
// It does not validate. These are chains SAGE knows one fact about, and
// refusing a body it does not recognise would reject traffic it has no basis
// to judge — the passthrough's behaviour, and right here too. The difference
// this plugin makes is on the response side.
func (p *Plugin) ParseRequest(_ context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	path, httpMethod := "", ""
	if req != nil && req.URL != nil {
		path, httpMethod = req.URL.RequestURI(), req.Method
	}
	method := ""
	if p.chain.RequestMethodPath != "" {
		method = gjson.GetBytes(body, p.chain.RequestMethodPath).String()
	}
	return []domain.Payload{domain.NewPayload(body, rpcType, method).WithHTTP(path, httpMethod)}, nil
}

// SelectEndpoints filters the pool by how far behind the perceived head an
// endpoint is, in the tiers qos.Select provides.
//
// Unfiltered when syncAllowance is zero or nothing has reported a height yet:
// a cold start must not empty a pool, which is the rule every other plugin
// follows.
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, _ []domain.Payload) (domain.EndpointAddrList, error) {
	if len(endpoints) == 0 {
		return endpoints, nil
	}
	// No early return for a zero allowance or a cold start: qos.MinAllowedHeight
	// yields 0 for either, which makes the filter pass everything. That rule
	// lives there, the EVM plugin relies on it the same way, and a second copy
	// here would be one to keep in step.
	perceived := p.consensus.PerceivedBlock()

	getHeight := qos.HeightGetter(p.store, func(s endpointState) uint64 { return s.blockHeight })
	result := qos.SelectWithKnownHeights(
		endpoints,
		getHeight,
		[]qos.FilterFunc{qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, p.syncAllowance))},
		[]qos.FilterFunc{qos.BlockHeightFilter(getHeight, qos.MinAllowedHeight(perceived, p.syncAllowance*2))},
		nil,
		qos.LeastStaleFallback(getHeight, perceived),
	)
	if result.Degraded {
		p.logger.Warn("endpoint selection degraded",
			"chain", p.chain.Name,
			"tier", result.Tier,
			"selected", len(result.Endpoints),
			"total", len(endpoints),
			"perceived_block", perceived,
		)
	}
	return result.Endpoints, nil
}

// --- qos.HealthChecker --- //

// HealthChecks returns the one probe this chain declares. It is Essential: it
// is the only source of the fact the plugin exists to learn.
func (p *Plugin) HealthChecks() []qos.HealthCheck {
	return []qos.HealthCheck{{
		Name:      p.chain.CheckName,
		Payload:   p.chain.Probe,
		Essential: true,
	}}
}

// --- qos.DataExtractor --- //

// ExtractData reads the height out of a response that carries one.
//
// The rule is the response, not the request: if the declared path is there,
// the response answered the question this plugin asks, and it does not matter
// who asked. That is what lets client traffic keep a busy service's chain view
// fresh between probe cycles — and it is the only rule that works for a REST
// chain, whose probe carries no body to recognise it by, so a request-shaped
// test could never tell its own probe from a client's call.
//
// A response WITHOUT the path is nothing rather than an error, because most
// traffic is not asking for the head and grading a supplier for that would be
// nonsense. The exception is a request recognisable as this plugin's own
// probe: that one asked for the head specifically, so a response with no
// height in it is the endpoint failing to answer, and it is graded.
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*qos.ExtractedData, error) {
	height, err := p.heightFrom(response)
	if err != nil {
		if p.askedForHeight(request) {
			return nil, fmt.Errorf("%s: %w", p.chain.Name, err)
		}
		return &qos.ExtractedData{}, nil
	}
	if _, err := qos.ValidateBlockHeight(height, p.consensus.PerceivedBlock(), p.syncAllowance); err != nil {
		return nil, fmt.Errorf("%s: invalid block height from endpoint %s: %w", p.chain.Name, endpoint, err)
	}
	p.UpdateBlockHeight(endpoint, height)
	return &qos.ExtractedData{BlockHeight: &height}, nil
}

// askedForHeight reports whether a request specifically asked for the head, so
// a response without one is the endpoint's failure rather than an unrelated
// call. Recognised by a non-empty body matching the probe, or by the chain's
// declared method path. A REST chain with a bodyless probe recognises neither
// and grades nothing here — its health check grades the status and the parse.
func (p *Plugin) askedForHeight(request []byte) bool {
	if probe := p.chain.Probe.Bytes(); len(probe) > 0 && string(request) == string(probe) {
		return true
	}
	if p.chain.RequestMethodPath == "" {
		return false
	}
	return gjson.GetBytes(request, p.chain.RequestMethodPath).String() == p.chain.HeightMethod
}

// heightFrom reads the declared path, accepting a number or a decimal string.
func (p *Plugin) heightFrom(response []byte) (uint64, error) {
	v := gjson.GetBytes(response, p.chain.HeightPath)
	if !v.Exists() {
		return 0, fmt.Errorf("no height at %q", p.chain.HeightPath)
	}
	switch v.Type {
	case gjson.Number:
		if v.Float() < 0 {
			return 0, fmt.Errorf("negative height at %q", p.chain.HeightPath)
		}
		return uint64(v.Float()), nil
	case gjson.String:
		n, err := strconv.ParseUint(strings.TrimSpace(v.String()), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("height at %q is not a number: %q", p.chain.HeightPath, v.String())
		}
		return n, nil
	default:
		return 0, fmt.Errorf("height at %q is %s, not a number", p.chain.HeightPath, v.Type)
	}
}

// --- qos.BlockHeightTracker --- //

// UpdateBlockHeight records a height observation and feeds consensus.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	p.store.Update(endpoint, func(s *endpointState) { s.blockHeight = height })
	p.consensus.AddObservation(endpoint, height)
}

// SetExternalFloor takes a trusted outside height as the floor the perceived
// head may not fall below (qos.ExternalFloorSetter).
func (p *Plugin) SetExternalFloor(height uint64) { p.consensus.SetExternalFloor(height) }

// PerceivedBlockHeight returns the consensus head.
func (p *Plugin) PerceivedBlockHeight() uint64 { return p.consensus.PerceivedBlock() }

// StartSync is a no-op: health checks and client traffic drive updates.
func (p *Plugin) StartSync(_ context.Context) {}

// --- qos.ChainViewer / qos.HeightObserver --- //

// ChainView reports what this service believes about its chain.
func (p *Plugin) ChainView() qos.ChainView { return p.consensus.ChainView() }

// LastHeightObservation reports when any of these endpoints last supplied a
// height, so the executor can tell whether its probe would learn anything.
func (p *Plugin) LastHeightObservation(endpoints domain.EndpointAddrList) (time.Time, bool) {
	return p.consensus.LastHeightObservation(endpoints)
}

// --- qos.StateResetter --- //

// ResetState discards the consensus and every per-endpoint height, so the
// admin chain-state route works for these chains as it does for the others.
func (p *Plugin) ResetState() {
	p.consensus.Reset()
	p.store.Clear()
}

// --- qos.MethodNormalizer --- //

// NormalizeMethod names the one method this plugin catalogues — the height
// method — and buckets everything else, so method-aware state keys per host
// per method rather than collapsing to per host. A REST chain has no method
// notion here and reports "".
func (p *Plugin) NormalizeMethod(payload domain.Payload) string {
	if p.chain.RequestMethodPath == "" {
		return ""
	}
	method := gjson.GetBytes(payload.Bytes(), p.chain.RequestMethodPath).String()
	switch method {
	case "":
		return ""
	case p.chain.HeightMethod:
		return method
	default:
		return qos.MethodOther
	}
}

// --- qos.EndpointHeightLister --- //

// EndpointHeights reports the latest height each endpoint supplied.
func (p *Plugin) EndpointHeights() []qos.EndpointHeight { return p.consensus.EndpointHeights() }

// Compile-time interface assertions.
var (
	_ qos.Plugin               = (*Plugin)(nil)
	_ qos.HealthChecker        = (*Plugin)(nil)
	_ qos.DataExtractor        = (*Plugin)(nil)
	_ qos.BlockHeightTracker   = (*Plugin)(nil)
	_ qos.ChainViewer          = (*Plugin)(nil)
	_ qos.HeightObserver       = (*Plugin)(nil)
	_ qos.StateResetter        = (*Plugin)(nil)
	_ qos.MethodNormalizer     = (*Plugin)(nil)
	_ qos.EndpointHeightLister = (*Plugin)(nil)
	_ qos.ExternalFloorSetter  = (*Plugin)(nil)
)
