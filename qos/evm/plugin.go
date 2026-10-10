package evm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
)

// evmEndpoint holds per-endpoint state observed from health checks and relays.
//
// Archival retention lives in Plugin.archival, per host, not here.
type evmEndpoint struct {
	BlockNumber uint64
}

// archivalTTL is how long one archival observation is trusted.
//
// The observation is unverified: it comes from a request a client happened to
// send, and a node that fabricates state from its current head answers it as
// convincingly as one that kept the block. An hour keeps a wrong mark cheap and
// makes a node that has since pruned re-prove itself, at the cost of nothing —
// archival requests re-observe the endpoint every time they succeed.
//
// One constant for both directions on purpose. PATH ran 30m for its verified
// health-check mark and 8h for the unverified traffic mark, so the weaker
// evidence outlived the stronger by 16x; a comment claiming they matched was
// what held the invariant, and it did not.
const archivalTTL = 1 * time.Hour

// archivalMark is what one host has shown of its history, as depths behind
// the perceived head when it answered: the shallowest it refused, which
// decides, and the deepest it served, which only informs. Depth rather than
// block number, because a pruned node's window moves with the head.
//
// A refusal is not undone by a later answer at the same depth or deeper:
// behind a balancer whose backends keep different history both happen, and
// letting the answer win sent every request back to the backend that refuses
// it. It lapses on its own clock instead, archivalTTL after the last refusal,
// so a host that starts keeping more history is tried again.
type archivalMark struct {
	served    uint64
	hasServed bool
	refused   uint64
	refusedAt time.Time
}

// withServed records the host answering at depth.
func (m archivalMark) withServed(depth uint64) archivalMark {
	m.served, m.hasServed = max(m.served, depth), true
	return m
}

// withRefused records the host refusing at depth at now: the shallowest live
// refusal stands.
func (m archivalMark) withRefused(depth uint64, now time.Time) archivalMark {
	if !m.refusing(now) || depth < m.refused {
		m.refused = depth
	}
	m.refusedAt = now
	return m
}

// refusing reports whether the mark holds a refusal still in force at now.
func (m archivalMark) refusing(now time.Time) bool {
	return !m.refusedAt.IsZero() && now.Sub(m.refusedAt) < archivalTTL
}

// admits reports whether a request depth behind the head may reach the host
// at now: unless a live refusal at that depth or shallower says otherwise,
// since most hosts carry no evidence at all.
func (m archivalMark) admits(depth uint64, now time.Time) bool {
	return !m.refusing(now) || depth < m.refused
}

// maxArchivalHosts bounds the archival memory; hosts come from staked URLs.
const maxArchivalHosts = 4096

// hostKey is the archival memory's key for an address: its host, or the whole
// address when it carries none (tests and mocks use bare names).
func hostKey(addr domain.EndpointAddr) string {
	if h := addr.Domain(); h != "" {
		return h
	}
	return string(addr)
}

// coalescableMethods are read-only EVM methods safe for request coalescing.
var coalescableMethods = map[string]bool{
	"eth_blockNumber":           true,
	"eth_chainId":               true,
	"eth_gasPrice":              true,
	"eth_maxPriorityFeePerGas":  true,
	"eth_feeHistory":            true,
	"eth_getBalance":            true,
	"eth_getCode":               true,
	"eth_getBlockByNumber":      true,
	"eth_getBlockByHash":        true,
	"eth_getTransactionByHash":  true,
	"eth_getTransactionReceipt": true,
	"eth_getTransactionCount":   true,
	"eth_getLogs":               true,
	"eth_getStorageAt":          true,
	"net_version":               true,
	"web3_clientVersion":        true,
}

// Plugin is the EVM QoS plugin. It implements:
//   - qos.Plugin
//   - qos.BlockHeightTracker
//   - qos.HealthChecker
//   - qos.DataExtractor
//   - qos.ChainViewer
//   - qos.MethodNormalizer
//   - qos.CoalescenceClassifier
//   - qos.CachePolicy
//   - qos.ImmutableClassifier
//   - qos.StateResetter
//   - qos.SubscriptionClassifier
//   - qos.WebSocketProber
type Plugin struct {
	qos.SelectionTiers
	qos.HeightTracking

	logger          *slog.Logger
	store           *qos.EndpointStore[evmEndpoint]
	expectedChainID string
	stateCanary     func() bool
	// archival remembers, per host, how deep it served or refused historical
	// state; logs the same for eth_getLogs, a separate retention: a node that
	// prunes state may keep every log, and one that keeps state may not.
	archival *qos.HostMemory[archivalMark]
	logs     *qos.HostMemory[archivalMark]
}

// Config carries the per-service settings an EVM plugin needs.
//
// A plugin is built once per service (see cmd/sagegw/wire.go), so this struct
// is where per-chain customization lands. The split it encodes: how to run a
// check stays in code — calling eth_chainId and parsing the result is an EVM
// fact, identical for every EVM service — while the values a check asserts are
// per-chain data and come from config. Adding a knob here should not mean
// teaching config how to describe a request.
//
// Zero values are sensible defaults, per the config conventions in CLAUDE.md.
type Config struct {
	// SyncAllowance is how many blocks behind the perceived chain head an
	// endpoint may fall and still serve traffic.
	SyncAllowance uint64

	// ExpectedChainID is the hex chain ID this service must serve, as
	// eth_chainId reports it (e.g. "0x1" for Ethereum mainnet). Empty disables
	// the assertion.
	ExpectedChainID string

	// StateCanary reports, when asked, whether the state canary check runs
	// (featureflag.FlagStateCanary, canary.go). Nil means never.
	StateCanary func() bool
}

// Validate reports whether the config is usable, and is called at wire time so
// a bad value fails startup rather than every health check at 3am: a chain ID
// that can never match would eject every endpoint of the service.
//
// The hex rule lives here rather than in config because it is an EVM fact.
// Other chains identify themselves differently — CometBFT reports names like
// "cosmoshub-4" — so config carries chain_id opaquely and each plugin holds
// its own chain to account.
func (c Config) Validate() error {
	if c.ExpectedChainID == "" {
		return nil
	}
	if _, err := parseHexUint64(c.ExpectedChainID); err != nil {
		return fmt.Errorf("expected_chain_id: %w", err)
	}
	return nil
}

// NewPlugin creates an EVM QoS plugin for a single service.
func NewPlugin(logger *slog.Logger, cfg Config) *Plugin {
	if logger == nil {
		logger = slog.Default()
	}
	p := &Plugin{
		logger:          logger,
		store:           qos.NewEndpointStore[evmEndpoint](),
		expectedChainID: cfg.ExpectedChainID,
		stateCanary:     cfg.StateCanary,
		archival:        qos.NewHostMemory[archivalMark](archivalTTL, maxArchivalHosts),
		logs:            qos.NewHostMemory[archivalMark](archivalTTL, maxArchivalHosts),
	}
	p.Consensus = qos.NewBlockConsensus(logger, cfg.SyncAllowance)
	p.SetSyncAllowance(cfg.SyncAllowance)
	return p
}

// --- qos.Plugin ---

// ParseRequest validates the request body and extracts one Payload per JSON-RPC call.
func (p *Plugin) ParseRequest(_ context.Context, _ *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	return parseRequest(body, rpcType)
}

// SelectEndpoints filters the candidate list by block height and archival capability.
//
// Three degradation tiers (via qos.SelectWithKnownHeights):
//   - Tier 1: block height within syncAllowance
//   - Tier 2: block height within 2×syncAllowance
//   - Tier 3: no block height filter (archival filter still applied if needed)
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, payloads []domain.Payload) (domain.EndpointAddrList, error) {
	if len(endpoints) == 0 {
		return nil, nil
	}

	perceived := p.Consensus.PerceivedBlock()
	depth, needsArchival := p.requestDepth(payloads)
	logDepth, needsLogs := p.requestLogDepth(payloads)
	now := time.Now()

	getHeight := qos.HeightGetter(p.store, func(ep evmEndpoint) uint64 { return ep.BlockNumber }, p.Consensus.Projection())

	archivalFilter := func(addr domain.EndpointAddr) bool {
		if !needsArchival {
			return true
		}
		// Only a fresh refusal at this depth or shallower excludes an
		// endpoint. Archival status is inferred from traffic that happened to
		// name a historical block, so most hosts carry no observation at all —
		// and requiring proof of archival before serving an archival request
		// would exclude every one of them, exhausting all three tiers on every
		// such request and handing back the unfiltered list anyway.
		mark, known := p.archival.Get(hostKey(addr))
		return !known || mark.admits(depth, now)
	}
	logsFilter := func(addr domain.EndpointAddr) bool {
		if !needsLogs {
			return true
		}
		mark, known := p.logs.Get(hostKey(addr))
		return !known || mark.admits(logDepth, now)
	}

	minHeight := qos.MinAllowedHeight(perceived, p.SyncAllowance())
	relaxedMin := qos.MinAllowedHeight(perceived, p.SyncAllowance()*2)

	blockFilter := qos.BlockHeightFilter(getHeight, minHeight)
	relaxedBlockFilter := qos.BlockHeightFilter(getHeight, relaxedMin)

	filters := []qos.FilterFunc{blockFilter, archivalFilter, logsFilter}
	relaxedFilters := []qos.FilterFunc{relaxedBlockFilter, archivalFilter, logsFilter}
	nonBlockFilters := []qos.FilterFunc{archivalFilter, logsFilter}

	ranker := qos.LeastStaleFallback(getHeight, perceived)
	result := qos.SelectWithKnownHeights(endpoints, getHeight, filters, relaxedFilters, nonBlockFilters, ranker)
	p.ReportTier(result.Tier)

	if result.Degraded {
		p.logger.Warn("endpoint selection degraded",
			"tier", result.Tier,
			"selected", len(result.Endpoints),
			"total", len(endpoints),
			"perceived_block", perceived,
			"needs_archival", needsArchival,
		)
	}

	return result.Endpoints, nil
}

// --- qos.BlockHeightTracker ---

// UpdateBlockHeight records a new block height observation from an endpoint.
func (p *Plugin) UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64) {
	height = reportedHeight(height)
	p.store.ObserveHeight(endpoint, func(ep *evmEndpoint) {
		ep.BlockNumber = height
	})
	p.Consensus.AddObservation(endpoint, height)
}

// reportedHeight is the height to store for one an endpoint reported. The
// store reads 0 as "no height yet", which the height filter lets through; an
// endpoint that answered 0 is at genesis, which is a height, so it is stored
// as 1.
func reportedHeight(h uint64) uint64 {
	return max(h, 1)
}

// --- Archival routing ---

// requestDepth is the deepest archival depth any payload of the batch names
// (archivalDepth); ok is false when none does, or while the head is unknown.
func (p *Plugin) requestDepth(payloads []domain.Payload) (depth uint64, ok bool) {
	head := p.Consensus.PerceivedBlock()
	for _, payload := range payloads {
		params := gjson.GetBytes(payload.Bytes(), "params").Raw
		if d, archival := archivalDepth(payload.Method(), json.RawMessage(params), head); archival {
			depth, ok = max(depth, d), true
		}
	}
	return depth, ok
}

// requestLogDepth is the deepest log depth any eth_getLogs payload of the
// batch names (logDepth); ok is false when none does, or while the head is
// unknown.
func (p *Plugin) requestLogDepth(payloads []domain.Payload) (depth uint64, ok bool) {
	head := p.Consensus.PerceivedBlock()
	for _, payload := range payloads {
		if payload.Method() != methodGetLogs {
			continue
		}
		if d, deep := logDepth(json.RawMessage(gjson.GetBytes(payload.Bytes(), "params").Raw), head); deep {
			depth, ok = max(depth, d), true
		}
	}
	return depth, ok
}

// observeLogs records what an eth_getLogs answer says about a host's log
// retention, the way observeArchival does for state.
func (p *Plugin) observeLogs(endpoint domain.EndpointAddr, request, response []byte) {
	depth, ok := logDepth(json.RawMessage(gjson.GetBytes(request, "params").Raw), p.Consensus.PerceivedBlock())
	if !ok {
		return
	}
	switch classifyArchivalResponse(response) {
	case archivalServed:
		p.logs.Update(hostKey(endpoint), func(m archivalMark, _ bool) archivalMark { return m.withServed(depth) })
	case archivalMissing:
		now := time.Now()
		p.logs.Update(hostKey(endpoint), func(m archivalMark, _ bool) archivalMark { return m.withRefused(depth, now) })
	}
}

// observeArchival records what a relay says about an endpoint's history
// retention, and reports the status it recorded.
//
// The probe is free: the request was sent by a client, not by us, and it named
// a historical block, which is the only thing that distinguishes an archival
// query from an ordinary one. PATH marked an endpoint archival on any success
// for eth_getBalance / eth_call / eth_getCode / eth_getStorageAt /
// eth_getTransactionCount without reading the block parameter — and those are
// also the ordinary way to read current state, so every pruned node answering
// eth_getBalance(addr, "latest") was promoted into the archival pool. The gate
// here is isArchivalRequest, which is why that cannot happen.
func (p *Plugin) observeArchival(endpoint domain.EndpointAddr, method string, request, response []byte) (archival bool, observed bool) {
	params := gjson.GetBytes(request, "params").Raw
	depth, ok := archivalDepth(method, json.RawMessage(params), p.Consensus.PerceivedBlock())
	if !ok {
		return false, false
	}

	switch classifyArchivalResponse(response) {
	case archivalServed:
		p.archival.Update(hostKey(endpoint), func(m archivalMark, _ bool) archivalMark { return m.withServed(depth) })
		return true, true

	case archivalMissing:
		now := time.Now()
		p.archival.Update(hostKey(endpoint), func(m archivalMark, _ bool) archivalMark { return m.withRefused(depth, now) })
		return false, true

	default:
		return false, false
	}
}

// RecordArchival implements qos.ArchivalRecorder: one attempt's answer to a
// request naming a historical block, or eth_getLogs from one, marks its host.
// Other methods return before any parsing.
func (p *Plugin) RecordArchival(endpoint domain.EndpointAddr, payload domain.Payload, response []byte) {
	switch m := payload.Method(); {
	case m == methodGetLogs:
		p.observeLogs(endpoint, payload.Bytes(), response)
	case stateMethods[m]:
		p.observeArchival(endpoint, m, payload.Bytes(), response)
	}
}

// ArchivalHosts implements qos.ArchivalLister.
func (p *Plugin) ArchivalHosts() map[string]qos.ArchivalMark {
	now := time.Now()
	out := map[string]qos.ArchivalMark{}
	for host, m := range p.archival.Snapshot() {
		v := out[host]
		if m.hasServed {
			v.ServedDepth = &m.served
		}
		if m.refusing(now) {
			v.RefusedDepth = &m.refused
		}
		out[host] = v
	}
	for host, m := range p.logs.Snapshot() {
		v := out[host]
		if m.hasServed {
			v.LogsServedDepth = &m.served
		}
		if m.refusing(now) {
			v.LogsRefusedDepth = &m.refused
		}
		out[host] = v
	}
	return out
}

var (
	_ qos.ArchivalRecorder = (*Plugin)(nil)
	_ qos.ArchivalLister   = (*Plugin)(nil)
)

// --- qos.HealthChecker ---

// HealthChecks returns the standard EVM health check payloads for an endpoint.
func (p *Plugin) HealthChecks() []qos.HealthCheck {
	blockNumberBody := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	chainIDBody := []byte(`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}`)

	checks := []qos.HealthCheck{
		{
			Name:    "eth_blockNumber",
			Payload: domain.NewPayload(blockNumberBody, domain.RPCTypeJSONRPC, "eth_blockNumber"),
			// The only method ExtractData reads a height out of, so client
			// traffic cannot stand in for it however much of it there is.
			Essential: true,
		},
		{
			Name:    "eth_chainId",
			Payload: domain.NewPayload(chainIDBody, domain.RPCTypeJSONRPC, "eth_chainId"),
			// A chain id does not change; the check exists to catch a backend
			// serving another chain under this service's name, and once every
			// few minutes catches that as well as every cycle does. Probing it
			// every cycle was half of every EVM service's probe spend.
			Interval: chainIDCheckInterval,
		},
	}
	if p.stateCanary != nil && p.stateCanary() {
		checks = append(checks, CanaryCheck(time.Now()))
	}
	return checks
}

// chainIDCheckInterval is how often the chain-id check is repeated per
// backend. A new backend is checked on the first cycle it appears regardless.
const chainIDCheckInterval = 5 * time.Minute

// --- qos.DataExtractor ---

// ExtractData parses health check responses for block number and chain ID.
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*qos.ExtractedData, error) {
	method := gjson.GetBytes(request, "method").String()

	switch method {
	case "eth_blockNumber":
		height, err := ParseBlockNumber(response)
		if err != nil {
			return nil, fmt.Errorf("eth_blockNumber: %w", err)
		}
		height = reportedHeight(height)
		p.store.ObserveHeight(endpoint, func(ep *evmEndpoint) {
			ep.BlockNumber = height
		})
		p.Consensus.AddObservation(endpoint, height)
		return &qos.ExtractedData{BlockHeight: &height}, nil

	case "eth_chainId":
		chainID, err := extractChainID(response)
		if err != nil {
			return nil, fmt.Errorf("eth_chainId: %w", err)
		}
		if err := p.assertChainID(endpoint, chainID); err != nil {
			return nil, err
		}
		return &qos.ExtractedData{ChainID: &chainID}, nil
	}

	// Anything else is user traffic, or the state canary (at latest, which
	// names no historical block). A relay that named a historical block
	// reports, for free, whether the endpoint retains it.
	if archival, observed := p.observeArchival(endpoint, method, request, response); observed {
		return &qos.ExtractedData{IsArchival: &archival}, nil
	}

	return nil, nil
}

// assertChainID checks a reported chain ID against the service's configured
// chain_id, and reports qos.ErrWrongChain when they disagree.
//
// The comparison is numeric, not textual. "0x531", "0x0531" and "0X531" are all
// Sei, and endpoints are inconsistent about padding and case; a string compare
// would eject honest endpoints for formatting. Parsing both sides also avoids
// the substring trap — matching "0x1" inside a response would accept "0x1388".
func (p *Plugin) assertChainID(endpoint domain.EndpointAddr, reported string) error {
	if p.expectedChainID == "" {
		return nil
	}

	want, err := parseHexUint64(p.expectedChainID)
	if err != nil {
		// config.validateChainID rejects this at load, so a running gateway
		// cannot reach here. If it somehow does, decline to assert rather than
		// eject every endpoint of the service over our own bad config.
		p.logger.Warn("evm: configured chain_id is unparseable, skipping assertion",
			"expected_chain_id", p.expectedChainID,
			"error", err,
		)
		return nil
	}

	got, err := parseHexUint64(reported)
	if err != nil {
		return fmt.Errorf("eth_chainId: %w", err)
	}

	if got != want {
		p.logger.Warn("evm: endpoint reported unexpected chain id",
			"endpoint", endpoint,
			"expected_chain_id", p.expectedChainID,
			"reported_chain_id", reported,
		)
		return fmt.Errorf("%w: want %s, got %s", qos.ErrWrongChain, p.expectedChainID, reported)
	}
	return nil
}

// --- qos.CoalescenceClassifier ---

// IsCoalescable returns true for read-only EVM methods that are safe to coalesce.
func (p *Plugin) IsCoalescable(method string) bool {
	return coalescableMethods[method]
}

// --- qos.CachePolicy ---

// CacheTTL returns how long a response for the given method may be cached.
//
// Rules:
//   - a response without a non-null result: 0. Null is "not yet" (a pending
//     transaction's receipt, a block a lagging node has not seen), and an
//     error is one node's answer; cached, either outlives the truth.
//   - eth_getTransactionReceipt: 5 min (confirmed transaction, immutable)
//   - eth_getBlockByNumber with a hex block param: 10 min (historical block, immutable)
//   - eth_blockNumber: 0 (always fresh)
//   - state-mutating or unknown methods: 0
func (p *Plugin) CacheTTL(method string, params []byte, response []byte) time.Duration {
	if r := gjson.GetBytes(response, "result"); !r.Exists() || r.Type == gjson.Null {
		return 0
	}
	switch method {
	case "eth_getTransactionReceipt":
		return 5 * time.Minute

	case "eth_getBlockByNumber":
		// Only cache if referencing a specific (historical) block, not "latest" etc.
		if r := gjson.ParseBytes(params); r.IsArray() {
			if arr := r.Array(); len(arr) > 0 && isExplicitBlockNumber(arr[0]) {
				return 10 * time.Minute
			}
		}
	}
	return 0
}

// --- qos.StateResetter ---

// ResetState discards the block consensus and every per-endpoint observation
// (block height, archival marks) this plugin has learned. It is the
// admin chain-state reset: nothing else about the plugin's configuration
// changes, and the next health-check cycle and the next relays repopulate
// both from scratch.
func (p *Plugin) ResetState() {
	p.Consensus.Reset()
	p.store.Clear()
	p.archival.Reset()
	p.logs.Reset()
}

// AllStale reports whether every endpoint in eps is known to sit below the
// relaxed height bound (qos.StaleChecker).
func (p *Plugin) AllStale(eps domain.EndpointAddrList) bool {
	return p.AllStaleBy(eps, qos.HeightGetter(p.store, func(ep evmEndpoint) uint64 { return ep.BlockNumber }, p.Consensus.Projection()))
}

var _ qos.StaleChecker = (*Plugin)(nil)

// HeadLag reads the head an eth_blockNumber or eth_getBlockByNumber("latest")
// answer names and measures it against the head expected now
// (qos.HeadLagReader).
func (p *Plugin) HeadLag(payload domain.Payload, response []byte, at time.Time) (lag uint64, stale, ok bool) {
	return HeadLag(payload, response, at, p.Consensus)
}

var _ qos.HeadLagReader = (*Plugin)(nil)

// HeadLag reads the head an EVM answer names (eth_blockNumber,
// eth_getBlockByNumber("latest"), the state canary) and measures it against
// consensus at at. Shared by every plugin that serves an EVM face.
func HeadLag(payload domain.Payload, response []byte, at time.Time, consensus *qos.BlockConsensus) (lag uint64, stale, ok bool) {
	if payload.Method() == "eth_call" {
		if !isCanary(payload.Bytes()) {
			return 0, false, false
		}
		ts, ok := canaryTimestamp(response)
		if !ok {
			return 0, false, false
		}
		return consensus.StateLag(ts, at)
	}
	head, ok := answeredHead(payload, response)
	if !ok {
		return 0, false, false
	}
	return consensus.AnswerLag(head, at)
}

// answeredHead is the head an EVM answer names: eth_blockNumber's, or the
// number of eth_getBlockByNumber("latest"); ok is false for anything else.
// A node at genesis names 0, a head like any other.
func answeredHead(payload domain.Payload, response []byte) (uint64, bool) {
	switch payload.Method() {
	case "eth_blockNumber":
		head, err := ParseBlockNumber(response)
		return head, err == nil
	case "eth_getBlockByNumber":
		if gjson.GetBytes(payload.Bytes(), "params.0").String() == "latest" {
			n := gjson.GetBytes(response, "result.number")
			if !n.Exists() {
				return 0, false
			}
			head, err := parseHexUint64(n.String())
			return head, err == nil
		}
	}
	return 0, false
}

// RecordHead implements qos.HeadRecorder: the head an answer names becomes
// the answering endpoint's height, without a consensus observation.
func (p *Plugin) RecordHead(endpoint domain.EndpointAddr, payload domain.Payload, response []byte) {
	if head, ok := answeredHead(payload, response); ok {
		head = reportedHeight(head)
		p.store.ObserveHeight(endpoint, func(ep *evmEndpoint) { ep.BlockNumber = head })
	}
}

var _ qos.HeadRecorder = (*Plugin)(nil)

// RefineVerdict implements qos.VerdictRefiner: a missing-state answer about a
// block too recent to have been discarded is the supplier refusing
// (RefusalVerdict).
func (p *Plugin) RefineVerdict(endpoint domain.EndpointAddr, payload domain.Payload, result heuristic.AnalysisResult) (heuristic.AnalysisResult, bool) {
	refined, ok, skip := RefusalVerdict(payload, result, p.Consensus.PerceivedBlock(), p.Consensus.BlocksIn)
	if skip != "" {
		p.logger.Debug("prune claim not judged", "endpoint", endpoint, "method", payload.Method(), "skip", skip, "detail", result.Details)
	}
	return refined, ok
}

var _ qos.VerdictRefiner = (*Plugin)(nil)
