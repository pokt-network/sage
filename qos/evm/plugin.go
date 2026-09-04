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
	"github.com/pokt-network/sage/qos"
)

// evmEndpoint holds per-endpoint state observed from health checks and relays.
//
// IsArchival is meaningful only while ArchivalExpiry is in the future. A zero
// or elapsed expiry means "never observed / no longer known", which is a third
// state and not the same as a negative: an endpoint nothing has asked for
// historical state must not be treated as having refused it.
type evmEndpoint struct {
	BlockNumber    uint64
	ChainID        string
	IsArchival     bool
	ArchivalExpiry time.Time
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
//   - qos.ArchivalDetector
//   - qos.HealthChecker
//   - qos.DataExtractor
//   - qos.ChainViewer
//   - qos.MethodNormalizer
//   - qos.CoalescenceClassifier
//   - qos.CachePolicy
//   - qos.StateResetter
//   - qos.SubscriptionClassifier
type Plugin struct {
	logger          *slog.Logger
	store           *qos.EndpointStore[evmEndpoint]
	consensus       *qos.BlockConsensus
	syncAllowance   uint64
	expectedChainID string
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
	return &Plugin{
		logger:          logger,
		store:           qos.NewEndpointStore[evmEndpoint](logger),
		consensus:       qos.NewBlockConsensus(logger, cfg.SyncAllowance),
		syncAllowance:   cfg.SyncAllowance,
		expectedChainID: cfg.ExpectedChainID,
	}
}

// --- qos.Plugin ---

// ParseRequest validates the request body and extracts one Payload per JSON-RPC call.
func (p *Plugin) ParseRequest(_ context.Context, _ *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error) {
	return parseRequest(body, rpcType)
}

// SelectEndpoints filters the candidate list by block height and archival capability.
//
// Three degradation tiers (via qos.Select):
//   - Tier 1: block height within syncAllowance
//   - Tier 2: block height within 2×syncAllowance
//   - Tier 3: no block height filter (archival filter still applied if needed)
func (p *Plugin) SelectEndpoints(endpoints domain.EndpointAddrList, payloads []domain.Payload) (domain.EndpointAddrList, error) {
	if len(endpoints) == 0 {
		return nil, nil
	}

	perceived := p.consensus.PerceivedBlock()
	needsArchival := p.IsArchivalRequest(payloads)

	getHeight := qos.HeightGetter(p.store, func(ep evmEndpoint) uint64 { return ep.BlockNumber })

	archivalFilter := func(addr domain.EndpointAddr) error {
		if !needsArchival {
			return nil
		}
		ep, ok := p.store.Get(addr)
		if !ok {
			// Unknown — let through for eventual consistency.
			return nil
		}
		// Only a fresh negative observation excludes an endpoint. Archival
		// status is inferred from traffic that happened to name a historical
		// block, so most endpoints carry no observation at all — and requiring
		// proof of archival before serving an archival request would exclude
		// every one of them, exhausting all three tiers on every such request
		// and handing back the unfiltered list anyway.
		if !archivalKnown(ep) {
			return nil
		}
		if !ep.IsArchival {
			return &domain.RelayError{
				Kind:      domain.ErrCapability,
				Message:   "endpoint does not retain historical state",
				Retryable: true,
			}
		}
		return nil
	}

	minHeight := qos.MinAllowedHeight(perceived, p.syncAllowance)
	relaxedMin := qos.MinAllowedHeight(perceived, p.syncAllowance*2)

	blockFilter := qos.BlockHeightFilter(getHeight, minHeight)
	relaxedBlockFilter := qos.BlockHeightFilter(getHeight, relaxedMin)

	filters := []qos.FilterFunc{blockFilter, archivalFilter}
	relaxedFilters := []qos.FilterFunc{relaxedBlockFilter, archivalFilter}
	nonBlockFilters := []qos.FilterFunc{archivalFilter}

	ranker := qos.LeastStaleFallback(getHeight, perceived)
	result := qos.SelectWithKnownHeights(endpoints, getHeight, filters, relaxedFilters, nonBlockFilters, ranker)

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
	p.store.Update(endpoint, func(ep *evmEndpoint) {
		ep.BlockNumber = height
	})
	p.consensus.AddObservation(endpoint, height)
}

// EndpointHeights reports the latest height each endpoint supplied
// (qos.EndpointHeightLister).
func (p *Plugin) EndpointHeights() []qos.EndpointHeight { return p.consensus.EndpointHeights() }

// SetExternalFloor takes a trusted outside height as the floor the perceived
// head may not fall below (qos.ExternalFloorSetter).
func (p *Plugin) SetExternalFloor(height uint64) { p.consensus.SetExternalFloor(height) }

// PerceivedBlockHeight returns the current consensus block height.
func (p *Plugin) PerceivedBlockHeight() uint64 {
	return p.consensus.PerceivedBlock()
}

// StartSync is a no-op for EVM; block heights are updated via health checks.
func (p *Plugin) StartSync(ctx context.Context) {}

// --- qos.BlockHeightParser ---

// ParseBlockHeight extracts a block number from an eth_blockNumber response.
func (p *Plugin) ParseBlockHeight(response []byte) (uint64, error) {
	return extractBlockNumber(response)
}

// --- qos.ArchivalDetector ---

// IsArchivalRequest returns true if any payload in the batch requests archival state.
func (p *Plugin) IsArchivalRequest(payloads []domain.Payload) bool {
	for _, payload := range payloads {
		params := gjson.GetBytes(payload.Bytes(), "params").Raw
		if isArchivalRequest(payload.Method(), json.RawMessage(params)) {
			return true
		}
	}
	return false
}

// IsArchivalEndpoint returns true if the endpoint is known to support archival
// data. An endpoint nothing has observed serving historical state returns
// false: this asks what is known, not what is allowed. Selection uses the
// weaker question — see the archival filter in SelectEndpoints.
func (p *Plugin) IsArchivalEndpoint(endpoint domain.EndpointAddr) bool {
	ep, ok := p.store.Get(endpoint)
	if !ok {
		return false
	}
	return archivalKnown(ep) && ep.IsArchival
}

// archivalKnown reports whether the endpoint carries an archival observation
// that has not aged out.
func archivalKnown(ep evmEndpoint) bool {
	return !ep.ArchivalExpiry.IsZero() && time.Now().Before(ep.ArchivalExpiry)
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
	if !isArchivalRequest(method, json.RawMessage(params)) {
		return false, false
	}

	switch classifyArchivalResponse(response) {
	case archivalServed:
		p.store.Update(endpoint, func(ep *evmEndpoint) {
			ep.IsArchival = true
			ep.ArchivalExpiry = time.Now().Add(archivalTTL)
		})
		return true, true

	case archivalMissing:
		p.store.Update(endpoint, func(ep *evmEndpoint) {
			ep.IsArchival = false
			ep.ArchivalExpiry = time.Now().Add(archivalTTL)
		})
		return false, true

	default:
		return false, false
	}
}

// --- qos.HealthChecker ---

// HealthChecks returns the standard EVM health check payloads for an endpoint.
func (p *Plugin) HealthChecks() []qos.HealthCheck {
	blockNumberBody := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	chainIDBody := []byte(`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}`)

	return []qos.HealthCheck{
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
}

// chainIDCheckInterval is how often the chain-id check is repeated per
// backend. A new backend is checked on the first cycle it appears regardless.
const chainIDCheckInterval = 5 * time.Minute

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

// --- qos.DataExtractor ---

// ExtractData parses health check responses for block number and chain ID.
func (p *Plugin) ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*qos.ExtractedData, error) {
	method := gjson.GetBytes(request, "method").String()

	switch method {
	case "eth_blockNumber":
		height, err := extractBlockNumber(response)
		if err != nil {
			return nil, fmt.Errorf("eth_blockNumber: %w", err)
		}
		p.store.Update(endpoint, func(ep *evmEndpoint) {
			ep.BlockNumber = height
		})
		p.consensus.AddObservation(endpoint, height)
		return &qos.ExtractedData{BlockHeight: &height}, nil

	case "eth_chainId":
		chainID, err := extractChainID(response)
		if err != nil {
			return nil, fmt.Errorf("eth_chainId: %w", err)
		}
		// Record what the endpoint actually reported before asserting, so a
		// mismatch is visible in endpoint state and not only in the error.
		p.store.Update(endpoint, func(ep *evmEndpoint) {
			ep.ChainID = chainID
		})
		if err := p.assertChainID(endpoint, chainID); err != nil {
			return nil, err
		}
		return &qos.ExtractedData{ChainID: &chainID}, nil
	}

	// Anything else is user traffic: health checks send only the two methods
	// above. A relay that named a historical block reports, for free, whether
	// the endpoint retains it.
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
//   - eth_getTransactionReceipt: 5 min (confirmed transaction, immutable)
//   - eth_getBlockByNumber with a hex block param: 10 min (historical block, immutable)
//   - eth_blockNumber: 0 (always fresh)
//   - state-mutating or unknown methods: 0
func (p *Plugin) CacheTTL(method string, params []byte, response []byte) time.Duration {
	switch method {
	case "eth_getTransactionReceipt":
		return 5 * time.Minute

	case "eth_getBlockByNumber":
		// Only cache if referencing a specific (historical) block, not "latest" etc.
		result := gjson.ParseBytes(params)
		if result.IsArray() {
			arr := result.Array()
			if len(arr) > 0 && arr[0].Type == gjson.String {
				blockParam := arr[0].String()
				if !recentStateBlockTags[blockParam] {
					if _, err := parseHexUint64(blockParam); err == nil {
						return 10 * time.Minute
					}
				}
			}
		}
		return 0

	case "eth_blockNumber":
		return 0

	case "eth_sendRawTransaction",
		"eth_sendTransaction",
		"eth_signTransaction",
		"eth_sign",
		"personal_sign":
		return 0
	}

	return 0
}

// --- qos.ResponseFormatValidator ---

// ValidateResponseFormat checks that the result field matches the expected type for the method.
func (p *Plugin) ValidateResponseFormat(method string, result json.RawMessage) error {
	switch method {
	case "eth_blockNumber", "eth_chainId", "eth_gasPrice", "eth_maxPriorityFeePerGas":
		return validateHexString(method, result)
	}
	return nil
}

// validateHexString returns an error if result is not a JSON string containing a hex value.
func validateHexString(method string, result json.RawMessage) error {
	parsed := gjson.ParseBytes(result)
	if parsed.Type != gjson.String {
		return fmt.Errorf("%s: expected hex string result, got %s", method, parsed.Type)
	}
	s := parsed.String()
	if _, err := parseHexUint64(s); err != nil {
		return fmt.Errorf("%s: result is not valid hex: %w", method, err)
	}
	return nil
}

// --- qos.LifecycleHooks ---

// OnSessionChange touches known endpoints and sweeps those that have left the session.
func (p *Plugin) OnSessionChange(serviceID domain.ServiceID, added, removed domain.EndpointAddrList) {
	p.store.Touch(added)
	for _, addr := range removed {
		p.logger.Debug("endpoint removed from session",
			"service", serviceID,
			"endpoint", addr,
		)
	}
}

// OnEndpointDiscovered initialises a zero-valued store entry for a newly seen endpoint.
func (p *Plugin) OnEndpointDiscovered(serviceID domain.ServiceID, endpoint domain.EndpointAddr) {
	p.store.Update(endpoint, func(_ *evmEndpoint) {})
	p.logger.Debug("endpoint discovered", "service", serviceID, "endpoint", endpoint)
}

// OnEndpointEvicted logs the eviction; the store entry is kept until SweepStale removes it.
func (p *Plugin) OnEndpointEvicted(serviceID domain.ServiceID, endpoint domain.EndpointAddr) {
	p.logger.Debug("endpoint evicted", "service", serviceID, "endpoint", endpoint)
}

// --- qos.StateResetter ---

// ResetState discards the block consensus and every per-endpoint observation
// (block height, chain ID, archival marks) this plugin has learned. It is the
// admin chain-state reset: nothing else about the plugin's configuration
// changes, and the next health-check cycle and the next relays repopulate
// both from scratch.
func (p *Plugin) ResetState() {
	p.consensus.Reset()
	p.store.Clear()
}
