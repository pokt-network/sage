package qos

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
)

// ErrWrongChain is returned by DataExtractor.ExtractData when an endpoint
// reports a chain identifier that disagrees with the service's configured
// chain_id.
//
// It is separated from ordinary extraction errors because the two mean
// different things: a malformed response is an endpoint having a bad moment,
// while a wrong chain ID means the endpoint is healthy and confidently serving
// somebody else's chain. Callers escalate accordingly — see
// healthcheck.checkSignal.
var ErrWrongChain = errors.New("qos: endpoint reported unexpected chain id")

// Plugin is the interface that chain-specific QoS implementations provide.
// Adding a new chain requires implementing ONLY this interface.
type Plugin interface {
	// ParseRequest validates the request and extracts payloads. body is the
	// already-read request body (nil if there was none); implementations must
	// use it instead of reading req.Body, which has already been consumed once.
	// req is provided for path/header/method inspection only.
	ParseRequest(ctx context.Context, req *http.Request, body []byte, rpcType domain.RPCType) ([]domain.Payload, error)

	// SelectEndpoints adds chain-specific filtering (block height, archival, sync, etc.).
	// Generic reputation/tiering is handled by infrastructure, not plugins.
	SelectEndpoints(endpoints domain.EndpointAddrList, payloads []domain.Payload) (domain.EndpointAddrList, error)
}

// --- Optional extension interfaces --- //
// Plugins implement only what they need.

// BlockHeightTracker is implemented by plugins that track block height per endpoint.
type BlockHeightTracker interface {
	UpdateBlockHeight(endpoint domain.EndpointAddr, height uint64)
	PerceivedBlockHeight() uint64
}

// RPCTypeClassifier is implemented by a plugin whose chain fronts several
// protocols on one service and can say which one a request addresses better
// than the generic detector in relay/middleware/parse.go can. Parse consults
// it after generic detection and before the client's RPC-Type header, which
// wins over both.
//
// It exists because a surface is not always its own RPC type. CometBFT is
// one node answering JSON-RPC POSTs and HTTP GETs on one port, and on Pocket
// a supplier commonly stakes json_rpc for the first face and rest for the
// second, with no comet_bft stake at all. Which type a CometBFT request
// should be relayed as therefore depends on what the service declares, and
// that knowledge belongs to the chain's plugin, not to config or to the
// generic detector.
type RPCTypeClassifier interface {
	// ClassifyRPCType returns the type the request should be validated,
	// pooled, scored and sent as, given what generic detection said. It is
	// the one type the request carries end to end: the plugin's ParseRequest
	// is handed the result and must type its payloads the same way.
	ClassifyRPCType(req *http.Request, body []byte, detected domain.RPCType) domain.RPCType
}

// HealthChecker is implemented by plugins that provide health check payloads.
//
// The checks are a property of the plugin rather than of any one endpoint —
// every implementation ignored the endpoint it used to be handed — and holding
// that parameter had a cost beyond a dead argument: the executor could not
// learn which RPC types a service's checks needed until it had already fetched
// endpoints for one type it had to guess. It guessed JSON-RPC, so a service
// staked only for REST was never health-checked at all.
type HealthChecker interface {
	HealthChecks() []HealthCheck
}

// HealthCheck describes a single health check request for an endpoint.
type HealthCheck struct {
	Payload domain.Payload
	Name    string
	// Timeout bounds this one check. Zero means the executor's probe timeout.
	// A configured check may name its own (local[].checks[].timeout); a
	// plugin's checks leave it zero.
	Timeout time.Duration
	// ExpectedStatus is the one HTTP status graded as healthy. Zero means any
	// 2xx, which is what nearly every check wants.
	ExpectedStatus int
	// Interval is the check's own minimum spacing per backend, for a fact that
	// does not change between cycles (a chain id). Zero means the service's
	// cadence. It only ever slows a check down: the executor runs it at the
	// longer of this and the service's interval, and always on the first cycle
	// a backend is seen.
	Interval time.Duration

	// GradesHead marks a check whose answer names the chain head: the
	// executor grades it through qos.HeadLagReader and records it beside
	// client head answers, so a party's stale share counts it. Such a check
	// records no reputation signal of its own.
	GradesHead bool

	// Essential marks a check that client traffic cannot stand in for, so
	// traffic-informed probing never skips it.
	//
	// The distinction is about what an observation CONTAINS, not how many
	// there are. A probe sends a payload the plugin chose because its response
	// yields a specific fact — for EVM, eth_blockNumber is the only method
	// ExtractData reads a height out of. A sampled client relay is whatever
	// the client asked for, so a service carrying heavy eth_call traffic can
	// produce thousands of observations a minute and not one block height.
	// Traffic-informed probing's threshold guarantees observation count; only
	// this flag guarantees the plugin still learns what it probes for.
	//
	// Mark the minimum: every essential check is a probe relay that gets paid
	// for on every cycle regardless of traffic, which is exactly the cost the
	// skipping exists to avoid.
	Essential bool
}

// DataExtractor is implemented by plugins that extract structured data from responses.
type DataExtractor interface {
	ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*ExtractedData, error)
}

// ExtractedData holds structured data parsed from a relay response.
type ExtractedData struct {
	BlockHeight *uint64
	ChainID     *string
	IsArchival  *bool
}

// Empty reports that a response yielded no fact at all. For a probe that
// exists to learn one, an empty answer is the endpoint failing to answer.
func (d *ExtractedData) Empty() bool {
	return d == nil || (d.BlockHeight == nil && d.ChainID == nil && d.IsArchival == nil)
}

// ExternalFloorSetter is implemented by plugins whose block consensus can
// take a height from outside the pool (services[].external_block_sources): a
// node the operator trusts, whose height is a floor the perceived head may
// not fall below. Feeding such a height in as an ordinary endpoint
// observation — which is what wiring did until 2026-09-04 — gave it one vote
// among many and a fake endpoint in the chain view.
type ExternalFloorSetter interface {
	SetExternalFloor(height uint64)
}

// MethodFamilyLister is implemented by a plugin that can say which other
// catalogued methods a host refusing one method will refuse too. The cosmos
// plugin answers for the EVM face of a chain like kava: a json_rpc host that
// says -32601 to eth_blockNumber is a CometBFT node with no EVM at all, and
// every eth_ method will get the same answer. Nil means no inference.
type MethodFamilyLister interface {
	MethodFamily(method string) []string
}

// VerdictRefiner is implemented by a plugin that can re-attribute a heuristic
// verdict from the request's shape: the analyzer sees a status and a body,
// the plugin knows which routes a node answers with that status by design.
// The cosmos plugin answers for the REST paths a gRPC-gateway node turns a
// query failure into a 5xx on (a cosmwasm smart query against the wrong
// contract, a transaction lookup at a height the node does not hold): the
// answer is the chain's, delivered to the client, nobody scored. The
// refined verdict replaces the analyzer's; ok false leaves it as it was.
// endpoint is the host that answered, for the plugin's own logging of what it
// did not refine.
type VerdictRefiner interface {
	RefineVerdict(endpoint domain.EndpointAddr, payload domain.Payload, result heuristic.AnalysisResult) (refined heuristic.AnalysisResult, ok bool)
}

// SyncAllowanceTuner is implemented by plugins whose block-height filter has
// a sync allowance that the tuning knob qos.sync_allowance may move at
// runtime, per service. SyncAllowance reports the value in force.
type SyncAllowanceTuner interface {
	SyncAllowance() uint64
	SetSyncAllowance(blocks uint64)
}

// MethodOther is the bucket NormalizeMethod returns for a method the plugin
// does not catalogue. One bucket, so unknown methods cost one key, not one
// per client-chosen string.
const MethodOther = "_other"

// MethodNormalizer is implemented by plugins that can name a payload's method
// from a bounded set. The returned string is a key in method-aware state and
// a metric label, so it must come from the plugin's own catalogue — never
// from the request verbatim. Only a label's value SET bounds it; a sanitizer
// bounds shape, not set.
type MethodNormalizer interface {
	// NormalizeMethod returns the catalogued name, MethodOther for a method
	// the plugin does not list, or "" when the payload has no method notion
	// at all (a raw body under a plugin that does not parse it).
	NormalizeMethod(payload domain.Payload) string
}

// MethodClassifier is implemented by plugins that can say what a payload's
// method costs a node to answer: domain.MethodClassLight, MethodClassStandard
// or MethodClassHeavy. It keys per-class failure shares and the per-class
// attempt metric, so a party that answers only the cheap calls is told apart
// from one that answers all of them. Without it every method is standard.
type MethodClassifier interface {
	MethodClass(payload domain.Payload) string
}

// CoalescenceClassifier is implemented by plugins that support request coalescing.
type CoalescenceClassifier interface {
	IsCoalescable(method string) bool
}

// ImmutableClassifier is implemented by plugins that can say a request's answer
// cannot change once it exists: a transaction by hash, a block by hash or by
// explicit number. Any two synced nodes give the same answer to such a request,
// so the quorum middleware lets a majority of byte-identical answers decide it.
// A request the plugin cannot vouch for — anything at "latest", anything
// pending — is answered in collect mode instead, since nodes a block apart
// legitimately disagree on it.
type ImmutableClassifier interface {
	IsImmutable(payload domain.Payload) bool
}

// CachePolicy is implemented by plugins that control per-method response caching.
type CachePolicy interface {
	CacheTTL(method string, params []byte, response []byte) time.Duration
}

// EndpointHeight is one endpoint's latest reported height.
type EndpointHeight struct {
	Endpoint   domain.EndpointAddr `json:"endpoint"`
	Height     uint64              `json:"height"`
	ObservedAt time.Time           `json:"observed_at"`
}

// ArchivalRecorder is implemented by plugins that learn per host whether an
// endpoint retains historical state, from the answers to requests naming an
// old block. The Heuristic middleware hands it every attempt's answer: a
// refusal a retry rescued still marks the host that refused, which the
// observation pipeline, seeing only the delivered answer, never does.
type ArchivalRecorder interface {
	RecordArchival(endpoint domain.EndpointAddr, payload domain.Payload, response []byte)
}

// ArchivalMark is what one host has shown of its history, as depths behind
// the head when it answered: the deepest it served and the shallowest it
// refused. Nil is no evidence.
type ArchivalMark struct {
	ServedDepth  *uint64 `json:"served_depth,omitempty"`
	RefusedDepth *uint64 `json:"refused_depth,omitempty"`
	// LogsServedDepth and LogsRefusedDepth are the same for eth_getLogs,
	// whose retention a node keeps apart from its state.
	LogsServedDepth  *uint64 `json:"logs_served_depth,omitempty"`
	LogsRefusedDepth *uint64 `json:"logs_refused_depth,omitempty"`
}

// ArchivalLister is implemented by plugins that can list their live archival
// marks per host, for the admin chain-state read.
type ArchivalLister interface {
	ArchivalHosts() map[string]ArchivalMark
}

// EndpointHeightLister is implemented by plugins that can say what each
// endpoint last reported, for the admin chain-state read. It exists because
// on 2026-09-04 one sui endpoint reported a near-zero height for two cycles
// and there was no way to ask which.
type EndpointHeightLister interface {
	EndpointHeights() []EndpointHeight
}

// HeadLagReader is implemented by plugins that can read the chain head out of
// the answers to some methods (eth_blockNumber, Solana getBlockHeight, …).
// HeadLag reports how far the answer's head lags the head the plugin expects
// at the moment the answer was received, and whether that counts as stale; ok
// is false for any other method, any answer that names no head, or while the
// plugin has no head yet. at matters for a health check another pod ran: its
// result is applied here later than it was received.
//
// It exists because a response cache only looks fast. On mainnet solana
// (2026-09-29) one owner served getEpochInfo frozen for a minute and
// getBlockHeight 60-110 slots behind, from one cache in front of every
// hostname, at a tenth of its peers' latency — which the latency tie-break
// rewarded. The height filter never saw it: the lag sat inside the sync
// allowance. Only the answer says how old it is.
type HeadLagReader interface {
	HeadLag(payload domain.Payload, response []byte, at time.Time) (lag uint64, stale, ok bool)
}

// HeadRecorder takes the head a client answer names (an eth_blockNumber, a
// "latest" block) as the answering endpoint's height
// (featureflag.FlagHeadAnswersHeight). Health checks reach an endpoint once a
// cycle (2 minutes on mainnet), so between them the height filter reads an
// old number; an endpoint serving traffic answers head calls many times a
// second. Only the endpoint's height is written, not the consensus: a party
// already casts one vote there, and a busy one would crowd the window.
type HeadRecorder interface {
	RecordHead(endpoint domain.EndpointAddr, payload domain.Payload, response []byte)
}

// HeightLagReader measures a bare head height, one a supplier pushed rather
// than answered (an EVM newHeads notification), the way HeadLagReader
// measures an answer's: lag behind the head expected at at, stale past the
// same tolerance. HeightTracking implements it for every height-aware plugin.
type HeightLagReader interface {
	HeightLag(height uint64, at time.Time) (lag uint64, stale, ok bool)
}

// ResultValidator is implemented by a plugin that knows a method's result
// encoding. InvalidResult reports a successful answer whose result no node
// produces for that method (an EVM DATA result that is not hex bytes), with
// a detail for the verdict. The heuristic middleware grades it
// invalid_result, a supplier failure retried elsewhere, behind
// featureflag.FlagInvalidResult; the answer is counted either way.
type ResultValidator interface {
	InvalidResult(payload domain.Payload, response []byte) (detail string, invalid bool)
}

// StaleChecker is implemented by plugins that filter on block height. AllStale
// reports whether every endpoint in eps is known to sit below the relaxed
// (tier-2) height bound: a list SelectEndpoints can serve only by abandoning
// the height filter. An endpoint with no known height is not stale.
//
// It exists for the middleware that narrows the pool before the plugin sees
// it. Retry and hedge each prefer an operator not already tried; the plugin
// is handed only what is left and cannot know a fresh host was set aside. On
// mainnet metis (2026-09-28) the operators left after the in-sync ones were
// tried was one operator 79,000 blocks behind, the plugin fell to tier 3, and
// ~120 relays an hour were served from it. The narrowing asks first.
type StaleChecker interface {
	AllStale(eps domain.EndpointAddrList) bool
}

// StateResetter is implemented by plugins that hold learned chain state — block
// consensus, per-endpoint heights, chain-id assertions, archival marks — that an
// operator may need to discard without a restart.
type StateResetter interface {
	// ResetState discards everything the plugin has learned for its service:
	// block consensus (perceived height, external floor) and any per-endpoint
	// QoS state (heights, chain-id observations, archival marks). Nothing else
	// is touched, and nothing is unsafe about the moment right after — an
	// endpoint the store no longer knows is treated as unknown, which
	// SelectEndpoints already lets through, so the next health-check cycle and
	// the next relays simply repopulate it.
	ResetState()
}
