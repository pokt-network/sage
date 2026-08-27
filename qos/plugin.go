package qos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/pokt-network/sage/domain"
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
	StartSync(ctx context.Context)
}

// BlockHeightParser is implemented by plugins that can extract block height from responses.
type BlockHeightParser interface {
	ParseBlockHeight(response []byte) (uint64, error)
}

// ArchivalDetector is implemented by plugins that distinguish archival requests/endpoints.
type ArchivalDetector interface {
	IsArchivalRequest(payloads []domain.Payload) bool
	IsArchivalEndpoint(endpoint domain.EndpointAddr) bool
}

// HealthChecker is implemented by plugins that provide health check payloads.
type HealthChecker interface {
	HealthChecks(endpoint domain.EndpointAddr) []HealthCheck
}

// HealthCheck describes a single health check request for an endpoint.
type HealthCheck struct {
	Payload domain.Payload
	Name    string
}

// DataExtractor is implemented by plugins that extract structured data from responses.
type DataExtractor interface {
	ExtractData(endpoint domain.EndpointAddr, request, response []byte) (*ExtractedData, error)
}

// ExtractedData holds structured data parsed from a relay response.
type ExtractedData struct {
	BlockHeight *uint64
	ChainID     *string
	IsSyncing   *bool
	IsArchival  *bool
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

// CoalescenceClassifier is implemented by plugins that support request coalescing.
type CoalescenceClassifier interface {
	IsCoalescable(method string) bool
}

// CachePolicy is implemented by plugins that control per-method response caching.
type CachePolicy interface {
	CacheTTL(method string, params []byte, response []byte) time.Duration
}

// ResponseFormatValidator is implemented by plugins that validate response structure.
type ResponseFormatValidator interface {
	ValidateResponseFormat(method string, result json.RawMessage) error
}

// LifecycleHooks is implemented by plugins that react to session and endpoint changes.
type LifecycleHooks interface {
	OnSessionChange(serviceID domain.ServiceID, added, removed domain.EndpointAddrList)
	OnEndpointDiscovered(serviceID domain.ServiceID, endpoint domain.EndpointAddr)
	OnEndpointEvicted(serviceID domain.ServiceID, endpoint domain.EndpointAddr)
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
