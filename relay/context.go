package relay

import (
	"context"
	"log/slog"
	"net/http"
	"net/netip"
	"sync/atomic"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/heuristic"
	"github.com/pokt-network/sage/qos"
)

// Context carries state through the middleware chain.
// Each field is set by exactly one middleware and read by others.
type Context struct {
	// Immutable: set at creation
	Ctx         context.Context
	HTTPRequest *http.Request
	Logger      *slog.Logger
	RequestID   string

	// Set by ClientIP middleware: the address this request is attributed to,
	// resolved once from the peer and any trusted forwarded headers so every
	// downstream concern (rate limiting, affinity, abuse controls) agrees on who
	// the client is. Invalid (zero value) when the middleware is not in the chain
	// or the address could not be determined — callers must check IsValid.
	ClientIP netip.Addr

	// Set by Parse middleware
	ServiceID domain.ServiceID
	RPCType   domain.RPCType
	Plugin    qos.Plugin // nil if no plugin registered for the service

	// Set by QoS parsing
	Payloads []domain.Payload

	// Set by SelectEndpoint middleware
	Endpoint  domain.EndpointAddr
	Endpoints domain.EndpointAddrList

	// SelectedEndpoint, when non-nil, receives a copy of Endpoint as soon as
	// SelectEndpoint picks one. It exists for the single case where another
	// goroutine must learn an in-flight relay's endpoint before that relay
	// finishes: Hedge waits out the hedge delay and then needs the primary
	// arm's endpoint to steer the hedge arm elsewhere.
	//
	// Reading Endpoint directly for that is a data race — the primary arm
	// writes it from its own goroutine, and no channel or lock orders that
	// write against the waiter's read. The race is real rather than theoretical:
	// the two goroutines genuinely run at once by construction, and a torn
	// string header is a crash, not a stale value.
	//
	// Nil for every relay that is not being hedged, which is nearly all of
	// them. Hedge allocates one per arm, so the shallow Clone() cannot alias
	// two arms onto the same slot.
	SelectedEndpoint *atomic.Pointer[domain.EndpointAddr]

	// Set by SendRelay middleware
	Response *domain.Response
	Err      error

	// Set by Heuristic middleware after SendRelay runs.
	// Nil when the heuristic flag is disabled or there is no response to analyse.
	HeuristicResult *heuristic.AnalysisResult

	// Metadata flags set by middleware
	Degraded  bool // true if fallback strategy was used
	Cached    bool // true if response came from cache
	Coalesced bool // true if response was shared via singleflight

	// For writing the final HTTP response
	Writer ResponseWriter
}

// NewContext creates a new relay context from an HTTP request.
func NewContext(ctx context.Context, req *http.Request, logger *slog.Logger, writer ResponseWriter) *Context {
	return &Context{
		Ctx:         ctx,
		HTTPRequest: req,
		Logger:      logger,
		Writer:      writer,
	}
}

// Clone creates a shallow copy of the context. Used by hedge middleware
// to run a parallel request without mutating the original.
func (c *Context) Clone() *Context {
	cp := *c
	return &cp
}
