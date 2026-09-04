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
//
// Each field names the middleware that write it. Most have one writer; the
// ones that have more are the ones a new middleware is most likely to get
// wrong, so the set is written out rather than summarised as "one". The
// discipline the comments enforce: a field is written by the middleware
// listed and read by anyone, and a writer not on the list is a review
// finding. (This header used to claim exactly one writer per field, which
// was false for four of them and checkable for none.)
type Context struct {
	// Set at creation, and re-wrapped — not replaced — by the middleware
	// that scope it: timeout derives a deadline, retry and hedge detach a
	// per-attempt context, request_id enriches the logger. Everything else
	// reads.
	Ctx         context.Context
	HTTPRequest *http.Request
	Logger      *slog.Logger
	// RequestID is set by request_id only.
	RequestID string

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

	// Endpoint is the pick for this attempt: written by select_endpoint only.
	Endpoint domain.EndpointAddr
	// Endpoints is the candidate list select_endpoint chooses from. It is
	// filled by select_endpoint (from the protocol) and PRUNED, in chain
	// order, by supplier_affinity (reordered), circuit_break (broken domains
	// out), method_blocks (blocked hosts out), retry and hedge (the attempt
	// that just failed out). Every pruner runs before select_endpoint; the
	// mustPrecede rules in chain_order.go hold that order.
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

	// Response is what the client gets. Written by send_relay for a relay
	// that reached a supplier; by cache and singleflight for one that did
	// not need to; by batch, which assembles the sub-relays' responses into
	// one. Shared through Clone(): every fan-out resets it on the clone
	// before running, and nothing writes through the pointer.
	Response *domain.Response
	// Err is why the relay failed. Written by whichever middleware refused
	// the request before a relay (parse, validate, batch — each renders the
	// rejection too), by send_relay for a failed attempt, and by heuristic
	// when its verdict is to try elsewhere (domain.ErrRetryVerdict).
	Err error

	// HeuristicResult is written by heuristic after send_relay returns; nil
	// when the flag is off or there was nothing to analyse. Shared through
	// Clone() like Response, and reset by every fan-out for the same reason.
	HeuristicResult *heuristic.AnalysisResult

	// ScoreSink, when non-nil, collects this request tree's per-attempt
	// reputation signals so batch can emit one per endpoint instead of one
	// per payload. Set by exactly one middleware, batch, on the parent before
	// it clones; nil on every non-batch request. SHARED across clones on
	// purpose — see ScoreSink.
	ScoreSink *ScoreSink

	// Degraded means some stage settled for less than it wanted: set by
	// select_endpoint (a below-floor pick), method_blocks (a blocked host
	// served anyway) and batch (merged from its sub-relays, atomically). The
	// router turns it into X-Degraded and sage_degraded_total.
	Degraded bool
	// Cached is set by cache; Coalesced by singleflight.
	Cached    bool
	Coalesced bool

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
