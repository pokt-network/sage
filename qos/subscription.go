package qos

import (
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

// SubscriptionAction is what a client frame asks of the subscription state.
type SubscriptionAction int

// The client-side subscription actions a plugin can recognise.
const (
	// SubscriptionNone is any frame that is not about subscriptions.
	SubscriptionNone SubscriptionAction = iota
	// SubscriptionSubscribe opens a subscription. The subscription's ID is
	// not known until the endpoint answers; RequestID links the two.
	SubscriptionSubscribe
	// SubscriptionUnsubscribe closes the subscription named by SubscriptionID.
	SubscriptionUnsubscribe
	// SubscriptionUnsubscribeAll closes every subscription on the connection.
	SubscriptionUnsubscribeAll
)

// ClientFrameInfo is a plugin's reading of one client→endpoint frame.
type ClientFrameInfo struct {
	Action SubscriptionAction
	// RequestID is the JSON-RPC id, in its raw JSON form ("1", "\"abc\"") so
	// a string id and a numeric id never collide.
	RequestID string
	// Method is the subscribe/unsubscribe method name, for reporting.
	Method string
	// SubscriptionID names the subscription an unsubscribe targets.
	SubscriptionID string
}

// EndpointFrameKind is what an endpoint→client frame is.
type EndpointFrameKind int

// The endpoint-side frame kinds a plugin can recognise.
const (
	// EndpointFrameOther is a frame that is not a subscribe response or a
	// subscription notification.
	EndpointFrameOther EndpointFrameKind = iota
	// EndpointFrameResponse answers a request: RequestID is set, and for a
	// successful subscribe SubscriptionID carries the id the endpoint
	// assigned.
	EndpointFrameResponse
	// EndpointFrameNotification is data for an established subscription.
	EndpointFrameNotification
)

// EndpointFrameInfo is a plugin's reading of one endpoint→client frame.
type EndpointFrameInfo struct {
	Kind EndpointFrameKind
	// RequestID is the raw JSON-RPC id of the request a response answers.
	RequestID string
	// SubscriptionID is the id a subscribe response assigned, or the id a
	// notification is for.
	SubscriptionID string
	// IsError marks a response carrying an error member; a subscribe that
	// failed opens nothing.
	IsError bool
}

// SubscriptionClassifier is implemented by plugins whose chain has
// subscriptions over WebSocket. It reads frames and says what they mean; it
// keeps no state. The chain-specific part is small — which methods subscribe,
// where the subscription id lives — and everything else (matching responses
// to requests, tracking what is live, remembering the original request for a
// replay) is SubscriptionRegistry, shared.
type SubscriptionClassifier interface {
	ClassifyClientFrame(data []byte) ClientFrameInfo
	ClassifyEndpointFrame(data []byte) EndpointFrameInfo
}

// Subscription is one established subscription on a connection.
type Subscription struct {
	// ID is the endpoint-assigned subscription id.
	ID string
	// Method is the method that opened it.
	Method string
	// Request is the client's original subscribe frame, verbatim, so a
	// rebind can replay it to a different endpoint.
	Request []byte
	// Since is when the endpoint confirmed it.
	Since time.Time
}

// maxTrackedSubscriptions bounds both the pending and the active tables per
// connection. A client chooses how many subscribe frames to send; past the
// cap new ones are still forwarded, just not remembered.
const maxTrackedSubscriptions = 1024

// SubscriptionRegistry tracks the subscriptions on one WebSocket connection
// from the frames that cross it. Feed it every client frame and every
// endpoint frame; ask it what is live.
//
// It exists to unblock two things the bridge cannot do without it: a rebind
// (reconnect to a different supplier and replay the live subscriptions), and
// a stall watchdog (a subscription that has stopped delivering is only a
// stall if there IS a subscription). A nil classifier makes the registry
// inert: every frame is SubscriptionNone and nothing is ever active.
type SubscriptionRegistry struct {
	classifier SubscriptionClassifier

	mu       sync.Mutex
	pending  map[string]pendingSubscription // request id → the subscribe awaiting its response
	active   map[string]Subscription        // subscription id → live subscription
	lastData time.Time
	dropped  int // subscribe frames not tracked because a table was full
}

type pendingSubscription struct {
	method  string
	request []byte
}

// NewSubscriptionRegistry returns a registry driven by classifier. nil is
// allowed and yields an inert registry.
func NewSubscriptionRegistry(classifier SubscriptionClassifier) *SubscriptionRegistry {
	return &SubscriptionRegistry{
		classifier: classifier,
		pending:    make(map[string]pendingSubscription),
		active:     make(map[string]Subscription),
	}
}

// ObserveClientFrame records what a client→endpoint frame does to the
// subscription state. The frame is not modified and not retained beyond a
// copy of a subscribe request.
func (r *SubscriptionRegistry) ObserveClientFrame(data []byte) {
	if r == nil || r.classifier == nil {
		return
	}
	info := r.classifier.ClassifyClientFrame(data)
	if info.Action == SubscriptionNone {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch info.Action {
	case SubscriptionSubscribe:
		if info.RequestID == "" {
			return // A notification-style request with no id can never be matched to its answer.
		}
		if len(r.pending) >= maxTrackedSubscriptions {
			r.dropped++
			return
		}
		r.pending[info.RequestID] = pendingSubscription{
			method:  info.Method,
			request: append([]byte(nil), data...),
		}
	case SubscriptionUnsubscribe:
		delete(r.active, info.SubscriptionID)
	case SubscriptionUnsubscribeAll:
		clear(r.active)
	}
}

// ObserveEndpointFrame records what an endpoint→client frame does: a
// subscribe response promotes a pending entry to active (or drops it on
// error), a notification marks data flowing.
func (r *SubscriptionRegistry) ObserveEndpointFrame(data []byte) {
	if r == nil || r.classifier == nil {
		return
	}
	info := r.classifier.ClassifyEndpointFrame(data)
	if info.Kind == EndpointFrameOther {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch info.Kind {
	case EndpointFrameResponse:
		p, ok := r.pending[info.RequestID]
		if !ok {
			return // A response to something that was not a subscribe.
		}
		delete(r.pending, info.RequestID)
		if info.IsError || info.SubscriptionID == "" {
			return
		}
		if len(r.active) >= maxTrackedSubscriptions {
			r.dropped++
			return
		}
		r.active[info.SubscriptionID] = Subscription{
			ID:      info.SubscriptionID,
			Method:  p.method,
			Request: p.request,
			Since:   time.Now(),
		}
	case EndpointFrameNotification:
		if _, live := r.active[info.SubscriptionID]; live {
			r.lastData = time.Now()
		}
	}
}

// Active lists the live subscriptions, in no particular order.
func (r *SubscriptionRegistry) Active() []Subscription {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Subscription, 0, len(r.active))
	for _, s := range r.active {
		out = append(out, s)
	}
	return out
}

// HasActive reports whether at least one subscription is established.
func (r *SubscriptionRegistry) HasActive() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.active) > 0
}

// LastData is when a notification for a live subscription last arrived; zero
// if never.
func (r *SubscriptionRegistry) LastData() time.Time {
	if r == nil {
		return time.Time{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastData
}

// Dropped counts subscribe frames that were forwarded but not tracked because
// a table was at its cap.
func (r *SubscriptionRegistry) Dropped() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// --- JSON-RPC helpers shared by the classifiers ---

// JSONRPCRequestID returns the raw JSON of a frame's "id" member ("1",
// "\"abc\""), or "" when absent or null. Raw rather than parsed, so a string
// id and a numeric id that print the same cannot be confused.
func JSONRPCRequestID(data []byte) string {
	id := gjson.GetBytes(data, "id")
	if !id.Exists() || id.Type == gjson.Null {
		return ""
	}
	return id.Raw
}

// JSONRPCMethod returns a frame's "method" member, or "".
func JSONRPCMethod(data []byte) string {
	return gjson.GetBytes(data, "method").String()
}

// JSONRPCHasError reports whether a frame carries a non-null "error" member.
func JSONRPCHasError(data []byte) bool {
	e := gjson.GetBytes(data, "error")
	return e.Exists() && e.Type != gjson.Null
}

// JSONRPCResultScalar returns a frame's "result" as a string when it is a
// string or a number — the two shapes a subscription id takes — or "".
func JSONRPCResultScalar(data []byte) string {
	res := gjson.GetBytes(data, "result")
	switch res.Type {
	case gjson.String:
		return res.String()
	case gjson.Number:
		return res.Raw
	}
	return ""
}

// JSONRPCFirstParam returns params[0] as a string when it is a string or a
// number, or "".
func JSONRPCFirstParam(data []byte) string {
	p := gjson.GetBytes(data, "params.0")
	switch p.Type {
	case gjson.String:
		return p.String()
	case gjson.Number:
		return p.Raw
	}
	return ""
}

// HasSuffixFold reports whether s ends with suffix, ASCII case-insensitively.
func HasSuffixFold(s, suffix string) bool {
	return len(s) >= len(suffix) && strings.EqualFold(s[len(s)-len(suffix):], suffix)
}
