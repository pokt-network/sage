package qos

import (
	"fmt"
	"hash/maphash"
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

// Span locates a value inside a frame, so the registry can rewrite it in
// place. A zero Len means unknown, and the registry then leaves the frame
// alone.
type Span struct {
	Index, Len int
}

// ClientFrameInfo is a plugin's reading of one client→endpoint frame.
//
// Every id is in its raw JSON form ("1", "\"0xabc\"") — never decoded — so a
// string and a number that print alike cannot collide, and so a rewrite can
// splice one id over another byte for byte.
type ClientFrameInfo struct {
	Action SubscriptionAction
	// RequestID is the raw JSON-RPC id.
	RequestID string
	// Method is the subscribe/unsubscribe method name, for reporting.
	Method string
	// Topic is what a subscribe asks to be told about — "newHeads", "logs",
	// "logsSubscribe" — for per-topic reporting. Chains differ in how a
	// topic is named, so the classifier decides; "" reports as the method.
	Topic string
	// Periodic marks a subscription that must deliver on every block (or
	// slot) — newHeads, slotSubscribe, NewBlock — whatever the client
	// filters for. Only a periodic subscription gone silent is evidence that
	// the supplier stopped: a logs filter that matches nothing for minutes
	// is a quiet feed, not a dead one. See Heartbeat.
	Periodic bool
	// SubscriptionID is the raw id of the subscription an unsubscribe
	// targets, and SubscriptionIDSpan where it sits in the frame.
	SubscriptionID     string
	SubscriptionIDSpan Span
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

// EndpointFrameInfo is a plugin's reading of one endpoint→client frame. Ids
// are raw JSON, as in ClientFrameInfo.
type EndpointFrameInfo struct {
	Kind EndpointFrameKind
	// RequestID is the raw JSON-RPC id of the request a response answers.
	RequestID string
	// SubscriptionID is the raw id a subscribe response assigned, or the id
	// a notification is for; SubscriptionIDSpan is where a notification
	// carries it, so it can be rewritten after a rebind.
	SubscriptionID     string
	SubscriptionIDSpan Span
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

// WebSocketProber is implemented by plugins whose chain answers plain
// requests over its WebSocket endpoint. WebSocketProbe returns one cheap
// request frame (a JSON-RPC call with id 1) whose answer proves the
// supplier's WebSocket path works end to end: the relay miner's bridge and
// the backend's WebSocket port. The health-check executor only sends
// one-shot HTTP relays, so without this a WebSocket key that lost its
// traffic had no evidence to recover by.
type WebSocketProber interface {
	WebSocketProbe() []byte
}

// ReplayIDEncoder is optionally implemented by a classifier whose dialect
// does not write request ids as JSON strings. The default quotes.
type ReplayIDEncoder interface {
	EncodeReplayID(id string) string
}

// RequestIDLocator is optionally implemented by a classifier whose request
// id is not the JSON-RPC "id" member. The default is JSONRPCRequestIDSpan.
type RequestIDLocator interface {
	RequestIDSpan(data []byte) Span
}

// Subscription is one established subscription on a connection.
type Subscription struct {
	// ID is the subscription id as the CLIENT knows it: the one the first
	// supplier assigned. It never changes for the life of the connection.
	ID string
	// EndpointID is the id the CURRENT supplier knows it by. Equal to ID
	// until a rebind replays the subscription elsewhere.
	EndpointID string
	// Method is the method that opened it.
	Method string
	// Topic is what it is for; see ClientFrameInfo.Topic.
	Topic string
	// Periodic: see ClientFrameInfo.Periodic.
	Periodic bool
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
// from the frames that cross it, and translates those frames across a
// rebind. Pass every client frame through TranslateClientFrame and every
// endpoint frame through TranslateEndpointFrame; ask it what is live.
//
// It exists to unblock two things the bridge cannot do without it: a rebind
// (reconnect to a different supplier and replay the live subscriptions), and
// a stall watchdog (a subscription that has stopped delivering is only a
// stall if there IS a subscription). A nil classifier makes the registry
// inert: every frame is SubscriptionNone and nothing is ever active.
//
// Across a rebind the client keeps the subscription ids the first supplier
// assigned. ReplayFrames re-sends each live subscribe to the new supplier
// under a gateway-owned request id; the ack that comes back is consumed
// here (the client already had one) and its new subscription id is mapped
// to the client's; notifications are rewritten to the client's id on the
// way out and unsubscribes to the supplier's id on the way in.
type SubscriptionRegistry struct {
	classifier SubscriptionClassifier

	mu sync.Mutex
	// pending is request id → the subscribes awaiting their response, oldest
	// first. A list, not one entry: a client may reuse a request id, and a
	// second subscribe sent before the first is answered would otherwise
	// overwrite it, leaving one of the two subscriptions untracked — graded
	// unsolicited, and never replayed on a rebind.
	pending  map[string][]pendingSubscription
	npending int                     // total entries across pending, for the cap
	active   map[string]Subscription // client-facing subscription id → live subscription
	lastData time.Time
	// lastActivity is the later of the last notification and the last
	// subscribe ack (including a replay ack): what a stall watchdog measures
	// from, so a subscription just established is not stalled before its
	// first event could arrive.
	lastActivity time.Time
	// lastPeriodic is lastActivity counted over periodic subscriptions
	// only: what Heartbeat reports.
	lastPeriodic time.Time
	dropped      int // subscribe frames not tracked because a table was full

	// Rebind state. toClient maps the current supplier's id to the client's
	// where they differ; replay maps a gateway-owned replay request id to
	// the client's subscription id it will re-establish.
	toClient map[string]string
	replay   map[string]string
	replays  int

	// Notification grading. recent is keyed by the client-facing id; closed
	// holds endpoint ids unsubscribed recently; seed hashes frames.
	recent map[string]*dupRing
	closed map[string]struct{}
	seed   maphash.Seed
}

type pendingSubscription struct {
	method   string
	topic    string
	periodic bool
	request  []byte
}

// NotificationKind grades one endpoint→client notification against the
// subscriptions the connection actually holds.
//
// Every notification a supplier pushes is a relay it is paid for, and the
// client never asked for it frame by frame. So the frame itself is the only
// place padding can be seen: a supplier that repeats a notification, or
// pushes one for a subscription nobody opened, is claiming relays no client
// wanted.
type NotificationKind int

// The grades a notification can get.
const (
	// NotificationNone: not a notification, or one the registry cannot
	// judge (no classifier, or subscriptions past the tracking cap).
	NotificationNone NotificationKind = iota
	// NotificationOK is data for a subscription open on this connection.
	NotificationOK
	// NotificationDuplicate repeats, byte for byte, one of the last
	// dupWindow notifications for the same subscription from the same
	// supplier.
	NotificationDuplicate
	// NotificationUnsolicited names a subscription this connection never
	// opened with the current supplier.
	NotificationUnsolicited
)

// Notification is TranslateEndpointFrameNote's grade for one frame.
type Notification struct {
	Kind NotificationKind
	// Topic is the subscription's topic; "" when unknown (unsolicited).
	Topic string
	// Gap is how long ago the same notification was last seen on this
	// subscription, when it was inside the gap window; 0 otherwise. Wider
	// than the duplicate grade on purpose, see gapWindow.
	Gap time.Duration
}

// gapWindow and gapMaxAge bound how far back a repeat is looked for when
// measuring its gap. The duplicate grade looks at the last dupWindow (8)
// notifications only, which on a feed of 250 a second is about 30ms: a second
// node's copy of the same transaction half a second later is outside it, so
// every repeat the grade catches on a busy feed looks like an immediate
// resend. The gap needs the wider view to tell a merged mempool feed (a copy
// 0.1-5s later) from padding (an immediate or periodic resend).
const (
	gapWindow = 64
	gapMaxAge = 10 * time.Second
)

// gapRing remembers when a subscription's last gapWindow notifications were
// seen.
type gapRing struct {
	hashes [gapWindow]uint64
	at     [gapWindow]int64
	n      int
}

// since reports how long ago h was last seen within gapMaxAge, then records h
// at now.
func (g *gapRing) since(h uint64, now time.Time) time.Duration {
	var gap time.Duration
	cut := now.Add(-gapMaxAge).UnixNano()
	for i := 0; i < min(g.n, gapWindow); i++ {
		if g.hashes[i] == h && g.at[i] >= cut {
			if d := time.Duration(now.UnixNano() - g.at[i]); gap == 0 || d < gap {
				gap = d
			}
		}
	}
	g.hashes[g.n%gapWindow] = h
	g.at[g.n%gapWindow] = now.UnixNano()
	g.n++
	return gap
}

// dupWindow is how many recent notifications per subscription are
// remembered for duplicate detection. A duplicate that matters is an
// immediate repeat; eight covers a supplier that interleaves a few.
const dupWindow = 8

// dupRing remembers the hashes of a subscription's last dupWindow
// notifications.
type dupRing struct {
	hashes [dupWindow]uint64
	n      int
	gaps   gapRing
}

// seen reports whether h is in the ring, then records it.
func (d *dupRing) seen(h uint64) bool {
	for i := 0; i < min(d.n, dupWindow); i++ {
		if d.hashes[i] == h {
			return true
		}
	}
	d.hashes[d.n%dupWindow] = h
	d.n++
	return false
}

// maxClosedIDs bounds the ids remembered after an unsubscribe, so a
// notification already in flight when the client unsubscribed is not graded
// unsolicited. A wholesale clear past the cap costs, at worst, a late frame
// graded as none.
const maxClosedIDs = 64

// NewSubscriptionRegistry returns a registry driven by classifier. nil is
// allowed and yields an inert registry.
func NewSubscriptionRegistry(classifier SubscriptionClassifier) *SubscriptionRegistry {
	return &SubscriptionRegistry{
		classifier: classifier,
		pending:    make(map[string][]pendingSubscription),
		active:     make(map[string]Subscription),
		toClient:   make(map[string]string),
		replay:     make(map[string]string),
		recent:     make(map[string]*dupRing),
		closed:     make(map[string]struct{}),
		seed:       maphash.MakeSeed(),
	}
}

// TranslateClientFrame records what a client→endpoint frame does to the
// subscription state and returns the frame to forward: the input itself,
// or a copy with an unsubscribe's id rewritten to the current supplier's.
func (r *SubscriptionRegistry) TranslateClientFrame(data []byte) []byte {
	if r == nil || r.classifier == nil {
		return data
	}
	if isJSONArray(data) {
		// A batch: each member is tracked as if sent alone, so a subscribe
		// inside one is replayed and graded like any other. The frame goes
		// out as sent.
		// ponytail: an unsubscribe inside a batch is not rewritten to the
		// supplier's id after a rebind; rewrite per member if clients do it.
		gjson.ParseBytes(data).ForEach(func(_, member gjson.Result) bool {
			r.translateClientFrame([]byte(member.Raw))
			return true
		})
		return data
	}
	return r.translateClientFrame(data)
}

func (r *SubscriptionRegistry) translateClientFrame(data []byte) []byte {
	info := r.classifier.ClassifyClientFrame(data)
	if info.Action == SubscriptionNone {
		return data
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch info.Action {
	case SubscriptionSubscribe:
		if info.RequestID == "" {
			return data // A request with no id can never be matched to its answer.
		}
		if r.npending >= maxTrackedSubscriptions {
			r.dropped++
			return data
		}
		r.pending[info.RequestID] = append(r.pending[info.RequestID], pendingSubscription{
			method:   info.Method,
			topic:    info.Topic,
			periodic: info.Periodic,
			request:  append([]byte(nil), data...),
		})
		r.npending++
	case SubscriptionUnsubscribe:
		sub, ok := r.active[info.SubscriptionID]
		if !ok {
			return data
		}
		r.forget(sub)
		if sub.EndpointID != sub.ID {
			return spliceSpan(data, info.SubscriptionIDSpan, sub.EndpointID)
		}
	case SubscriptionUnsubscribeAll:
		for _, sub := range r.active {
			r.markClosed(sub.EndpointID)
		}
		clear(r.active)
		clear(r.toClient)
		clear(r.recent)
	}
	return data
}

// TranslateEndpointFrame records what an endpoint→client frame does and
// returns the frame to forward and whether to forward it at all. A replay
// ack is consumed: the client already has one. A notification for a
// subscription the current supplier knows by a different id is rewritten to
// the id the client knows.
func (r *SubscriptionRegistry) TranslateEndpointFrame(data []byte) (out []byte, forward bool) {
	out, forward, _ = r.TranslateEndpointFrameNote(data)
	return out, forward
}

// TranslateEndpointFrameNote is TranslateEndpointFrame that also grades a
// notification (see NotificationKind). The grade never changes what is
// forwarded: a duplicate or unsolicited frame still reaches the client,
// because deciding a supplier is padding is for whoever reads the counts,
// not for a per-frame check that might be wrong about one frame.
func (r *SubscriptionRegistry) TranslateEndpointFrameNote(data []byte) (out []byte, forward bool, note Notification) {
	if r == nil || r.classifier == nil {
		return data, true, note
	}
	if isJSONArray(data) {
		// A batch response: its members answer the members of a batch
		// request. Notifications are never batched, and a replay ack never
		// is either (replays go one frame each), so only subscribe acks are
		// read here.
		gjson.ParseBytes(data).ForEach(func(_, member gjson.Result) bool {
			r.translateEndpointFrame([]byte(member.Raw))
			return true
		})
		return data, true, note
	}
	return r.translateEndpointFrame(data)
}

func (r *SubscriptionRegistry) translateEndpointFrame(data []byte) (out []byte, forward bool, note Notification) {
	info := r.classifier.ClassifyEndpointFrame(data)
	if info.Kind == EndpointFrameOther {
		return data, true, note
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch info.Kind {
	case EndpointFrameResponse:
		if clientID, replayed := r.replay[info.RequestID]; replayed {
			delete(r.replay, info.RequestID)
			r.completeReplay(clientID, info)
			return nil, false, note
		}
		queue := r.pending[info.RequestID]
		if len(queue) == 0 {
			return data, true, note // A response to something that was not a subscribe.
		}
		p := queue[0]
		if len(queue) == 1 {
			delete(r.pending, info.RequestID)
		} else {
			r.pending[info.RequestID] = queue[1:]
		}
		r.npending--
		if info.IsError || info.SubscriptionID == "" {
			return data, true, note
		}
		if len(r.active) >= maxTrackedSubscriptions {
			r.dropped++
			return data, true, note
		}
		now := time.Now()
		r.active[info.SubscriptionID] = Subscription{
			ID:         info.SubscriptionID,
			EndpointID: info.SubscriptionID,
			Method:     p.method,
			Topic:      p.topic,
			Periodic:   p.periodic,
			Request:    p.request,
			Since:      now,
		}
		delete(r.closed, info.SubscriptionID)
		r.lastActivity = now
		if p.periodic {
			r.lastPeriodic = now
		}
		return data, true, note
	case EndpointFrameNotification:
		if clientID, mapped := r.toClient[info.SubscriptionID]; mapped {
			r.touch(clientID)
			return spliceSpan(data, info.SubscriptionIDSpan, clientID), true, r.grade(clientID, data)
		}
		if sub, live := r.active[info.SubscriptionID]; live && sub.EndpointID == sub.ID {
			r.touch(sub.ID)
			return data, true, r.grade(sub.ID, data)
		}
		if _, late := r.closed[info.SubscriptionID]; late || r.dropped > 0 {
			return data, true, note // In flight past an unsubscribe, or past the cap: not judged.
		}
		note.Kind = NotificationUnsolicited
	}
	return data, true, note
}

// touch records a notification for the live subscription clientID. Caller
// holds mu.
func (r *SubscriptionRegistry) touch(clientID string) {
	r.lastData = time.Now()
	r.lastActivity = r.lastData
	if r.active[clientID].Periodic {
		r.lastPeriodic = r.lastData
	}
}

// grade classifies a notification for the live subscription clientID.
// Caller holds mu.
func (r *SubscriptionRegistry) grade(clientID string, data []byte) Notification {
	note := Notification{Kind: NotificationOK, Topic: r.active[clientID].Topic}
	ring := r.recent[clientID]
	if ring == nil {
		ring = &dupRing{}
		r.recent[clientID] = ring
	}
	h := maphash.Bytes(r.seed, data)
	if ring.seen(h) {
		note.Kind = NotificationDuplicate
	}
	note.Gap = ring.gaps.since(h, time.Now())
	return note
}

// markClosed remembers an endpoint id that was just unsubscribed. Caller
// holds mu.
func (r *SubscriptionRegistry) markClosed(endpointID string) {
	if len(r.closed) >= maxClosedIDs {
		clear(r.closed)
	}
	r.closed[endpointID] = struct{}{}
}

// completeReplay applies a replay ack. Caller holds mu.
func (r *SubscriptionRegistry) completeReplay(clientID string, info EndpointFrameInfo) {
	sub, ok := r.active[clientID]
	if !ok {
		return // Unsubscribed while the replay was in flight.
	}
	if info.IsError || info.SubscriptionID == "" {
		r.forget(sub) // The new supplier refused it; the client will find out when data stops.
		return
	}
	if sub.EndpointID != sub.ID {
		delete(r.toClient, sub.EndpointID)
	}
	sub.EndpointID = info.SubscriptionID
	if sub.EndpointID != sub.ID {
		r.toClient[sub.EndpointID] = sub.ID
	}
	r.active[clientID] = sub
	// A new supplier: its first notification may legitimately repeat the old
	// one's last (and on chains with small integer ids, byte for byte).
	delete(r.recent, clientID)
	r.lastActivity = time.Now()
	if sub.Periodic {
		r.lastPeriodic = r.lastActivity
	}
}

// forget drops one subscription and its id mapping. Caller holds mu.
func (r *SubscriptionRegistry) forget(sub Subscription) {
	delete(r.active, sub.ID)
	delete(r.recent, sub.ID)
	r.markClosed(sub.EndpointID)
	if sub.EndpointID != sub.ID {
		delete(r.toClient, sub.EndpointID)
	}
}

// ReplayFrames returns the subscribe frames to send to a new supplier — one
// per live subscription, the client's original request with a fresh
// gateway-owned request id — and arms the registry to consume their acks.
// Frames the registry cannot re-id (no request-id span) are skipped: replaying
// them would produce an ack the client would see twice.
func (r *SubscriptionRegistry) ReplayFrames() [][]byte {
	if r == nil || r.classifier == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	for clientID, sub := range r.active {
		span := r.requestIDSpan(sub.Request)
		if span.Len == 0 {
			continue
		}
		r.replays++
		raw := r.encodeReplayID(fmt.Sprintf("sage-replay-%d", r.replays))
		r.replay[raw] = clientID
		out = append(out, spliceSpan(sub.Request, span, raw))
	}
	return out
}

func (r *SubscriptionRegistry) requestIDSpan(data []byte) Span {
	if l, ok := r.classifier.(RequestIDLocator); ok {
		return l.RequestIDSpan(data)
	}
	return JSONRPCRequestIDSpan(data)
}

func (r *SubscriptionRegistry) encodeReplayID(id string) string {
	if e, ok := r.classifier.(ReplayIDEncoder); ok {
		return e.EncodeReplayID(id)
	}
	return `"` + id + `"`
}

// spliceSpan returns a copy of data with span replaced by raw. An unknown
// span returns data unchanged: a frame the registry cannot rewrite safely
// is forwarded as it came.
func spliceSpan(data []byte, span Span, raw string) []byte {
	if span.Len <= 0 || span.Index < 0 || span.Index+span.Len > len(data) {
		return data
	}
	out := make([]byte, 0, len(data)-span.Len+len(raw))
	out = append(out, data[:span.Index]...)
	out = append(out, raw...)
	out = append(out, data[span.Index+span.Len:]...)
	return out
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

// LastActivity is the later of the last notification for a live
// subscription and the last subscribe acknowledgement; zero if neither has
// happened. A stall watchdog measures silence from here.
func (r *SubscriptionRegistry) LastActivity() time.Time {
	if r == nil {
		return time.Time{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastActivity
}

// Heartbeat reports whether the connection holds a periodic subscription and,
// if so, when one last showed activity (a notification, or the subscribe or
// replay ack that opened it). It is what a stall watchdog judges: a periodic
// feed silent for longer than any block time means the supplier stopped
// delivering. A connection holding only filtered feeds has no heartbeat and
// can never be judged stalled — its silence is the filter's, not the
// supplier's.
func (r *SubscriptionRegistry) Heartbeat() (periodic bool, last time.Time) {
	if r == nil {
		return false, time.Time{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sub := range r.active {
		if sub.Periodic {
			return true, r.lastPeriodic
		}
	}
	return false, time.Time{}
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

// JSONRPCRequestIDSpan locates a frame's "id" value, for rewriting.
func JSONRPCRequestIDSpan(data []byte) Span {
	return SpanOf(gjson.GetBytes(data, "id"))
}

// JSONRPCResultScalar returns a frame's raw "result" when it is a string or
// a number — the two shapes a subscription id takes — or "".
func JSONRPCResultScalar(data []byte) string {
	return rawScalar(gjson.GetBytes(data, "result"))
}

// JSONRPCFirstParam returns raw params[0] when it is a string or a number,
// or "", and where it sits.
func JSONRPCFirstParam(data []byte) (string, Span) {
	p := gjson.GetBytes(data, "params.0")
	return rawScalar(p), SpanOf(p)
}

// JSONRPCPath returns the raw scalar at path and where it sits, or "" and a
// zero span.
func JSONRPCPath(data []byte, path string) (string, Span) {
	p := gjson.GetBytes(data, path)
	return rawScalar(p), SpanOf(p)
}

// SpanOf is where a gjson result sits in the frame it was read from.
func SpanOf(res gjson.Result) Span {
	if !res.Exists() || res.Index <= 0 && res.Raw == "" {
		return Span{}
	}
	return Span{Index: res.Index, Len: len(res.Raw)}
}

func rawScalar(res gjson.Result) string {
	switch res.Type {
	case gjson.String, gjson.Number:
		return res.Raw
	}
	return ""
}

// isJSONArray reports whether a frame is a JSON array: a batch.
func isJSONArray(data []byte) bool {
	for _, c := range data {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		case '[':
			return true
		}
		return false
	}
	return false
}
