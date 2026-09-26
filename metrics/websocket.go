package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/websockets"
)

// WebSocketMetrics exposes what the WebSocket bridges do. Until it existed the
// WS path had no metrics at all: a gateway could hold a thousand dead sockets,
// or none, and the dashboards would look the same.
//
//	sage_websocket_connections{service_id}                 live bridges
//	sage_websocket_frames_total{service_id,direction}      data frames routed
//	sage_websocket_bytes_total{service_id,direction}       payload bytes routed
//	sage_websocket_closes_total{service_id,initiator,code} bridges ended, by who and the client-facing code
//	sage_websocket_unresponsive_total{service_id,side}     liveness timeouts, by the silent side
//	sage_websocket_rejected_total{service_id,reason}       upgrades refused before a bridge existed
//	sage_websocket_rebinds_total{service_id,result}        lost suppliers replaced under a live client
//	sage_websocket_stalls_total{service_id}                subscriptions with no data for the stall timeout
//
// and, per supplier, keyed by operator (the endpoint's registrable domain) and
// owner (its on-chain owner address):
//
//	sage_websocket_supplier_frames_total{service_id,operator,owner,direction}
//	sage_websocket_supplier_notifications_total{service_id,operator,owner,topic,grade}
//	sage_websocket_supplier_connections{service_id,operator,owner}
//	sage_websocket_supplier_tenure_seconds{service_id,operator,owner}
//
// The per-service series cannot say who is paid for the traffic: every frame
// a supplier pushes is a relay it claims, so a supplier can earn out of all
// proportion to its stake by holding long connections and pushing more
// notifications than its peers. The supplier series are what that is judged
// from, owner included because it is the one identity an operator cannot
// rotate.
//
// Every label is a closed set: service_id is bounded by the configured
// services (as everywhere else), direction and side are the two ends of a
// bridge, initiator is the three parties that can end one, reason is the
// gateway's own refusal reasons, and code is folded by closeCodeLabel.
// operator, owner and topic come from the chain and from clients, so each is
// capped first-seen.
type WebSocketMetrics struct {
	services     *labelPolicy
	operators    *labelPolicy
	owners       *labelPolicy
	topics       *labelPolicy
	connections  *prometheus.GaugeVec
	frames       *prometheus.CounterVec
	bytes        *prometheus.CounterVec
	closes       *prometheus.CounterVec
	unresponsive *prometheus.CounterVec
	rejected     *prometheus.CounterVec
	rebinds      *prometheus.CounterVec
	stalls       *prometheus.CounterVec

	supplierFrames        *prometheus.CounterVec
	supplierNotifications *prometheus.CounterVec
	supplierConnections   *prometheus.GaugeVec
	supplierTenure        *prometheus.HistogramVec
	probes                *prometheus.CounterVec
}

// Caps for the supplier labels. Operators serving WebSocket number in the
// tens and owners a little more; the caps are headroom. Topics are chosen by
// clients (an EVM subscribe names any string it likes), so that cap is a
// defence, not an estimate.
const (
	maxWSOperatorLabels = 64
	maxWSOwnerLabels    = 128
	maxWSTopicLabels    = 32
)

// NewWebSocketMetrics builds and registers the WebSocket metrics on the
// default registry. knownServices bounds the service_id label.
func NewWebSocketMetrics(knownServices []domain.ServiceID) *WebSocketMetrics {
	m := newWebSocketMetrics(knownServices)
	prometheus.MustRegister(m.connections, m.frames, m.bytes, m.closes, m.unresponsive, m.rejected, m.rebinds, m.stalls,
		m.supplierFrames, m.supplierNotifications, m.supplierConnections, m.supplierTenure, m.probes)
	return m
}

func newWebSocketMetrics(knownServices []domain.ServiceID) *WebSocketMetrics {
	supplierLabels := []string{"service_id", "operator", "owner"}
	return &WebSocketMetrics{
		services:  allowedLabel(knownServices),
		operators: cappedLabel(maxWSOperatorLabels),
		owners:    cappedLabel(maxWSOwnerLabels),
		topics:    cappedLabel(maxWSTopicLabels),
		probes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_probes_total",
				Help:      "WebSocket recovery probes, by service and result: ok, other_dialect (answered -32601 method not found: alive but serving another API on that socket, e.g. a Cosmos EVM chain's EVM surface; graded ok), dial_failed (the upgrade was refused or never completed), no_answer (connected, no valid answer in time), invalid (the answer failed relay validation), error_response (a valid relay carrying a JSON-RPC error or no result), unresolved (nothing to sign with; not graded). Probes go only to WebSocket endpoints below full reputation, which a connection-only signal gave no way back.",
			},
			[]string{"service_id", "result"},
		),
		supplierFrames: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_supplier_frames_total",
				Help:      "Data frames exchanged with a supplier over WebSocket, by service, operator (the endpoint's registrable domain), owner (its on-chain owner address) and direction. Each is a relay the supplier can claim: client_to_endpoint frames are the requests SAGE signed, endpoint_to_client frames the responses and notifications the supplier signed. Frames that failed validation are not counted.",
			},
			append(append([]string(nil), supplierLabels...), "direction"),
		),
		supplierNotifications: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_supplier_notifications_total",
				Help:      "Subscription notifications a supplier pushed, by service, operator, owner, topic (what the subscription is for: newHeads, logs, slotSubscribe, …) and grade: ok (for a subscription open on the connection), duplicate (byte-for-byte repeat of one of the subscription's last 8 notifications from that supplier), unsolicited (for a subscription never opened with that supplier). Every notification is a paid relay no client asked for frame by frame; duplicate and unsolicited ones are padding. Graded, never dropped: the client still receives them.",
			},
			append(append([]string(nil), supplierLabels...), "topic", "grade"),
		),
		supplierConnections: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "sage",
				Name:      "websocket_supplier_connections",
				Help:      "Client WebSocket connections a supplier is currently serving, by service, operator and owner.",
			},
			supplierLabels,
		),
		supplierTenure: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "sage",
				Name:      "websocket_supplier_tenure_seconds",
				Help:      "How long a supplier served one client WebSocket connection, from bind to rebind or close, by service, operator and owner. The count is connections served. A supplier whose tenures run long relative to its peers on the same service holds more of the frame volume than its selection share explains.",
				Buckets:   []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200},
			},
			supplierLabels,
		),
		connections: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "sage",
				Name:      "websocket_connections",
				Help:      "Live WebSocket bridges (a client connection plus its supplier connection) by service.",
			},
			[]string{"service_id"},
		),
		frames: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_frames_total",
				Help:      "Data frames routed through WebSocket bridges, by service and direction (client_to_endpoint or endpoint_to_client). Control frames (ping, pong, close) are not counted.",
			},
			[]string{"service_id", "direction"},
		),
		bytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_bytes_total",
				Help:      "Payload bytes routed through WebSocket bridges, by service and direction, measured after processing (the bytes written to the receiving side).",
			},
			[]string{"service_id", "direction"},
		),
		closes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_closes_total",
				Help:      "WebSocket bridges ended, by service, who ended it (client, endpoint, or gateway — a deadline, a processing error, a shutdown) and the close code as determined, before wire sanitisation (1000–1015 verbatim, 3000–3999 as \"registered\", 4000–4999 as \"application\", anything else \"other\"). A 1006 here is a peer that vanished without a close handshake — the wire carried a 1011 in its place, but counting it as 1011 would dress client churn up as server errors.",
			},
			[]string{"service_id", "initiator", "code"},
		),
		unresponsive: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_unresponsive_total",
				Help:      "WebSocket bridges closed because one side sent nothing — no data, no pong — for a whole pong wait, by service and the silent side (client or endpoint). An endpoint count is a supplier that went away under a live socket.",
			},
			[]string{"service_id", "side"},
		),
		rebinds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_rebinds_total",
				Help:      "Attempts to replace a lost supplier under a live client connection, by service and result: ok (a new supplier took over and the live subscriptions were replayed), failed (no supplier could be reached; the client was told to reconnect), exhausted (the per-connection rebind limit was already spent).",
			},
			[]string{"service_id", "result"},
		),
		stalls: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_stalls_total",
				Help:      "Times the stall watchdog fired on a WebSocket bridge, by service: the client held established subscriptions and the supplier delivered nothing for them for the stall timeout, under a socket that still answered pings. Each one is followed by a rebind attempt (or a 1012 without one).",
			},
			[]string{"service_id"},
		),
		rejected: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "sage",
				Name:      "websocket_rejected_total",
				Help:      "WebSocket upgrades the gateway refused before opening a bridge, by service and reason (capacity: the max_concurrent_connections cap was reached).",
			},
			[]string{"service_id", "reason"},
		),
	}
}

// Rejected counts an upgrade refused before a bridge existed.
func (m *WebSocketMetrics) Rejected(serviceID domain.ServiceID, reason string) {
	m.rejected.WithLabelValues(m.services.serviceValue(serviceID), reason).Inc()
}

// ForService returns the per-bridge websockets.Observer for one service.
func (m *WebSocketMetrics) ForService(serviceID domain.ServiceID) websockets.Observer {
	return &webSocketServiceObserver{m: m, sid: m.services.serviceValue(serviceID)}
}

// webSocketServiceObserver records one service's bridge events.
type webSocketServiceObserver struct {
	m   *WebSocketMetrics
	sid string
}

var _ websockets.Observer = (*webSocketServiceObserver)(nil)

// Opened counts a bridge that connected both sides.
func (o *webSocketServiceObserver) Opened() {
	o.m.connections.WithLabelValues(o.sid).Inc()
}

// Frame counts one routed data frame and its bytes.
func (o *webSocketServiceObserver) Frame(source websockets.MessageSource, bytes int) {
	dir := directionLabel(source)
	o.m.frames.WithLabelValues(o.sid, dir).Inc()
	o.m.bytes.WithLabelValues(o.sid, dir).Add(float64(bytes))
}

// Unresponsive counts a liveness timeout on one side.
func (o *webSocketServiceObserver) Unresponsive(source websockets.MessageSource) {
	o.m.unresponsive.WithLabelValues(o.sid, source.String()).Inc()
}

// Closed counts the end of a bridge and releases its connection slot.
func (o *webSocketServiceObserver) Closed(initiator websockets.CloseInitiator, code int) {
	o.m.connections.WithLabelValues(o.sid).Dec()
	o.m.closes.WithLabelValues(o.sid, string(initiator), closeCodeLabel(code)).Inc()
}

func directionLabel(source websockets.MessageSource) string {
	if source == websockets.SourceClient {
		return "client_to_endpoint"
	}
	return "endpoint_to_client"
}

// closeCodeLabel folds a close code into a bounded label: the protocol codes
// verbatim, the two application ranges by name, and one bucket for anything
// a peer invents.
func closeCodeLabel(code int) string {
	switch {
	case code >= 1000 && code <= 1015:
		return strconv.Itoa(code)
	case code >= 3000 && code <= 3999:
		return "registered"
	case code >= 4000 && code <= 4999:
		return "application"
	}
	return "other"
}

// Rebound counts one attempt to replace a lost endpoint.
func (o *webSocketServiceObserver) Rebound(result websockets.RebindResult) {
	o.m.rebinds.WithLabelValues(o.sid, string(result)).Inc()
}

// Stalled counts one stall-watchdog verdict.
func (o *webSocketServiceObserver) Stalled() {
	o.m.stalls.WithLabelValues(o.sid).Inc()
}

// supplierValues resolves the capped supplier label values.
func (m *WebSocketMetrics) supplierValues(serviceID domain.ServiceID, operator, owner string) (string, string, string) {
	if owner == "" {
		owner = unknownLabel
	}
	return m.services.serviceValue(serviceID), m.operators.value(operator), m.owners.value(owner)
}

// SupplierBound counts a supplier taking over a client connection: at open,
// or at a rebind onto it.
func (m *WebSocketMetrics) SupplierBound(serviceID domain.ServiceID, operator, owner string) {
	sid, op, own := m.supplierValues(serviceID, operator, owner)
	m.supplierConnections.WithLabelValues(sid, op, own).Inc()
}

// SupplierReleased records the end of a supplier's tenure on a client
// connection: a rebind away from it, or the close.
func (m *WebSocketMetrics) SupplierReleased(serviceID domain.ServiceID, operator, owner string, tenure time.Duration) {
	sid, op, own := m.supplierValues(serviceID, operator, owner)
	m.supplierConnections.WithLabelValues(sid, op, own).Dec()
	m.supplierTenure.WithLabelValues(sid, op, own).Observe(tenure.Seconds())
}

// SupplierFrame counts one data frame exchanged with a supplier.
func (m *WebSocketMetrics) SupplierFrame(serviceID domain.ServiceID, operator, owner string, source websockets.MessageSource) {
	sid, op, own := m.supplierValues(serviceID, operator, owner)
	m.supplierFrames.WithLabelValues(sid, op, own, directionLabel(source)).Inc()
}

// SupplierNotification counts one graded notification. NotificationNone is
// not recorded.
func (m *WebSocketMetrics) SupplierNotification(serviceID domain.ServiceID, operator, owner string, note qos.Notification) {
	grade := notificationGrade(note.Kind)
	if grade == "" {
		return
	}
	topic := note.Topic
	if topic == "" {
		topic = unknownLabel
	}
	sid, op, own := m.supplierValues(serviceID, operator, owner)
	m.supplierNotifications.WithLabelValues(sid, op, own, m.topics.value(topic), grade).Inc()
}

func notificationGrade(k qos.NotificationKind) string {
	switch k {
	case qos.NotificationOK:
		return "ok"
	case qos.NotificationDuplicate:
		return "duplicate"
	case qos.NotificationUnsolicited:
		return "unsolicited"
	}
	return ""
}

// Probed counts one WebSocket recovery probe.
func (m *WebSocketMetrics) Probed(serviceID domain.ServiceID, result string) {
	m.probes.WithLabelValues(m.services.serviceValue(serviceID), result).Inc()
}

// NewWebSocketShoppingGauge exposes how many WebSocket clients the relayer's
// client ledger currently flags as shopping for a supplier — reconnecting
// until it lands on one owner, then holding that connection:
//
//	sage_websocket_shopping_clients <count>
//
// A client address cannot be a label; which clients and which owner is in
// GET /admin/websocket/clients?shopping=true.
func NewWebSocketShoppingGauge(count func() int) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "sage",
			Name:      "websocket_shopping_clients",
			Help:      "WebSocket clients this replica flags as shopping for a supplier over the last one to two hours: at least 5 tenures with other owners closed by the client within 30s, and at least 80% (and 10 minutes) of its connected time with one owner. Every frame is a relay that owner is paid for; which clients and which owner are in GET /admin/websocket/clients?shopping=true.",
		},
		func() float64 { return float64(count()) },
	)
}
