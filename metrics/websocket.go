package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/sage/domain"
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
// Every label is a closed set: service_id is bounded by the configured
// services (as everywhere else), direction and side are the two ends of a
// bridge, initiator is the three parties that can end one, reason is the
// gateway's own refusal reasons, and code is folded by closeCodeLabel.
type WebSocketMetrics struct {
	services     *labelPolicy
	connections  *prometheus.GaugeVec
	frames       *prometheus.CounterVec
	bytes        *prometheus.CounterVec
	closes       *prometheus.CounterVec
	unresponsive *prometheus.CounterVec
	rejected     *prometheus.CounterVec
	rebinds      *prometheus.CounterVec
	stalls       *prometheus.CounterVec
}

// NewWebSocketMetrics builds and registers the WebSocket metrics on the
// default registry. knownServices bounds the service_id label.
func NewWebSocketMetrics(knownServices []domain.ServiceID) *WebSocketMetrics {
	m := newWebSocketMetrics(knownServices)
	prometheus.MustRegister(m.connections, m.frames, m.bytes, m.closes, m.unresponsive, m.rejected, m.rebinds, m.stalls)
	return m
}

func newWebSocketMetrics(knownServices []domain.ServiceID) *WebSocketMetrics {
	return &WebSocketMetrics{
		services: allowedLabel(knownServices),
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
				Help:      "WebSocket bridges ended, by service, who ended it (client, endpoint, or gateway — a deadline, a processing error, a shutdown) and the close code sent to the client (1000–1015 verbatim, 3000–3999 as \"registered\", 4000–4999 as \"application\", anything else \"other\").",
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
