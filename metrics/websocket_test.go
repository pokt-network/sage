package metrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/websockets"
)

// value reads a counter or gauge without prometheus/testutil, which would add
// a dependency for one helper.
func value(t *testing.T, m prometheus.Metric) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	if out.Counter != nil {
		return out.Counter.GetValue()
	}
	return out.Gauge.GetValue()
}

func newIsolatedWebSocketMetrics(t *testing.T) (*WebSocketMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := newWebSocketMetrics([]domain.ServiceID{"eth"})
	reg.MustRegister(m.connections, m.frames, m.bytes, m.closes, m.unresponsive, m.rejected, m.rebinds, m.stalls)
	return m, reg
}

// One bridge's life, as the gauges and counters should tell it.
func TestWebSocketMetrics_BridgeLifecycle(t *testing.T) {
	m, _ := newIsolatedWebSocketMetrics(t)
	o := m.ForService("eth")

	o.Opened()
	if got := value(t, m.connections.WithLabelValues("eth")); got != 1 {
		t.Fatalf("connections after open = %v, want 1", got)
	}
	o.Frame(websockets.SourceClient, 10)
	o.Frame(websockets.SourceEndpoint, 300)
	o.Frame(websockets.SourceEndpoint, 5)
	if got := value(t, m.frames.WithLabelValues("eth", "client_to_endpoint")); got != 1 {
		t.Errorf("client→endpoint frames = %v, want 1", got)
	}
	if got := value(t, m.frames.WithLabelValues("eth", "endpoint_to_client")); got != 2 {
		t.Errorf("endpoint→client frames = %v, want 2", got)
	}
	if got := value(t, m.bytes.WithLabelValues("eth", "endpoint_to_client")); got != 305 {
		t.Errorf("endpoint→client bytes = %v, want 305", got)
	}
	o.Unresponsive(websockets.SourceEndpoint)
	o.Closed(websockets.InitiatorGateway, 1012)
	if got := value(t, m.connections.WithLabelValues("eth")); got != 0 {
		t.Errorf("connections after close = %v, want 0", got)
	}
	if got := value(t, m.unresponsive.WithLabelValues("eth", "endpoint")); got != 1 {
		t.Errorf("unresponsive endpoint = %v, want 1", got)
	}
	if got := value(t, m.closes.WithLabelValues("eth", "gateway", "1012")); got != 1 {
		t.Errorf("closes{gateway,1012} = %v, want 1", got)
	}
}

// A peer can send any close code it likes; the label must not follow it.
func TestWebSocketMetrics_CloseCodeLabelIsBounded(t *testing.T) {
	for code, want := range map[int]string{
		1000: "1000", 1012: "1012", 1015: "1015",
		3001: "registered", 4000: "application", 4999: "application",
		999: "other", 1016: "other", 5000: "other", 65535: "other",
	} {
		if got := closeCodeLabel(code); got != want {
			t.Errorf("closeCodeLabel(%d) = %q, want %q", code, got, want)
		}
	}
}

// An unknown service must not mint a label either.
func TestWebSocketMetrics_UnknownServiceCollapses(t *testing.T) {
	m, _ := newIsolatedWebSocketMetrics(t)
	m.Rejected("not-configured", "capacity")
	if got := value(t, m.rejected.WithLabelValues(unknownLabel, "capacity")); got != 1 {
		t.Errorf("rejected{unknown,capacity} = %v, want 1", got)
	}
}

// A supplier's tenure and traffic, as the supplier series should tell it.
func TestWebSocketMetrics_SupplierAccounting(t *testing.T) {
	m, _ := newIsolatedWebSocketMetrics(t)
	const op, owner = "op-alpha.example", "pokt1owner"

	m.SupplierBound("eth", op, owner)
	if got := value(t, m.supplierConnections.WithLabelValues("eth", op, owner)); got != 1 {
		t.Fatalf("supplier connections after bind = %v, want 1", got)
	}
	m.SupplierFrame("eth", op, owner, websockets.SourceClient)
	m.SupplierFrame("eth", op, owner, websockets.SourceEndpoint)
	m.SupplierFrame("eth", op, owner, websockets.SourceEndpoint)
	if got := value(t, m.supplierFrames.WithLabelValues("eth", op, owner, "endpoint_to_client")); got != 2 {
		t.Errorf("supplier endpoint→client frames = %v, want 2", got)
	}
	m.SupplierNotification("eth", op, owner, qos.Notification{Kind: qos.NotificationDuplicate, Topic: "newHeads"})
	m.SupplierNotification("eth", op, owner, qos.Notification{Kind: qos.NotificationUnsolicited})
	m.SupplierNotification("eth", op, owner, qos.Notification{Kind: qos.NotificationNone, Topic: "newHeads"})
	if got := value(t, m.supplierNotifications.WithLabelValues("eth", op, owner, "newHeads", "duplicate")); got != 1 {
		t.Errorf("duplicate newHeads = %v, want 1", got)
	}
	if got := value(t, m.supplierNotifications.WithLabelValues("eth", op, owner, unknownLabel, "unsolicited")); got != 1 {
		t.Errorf("unsolicited = %v, want 1 under the unknown topic", got)
	}
	m.SupplierReleased("eth", op, owner, 90*time.Second)
	if got := value(t, m.supplierConnections.WithLabelValues("eth", op, owner)); got != 0 {
		t.Errorf("supplier connections after release = %v, want 0", got)
	}
	var hist dto.Metric
	if err := m.supplierTenure.WithLabelValues("eth", op, owner).(prometheus.Metric).Write(&hist); err != nil {
		t.Fatal(err)
	}
	if hist.Histogram.GetSampleCount() != 1 || hist.Histogram.GetSampleSum() != 90 {
		t.Errorf("tenure histogram count=%d sum=%v, want 1 and 90", hist.Histogram.GetSampleCount(), hist.Histogram.GetSampleSum())
	}
}

// Topics are named by clients: an EVM subscribe can carry any string. The
// label must not follow them past the cap.
func TestWebSocketMetrics_SupplierTopicIsBounded(t *testing.T) {
	m, _ := newIsolatedWebSocketMetrics(t)
	for i := 0; i < maxWSTopicLabels+20; i++ {
		m.SupplierNotification("eth", "a.example", "pokt1a", qos.Notification{Kind: qos.NotificationOK, Topic: fmt.Sprintf("t%d", i)})
	}
	if got := value(t, m.supplierNotifications.WithLabelValues("eth", "a.example", "pokt1a", otherLabel, "ok")); got != 20 {
		t.Errorf("topics past the cap = %v, want 20 folded into %s", got, otherLabel)
	}
}

func TestWebSocketMetrics_Probed(t *testing.T) {
	m, _ := newIsolatedWebSocketMetrics(t)
	m.Probed("eth", "ok")
	m.Probed("eth", "dial_failed")
	m.Probed("unconfigured", "ok")
	if got := value(t, m.probes.WithLabelValues("eth", "ok")); got != 1 {
		t.Errorf("probes{eth,ok} = %v, want 1", got)
	}
	if got := value(t, m.probes.WithLabelValues(unknownLabel, "ok")); got != 1 {
		t.Errorf("an unconfigured service must fold into %s, got %v", unknownLabel, got)
	}
}
