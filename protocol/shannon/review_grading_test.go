package shannon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/heuristic"
)

// Review of 5fd0d96..c3a3a7a: grading changes that can hide a broken
// supplier. Each test asserts the invariant; on c3a3a7a they fail.

// newRefusingSupplier is a relay miner's WebSocket that upgrades, reads the
// first frame and closes with code and text, as the HA miner does on a frame
// it will not serve.
func newRefusingSupplier(t *testing.T, code int, text string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		up := websocket.Upgrader{}
		conn, err := up.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
		_, _, _ = conn.ReadMessage() // the client's close ack
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A refused probe returns before both probeBackoff.record and RecordSignal
// (ws_probe.go probeEndpoint). A demoted key whose miner refuses every probe
// (an allocation spent, 4002, or a session the miner never accepts) is
// therefore probed at every cycle, every minute,
// forever: each probe is two paid relays, the cost wsProbeMaxBackoff exists
// to cap. Targets sort lowest score first and are capped at
// wsProbeMaxPerCycle, so 32 such keys, which never move, take every slot and
// starve the probes of keys that could recover.
func TestReview_RefusedProbeIsNeverBackedOff(t *testing.T) {
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	supplier := newRefusingSupplier(t, heuristic.CloseMinerStakeLimit, "stake limit exceeded")
	r, rep, m := probeFixture(t, wsURL(supplier), 40, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, enabled)

	for i := 0; i < 3; i++ {
		r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
	}
	m.mu.Lock()
	probes := append([]string(nil), m.probes...)
	m.mu.Unlock()
	if len(probes) == 0 || probes[0] != wsProbeRefused {
		t.Fatalf("precondition: probes = %v, want the first one %q", probes, wsProbeRefused)
	}
	if len(probes) > 1 {
		t.Fatalf("a URL refusing every probe was probed %d times in three back-to-back cycles (%v) with %d signals: "+
			"no backoff and no grade, so it is re-probed every minute for as long as it stays below full score",
			len(probes), probes, len(rep.calls))
	}
}
