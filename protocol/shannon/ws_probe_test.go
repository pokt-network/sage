package shannon

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/reputation"
)

// scoredRepSvc is spyRepSvc with a chosen score for every key.
type scoredRepSvc struct {
	*spyRepSvc
	score float64
}

func (s scoredRepSvc) GetScore(context.Context, domain.ServiceID, domain.EndpointAddr, domain.RPCType) (float64, error) {
	return s.score, nil
}

// probeFixture is a relayer whose one "eth" supplier serves WebSocket at
// supplierURL, scored at score, answering every probe with answer (the echo
// supplier returns the frame; the mock full node's validation turns it into
// answer).
func probeFixture(t *testing.T, supplierURL string, score float64, answer string, flags map[string]bool) (*WSRelayer, *spyRepSvc, *spyWSMetrics) {
	t.Helper()
	session := &sessiontypes.Session{
		SessionId: "probe-session",
		Header: &sessiontypes.SessionHeader{
			SessionId: "probe-session", ServiceId: "eth", ApplicationAddress: "pokt1app",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers: []*sharedtypes.Supplier{{
			OperatorAddress: "pokt1supplier",
			OwnerAddress:    "pokt1owner",
			Services: []*sharedtypes.SupplierServiceConfig{{
				ServiceId: "eth",
				Endpoints: []*sharedtypes.SupplierEndpoint{{Url: supplierURL, RpcType: sharedtypes.RPCType_WEBSOCKET}},
			}},
		}},
	}
	fn := &mockRelayFullNode{
		session:          session,
		app:              &apptypes.Application{Address: "pokt1app"},
		validateResponse: &servicetypes.RelayResponse{Payload: []byte(answer)},
	}
	p := &Protocol{
		fullNode:  fn,
		sessions:  newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer:    &mockSigner{},
		bl:        newBlacklist(),
		ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		metrics:   noopSupplierMetrics{},
		logger:    newTestLogger(),
	}
	reg := qos.NewRegistry()
	if err := reg.Register("eth", &evm.Plugin{}); err != nil {
		t.Fatal(err)
	}
	spy := &spyRepSvc{}
	m := &spyWSMetrics{}
	r := NewWSRelayer(WSRelayerDeps{
		Protocol: p, Reputation: scoredRepSvc{spy, score}, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(flags), Logger: newTestLogger(), Metrics: m, QoS: reg,
	})
	return r, spy, m
}

func wsURL(srv *httptest.Server) string { return "ws" + strings.TrimPrefix(srv.URL, "http") }

func TestWSProbe_GradesADemotedKey(t *testing.T) {
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	supplier := newEchoSupplier(t)

	for _, tc := range []struct {
		name   string
		answer string
		want   string
		signal reputation.SignalType
	}{
		{"a real answer recovers", `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, wsProbeOK, reputation.SignalSuccess},
		{"a JSON-RPC error is a failure", `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`, wsProbeErrorResponse, reputation.SignalMajorError},
		// sei stakes its EVM surface as WebSocket; the cosmos probe's status
		// call gets method-not-found from every healthy supplier there.
		{"method not found is a live backend", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method status does not exist/is not available"}}`, wsProbeOtherDialect, reputation.SignalSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, rep, m := probeFixture(t, wsURL(supplier), 40, tc.answer, enabled)
			r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
			if len(m.probes) != 1 || m.probes[0] != tc.want {
				t.Fatalf("probe results = %v, want [%s]", m.probes, tc.want)
			}
			if len(rep.calls) != 1 || rep.calls[0].signal.Type != tc.signal || !rep.calls[0].signal.Probe {
				t.Fatalf("signals = %+v, want one %s marked as a probe", rep.calls, tc.signal)
			}
		})
	}

	t.Run("an endpoint that does not upgrade fails", func(t *testing.T) {
		dead := httptest.NewServer(nil) // answers 404 to the upgrade
		t.Cleanup(dead.Close)
		r, rep, m := probeFixture(t, wsURL(dead), 40, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, enabled)
		r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
		if len(m.probes) != 1 || m.probes[0] != wsProbeDialFailed || rep.calls[0].signal.Type != reputation.SignalMajorError {
			t.Fatalf("probes=%v signals=%+v, want one dial_failed major", m.probes, rep.calls)
		}
	})
}

// Probes are for keys nothing else can grade: a key at full score is left
// alone, and so is every key when the flag is off.
func TestWSProbe_SkipsFullScoreAndFlagOff(t *testing.T) {
	supplier := newEchoSupplier(t)
	answer := `{"jsonrpc":"2.0","id":1,"result":"0x10"}`

	r, rep, m := probeFixture(t, wsURL(supplier), 100, answer,
		map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true})
	r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
	if len(m.probes) != 0 || len(rep.calls) != 0 {
		t.Errorf("full score: probes=%v signals=%d, want none", m.probes, len(rep.calls))
	}

	r, rep, m = probeFixture(t, wsURL(supplier), 40, answer,
		map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: false})
	r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
	if len(m.probes) != 0 || len(rep.calls) != 0 {
		t.Errorf("flag off: probes=%v signals=%d, want none", m.probes, len(rep.calls))
	}
}

// A URL that keeps failing is probed less and less often, up to the cap, and
// at the normal cadence again once it answers. On mainnet (2026-09-26) the
// same dead URLs were redialled every minute, ~1,600 paid dial failures an
// hour.
func TestWSProbeBackoff_DoublesToTheCapAndResetsOnSuccess(t *testing.T) {
	var b wsProbeBackoff
	now := time.Unix(0, 0)
	const key = "eth|wss://dead.test"
	if !b.due(key, now) {
		t.Fatal("a URL never probed must be due")
	}
	var waits []time.Duration
	for i := 0; i < 8; i++ {
		b.record(key, true, now)
		wait := b.next[key].at.Sub(now)
		if b.due(key, now.Add(wait-time.Second)) || !b.due(key, now.Add(wait)) {
			t.Fatalf("failure %d: due before its %v wait, or not after it", i+1, wait)
		}
		waits = append(waits, wait)
	}
	want := []time.Duration{1, 2, 4, 8, 16, 32, 32, 32}
	for i, w := range want {
		if waits[i] != w*time.Minute {
			t.Fatalf("waits = %v, want %v minutes", waits, want)
		}
	}
	b.record(key, false, now)
	if !b.due(key, now) {
		t.Fatal("a success must clear the backoff")
	}
}
