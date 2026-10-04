package shannon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/drain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/reputation"
)

// nopSigner signs nothing, and is safe across the bridge's goroutines.
type nopSigner struct{}

func (nopSigner) signRelayRequest(_ context.Context, req *servicetypes.RelayRequest, _ *apptypes.Application) (*servicetypes.RelayRequest, error) {
	return req, nil
}

// signalLog is tieredRep that records signal reasons safely across goroutines.
type signalLog struct {
	tieredRep
	mu      sync.Mutex
	reasons map[domain.EndpointAddr][]string
}

func (s *signalLog) RecordSignal(_ context.Context, _ domain.ServiceID, ep domain.EndpointAddr, _ domain.RPCType, sig reputation.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reasons[ep] = append(s.reasons[ep], sig.Reason)
	return nil
}

func (s *signalLog) of(ep domain.EndpointAddr) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.reasons[ep]...)
}

// silentSupplier takes the upgrade and every frame, and never answers.
func silentSupplier(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// twoSuppliers is a relayer over a session of two WebSocket suppliers: a
// silent one, the only tier-1 pick, so a connection opens on it, and an echo
// one, tier 2, which a rebind drops to once the silent one is tried or
// drained. dial opens a client connection through Open.
type twoSuppliers struct {
	r                *WSRelayer
	p                *Protocol
	rep              *signalLog
	spy              *spyWSMetrics
	silentEP, echoEP domain.EndpointAddr
	dial             func() *websocket.Conn
}

func newTwoSuppliers(t *testing.T, flags map[string]bool) *twoSuppliers {
	t.Helper()
	silent := silentSupplier(t)
	echo, _ := echoSupplier(t)
	silentURL := wsURL(silent)
	echoURL := strings.Replace(wsURL(echo), "127.0.0.1", "localhost", 1) // another operator
	supplier := func(addr, url string) *sharedtypes.Supplier {
		return &sharedtypes.Supplier{OperatorAddress: addr, Services: []*sharedtypes.SupplierServiceConfig{{
			ServiceId: "eth", Endpoints: []*sharedtypes.SupplierEndpoint{{Url: url, RpcType: sharedtypes.RPCType_WEBSOCKET}},
		}}}
	}
	session := &sessiontypes.Session{
		SessionId: "s1",
		Header: &sessiontypes.SessionHeader{
			SessionId: "s1", ServiceId: "eth", ApplicationAddress: "pokt1app",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers:   []*sharedtypes.Supplier{supplier("pokt1silent", silentURL), supplier("pokt1echo", echoURL)},
	}
	f := &twoSuppliers{
		silentEP: domain.EndpointAddr("pokt1silent-" + silentURL),
		echoEP:   domain.EndpointAddr("pokt1echo-" + echoURL),
		spy:      &spyWSMetrics{},
	}
	if f.silentEP.Operator() == f.echoEP.Operator() {
		t.Fatalf("fixture: both suppliers are operator %q", f.silentEP.Operator())
	}
	fn := &perSupplierFullNode{
		mockRelayFullNode: mockRelayFullNode{session: session, app: &apptypes.Application{Address: "pokt1app"}},
		answers:           map[string]string{"pokt1echo": `{"jsonrpc":"2.0","id":7,"result":"0x1"}`},
	}
	f.p = &Protocol{
		fullNode: fn, sessions: newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer: nopSigner{}, bl: newBlacklist(), ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		metrics: noopSupplierMetrics{}, logger: newTestLogger(),
	}
	f.p.sessions.latestBlockHeight.Store(105)
	reg := qos.NewRegistry()
	if err := reg.Register("eth", evm.NewPlugin(nil, evm.Config{})); err != nil {
		t.Fatal(err)
	}
	f.rep = &signalLog{
		tieredRep: tieredRep{&spyRepSvc{}, map[string]float64{f.silentEP.Operator(): 100, f.echoEP.Operator(): 60}},
		reasons:   map[domain.EndpointAddr][]string{},
	}
	all := map[string]bool{featureflag.FlagWebsocketRelays: true}
	for k, v := range flags {
		all[k] = v
	}
	f.r = NewWSRelayer(WSRelayerDeps{
		Protocol: f.p, Reputation: f.rep, Observe: newDisabledQueue(),
		Flags: featureflag.NewMemoryStore(all), Logger: newTestLogger(), Metrics: f.spy, QoS: reg,
		RequestTimeout: func(domain.ServiceID) time.Duration { return 300 * time.Millisecond },
	})
	f.r.stallCheck = 50 * time.Millisecond

	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = f.r.Open(req.Context(), "eth", req, w)
	}))
	t.Cleanup(gw.Close)
	f.dial = func() *websocket.Conn {
		client, _, err := websocket.DefaultDialer.Dial(wsURL(gw), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	return f
}

// A supplier that takes a request and stays silent no longer holds the client
// for ever: past the relay timeout it is charged ws_no_answer and the
// connection moves. The read goes again to the next supplier and is answered;
// the write, which the silent one may have applied, is answered with an error
// rather than sent twice.
func TestWSOpen_SilentSupplierIsLeftAndEveryRequestAnswered(t *testing.T) {
	f := newTwoSuppliers(t, nil)
	rep, spy, silentEP, echoEP := f.rep, f.spy, f.silentEP, f.echoEP
	client := f.dial()

	for _, frame := range []string{
		`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`,
		`{"jsonrpc":"2.0","id":8,"method":"eth_sendRawTransaction","params":["0xf8"]}`,
	} {
		if err := client.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
			t.Fatal(err)
		}
	}
	got := map[string]string{}
	for len(got) < 2 {
		_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, msg, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("client read after %v: %v", got, err)
		}
		got[qos.JSONRPCRequestID(msg)] = string(msg)
	}
	if got["7"] != `{"jsonrpc":"2.0","id":7,"result":"0x1"}` {
		t.Errorf("the read was answered %s; want the next supplier's answer", got["7"])
	}
	if !strings.Contains(got["8"], `"error"`) || !strings.Contains(got["8"], "may or may not have been applied") {
		t.Errorf("the write was answered %s; want the lost-write error", got["8"])
	}
	if reasons := rep.of(silentEP); len(reasons) != 1 || reasons[0] != "ws_no_answer" {
		t.Errorf("the silent supplier was charged %v, want one ws_no_answer", reasons)
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.reissueReasons) != 1 || spy.reissueReasons[0] != "no_answer" {
		t.Errorf("reissues %v, want one no_answer", spy.reissueReasons)
	}
	// The answer is timed against the supplier that gave it; the silent one
	// gave none. The open is timed by phase, and so is the rebind's dial.
	if len(spy.answers) != 1 || spy.answers[0] != echoEP.Operator() {
		t.Errorf("answers timed %v, want one from %s", spy.answers, echoEP.Operator())
	}
	if strings.Join(spy.phases, ",") != "resolve,dial,rebind_dial" {
		t.Errorf("open phases %v, want resolve, dial, rebind_dial", spy.phases)
	}
}

// A drain on the supplier a live connection is bound to moves it, as a
// planned rebind, without waiting for the session to end; with
// ws_drain_rebind off it stays.
func TestWSOpen_DrainMovesALiveConnection(t *testing.T) {
	for _, on := range []bool{true, false} {
		f := newTwoSuppliers(t, map[string]bool{featureflag.FlagWSDrainRebind: on})
		drains := drain.NewMemoryStore()
		f.p.SetDrains(drains)
		f.r.chainHeight = func() int64 { return 105 } // inside the session: no rollover
		f.r.expiryCheck = 20 * time.Millisecond
		f.dial()
		require.Eventually(t, func() bool {
			f.spy.mu.Lock()
			defer f.spy.mu.Unlock()
			return len(f.spy.bound) == 1
		}, 2*time.Second, 10*time.Millisecond, "the connection never bound")

		require.NoError(t, drains.Set(context.Background(), drain.Entry{
			Key:   drain.Key{ServiceID: "eth", Operator: f.silentEP.Operator(), RPCType: domain.RPCTypeWebSocket},
			Until: time.Now().Add(time.Hour),
		}))
		moved := func() bool {
			f.spy.mu.Lock()
			defer f.spy.mu.Unlock()
			return len(f.spy.bound) == 2 && strings.HasPrefix(f.spy.bound[1], f.echoEP.Operator())
		}
		if on {
			require.Eventually(t, moved, 2*time.Second, 10*time.Millisecond, "the drained supplier's connection was not moved")
		} else {
			time.Sleep(200 * time.Millisecond)
			require.False(t, moved(), "with ws_drain_rebind off the connection must stay")
		}
	}
}

// A supplier's first silent request is minor; a second within the window is
// major, and so is every one with ws_no_answer_minor_first off.
func TestWSNoAnswerSeverity(t *testing.T) {
	for _, on := range []bool{true, false} {
		r := NewWSRelayer(WSRelayerDeps{
			Protocol: &Protocol{}, Reputation: &spyRepSvc{}, Observe: newDisabledQueue(),
			Flags:  featureflag.NewMemoryStore(map[string]bool{featureflag.FlagWSNoAnswerMinorFirst: on}),
			Logger: newTestLogger(),
		})
		first, second, other := r.noAnswerSeverity("eth", "ep1"), r.noAnswerSeverity("eth", "ep1"), r.noAnswerSeverity("eth", "ep2")
		want := reputation.SignalMinorError
		if !on {
			want = reputation.SignalMajorError
		}
		if first != want || second != reputation.SignalMajorError || other != want {
			t.Errorf("flag %v: first %v second %v other endpoint %v; want %v, major, %v", on, first, second, other, want, want)
		}
	}
}
