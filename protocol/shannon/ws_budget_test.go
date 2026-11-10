package shannon

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	sharedtypes "github.com/pokt-network/poktroll/x/shared/types"
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/tuning"
)

// The meter holds a window's notifications to rate × window, starting over
// each window; a budget turned off since the window started spares the
// connection at the crossing. It trips once, naming the topic that pushed
// the most, and stays tripped. With no rate it counts nothing.
func TestWSBudget_Meter(t *testing.T) {
	now := time.Unix(1_000, 0)
	rate := 2 // 40 a window
	var trips []string
	b := &wsBudget{rate: func() int { return rate }, now: func() time.Time { return now }, onTrip: func(topic string) { trips = append(trips, topic) }}
	spend := func(n int, topic string) (over int) {
		for range n {
			if b.spend(topic) {
				over++
			}
		}
		return over
	}

	if over := spend(30, "logs") + spend(10, "newHeads"); over != 0 {
		t.Fatalf("%d of 40 counted over a budget of 40 a window", over)
	}
	now = now.Add(wsBudgetWindow)
	rate = 1 // read at this window's start
	if over := spend(20, "logs"); over != 0 {
		t.Fatalf("the new window carried the last one's count: %d over", over)
	}
	rate = 0 // turned off mid-window
	if over := spend(5, "logs"); over != 0 || b.exceeded() {
		t.Fatalf("budget turned off: %d over (exceeded %v), want none", over, b.exceeded())
	}
	now = now.Add(wsBudgetWindow)
	rate = 1
	if over := spend(20, "logs"); over != 0 {
		t.Fatalf("%d over within a budget of 20", over)
	}
	if over := spend(1, "newHeads") + spend(5, "logs"); over != 6 || !b.exceeded() {
		t.Fatalf("%d over past a budget of 20 (exceeded %v), want 6", over, b.exceeded())
	}
	now = now.Add(wsBudgetWindow)
	if !b.spend("logs") || len(trips) != 1 || trips[0] != "logs" {
		t.Fatalf("after the trip: trips %v; want one, on logs, and every later notification over", trips)
	}

	off := &wsBudget{rate: func() int { return 0 }, now: time.Now, onTrip: func(string) { t.Fatal("tripped with no budget") }}
	for range 1_000 {
		off.spend("logs")
	}
	if off.count != 0 || len(off.topics) != 0 {
		t.Errorf("with no budget the meter counted %d (%v)", off.count, off.topics)
	}
	var none *wsBudget
	if none.spend("logs") || none.exceeded() {
		t.Error("a nil meter is over")
	}
}

// pushSupplier answers each subscribe with an ack on its id, then pushes push
// log notifications on it. With dropFirst its first connection is cut after
// the pushes, without a close frame. conns counts the connections it took.
func pushSupplier(t *testing.T, push int, dropFirst bool) (srv *httptest.Server, conns, ended *atomic.Int32) {
	t.Helper()
	conns, ended = &atomic.Int32{}, &atomic.Int32{}
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		n := conns.Add(1)
		defer ended.Add(1)
		conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		send := func(payload string) error {
			bz, _ := (&servicetypes.RelayResponse{Payload: []byte(payload)}).Marshal()
			return conn.WriteMessage(websocket.BinaryMessage, bz)
		}
		sub := fmt.Sprintf("0xs%d", n)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var rr servicetypes.RelayRequest
			if rr.Unmarshal(msg) != nil {
				return
			}
			if send(`{"jsonrpc":"2.0","id":`+gjson.GetBytes(rr.Payload, "id").Raw+`,"result":"`+sub+`"}`) != nil {
				return
			}
			for i := range push {
				if send(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":%q,"result":{"logIndex":"0x%x","conn":%d}}}`, sub, i, n)) != nil {
					return
				}
			}
			if dropFirst && n == 1 {
				// Long enough for the bridge to route what was pushed: a
				// frame still queued from a socket a rebind replaced is dropped.
				time.Sleep(500 * time.Millisecond)
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, conns, ended
}

type budgetFixture struct {
	r     *WSRelayer
	spy   *spyWSMetrics
	rep   *signalLog
	store *tuning.Store
	conns *atomic.Int32
	ended *atomic.Int32 // supplier connections whose socket the gateway closed
	gw    *httptest.Server

	supplier *httptest.Server
}

func newBudgetFixture(t *testing.T, push int, dropFirst bool) *budgetFixture {
	t.Helper()
	supplier, conns, ended := pushSupplier(t, push, dropFirst)
	url := wsURL(supplier)
	session := &sessiontypes.Session{
		SessionId: "s1",
		Header: &sessiontypes.SessionHeader{
			SessionId: "s1", ServiceId: "eth", ApplicationAddress: "pokt1app",
			SessionStartBlockHeight: 100, SessionEndBlockHeight: 110,
		},
		Application: &apptypes.Application{Address: "pokt1app"},
		Suppliers: []*sharedtypes.Supplier{{OperatorAddress: "pokt1push", Services: []*sharedtypes.SupplierServiceConfig{{
			ServiceId: "eth", Endpoints: []*sharedtypes.SupplierEndpoint{{Url: url, RpcType: sharedtypes.RPCType_WEBSOCKET}},
		}}}},
	}
	fn := passthroughFullNode{&mockRelayFullNode{session: session, app: &apptypes.Application{Address: "pokt1app"}}}
	p := &Protocol{
		fullNode: fn, sessions: newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer: nopSigner{}, bl: newBlacklist(), ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		metrics: noopSupplierMetrics{}, logger: newTestLogger(),
	}
	p.sessions.latestBlockHeight.Store(105)
	reg := qos.NewRegistry()
	if err := reg.Register("eth", evm.NewPlugin(nil, evm.Config{})); err != nil {
		t.Fatal(err)
	}
	f := &budgetFixture{
		spy:   &spyWSMetrics{},
		rep:   &signalLog{tieredRep: tieredRep{&spyRepSvc{}, map[string]float64{"pokt1push": 100}}, reasons: map[domain.EndpointAddr][]string{}},
		store: tuning.NewStore(),
		conns: conns,
		ended: ended,

		supplier: supplier,
	}
	f.r = NewWSRelayer(WSRelayerDeps{
		Protocol: p, Reputation: f.rep, Observe: newDisabledQueue(),
		Flags:  featureflag.NewMemoryStore(map[string]bool{featureflag.FlagWebsocketRelays: true}),
		Logger: newTestLogger(), Metrics: f.spy, QoS: reg,
		NotificationBudget: func(id domain.ServiceID) int { return f.store.Int(tuning.KnobWSMaxNotificationsPerSecond, id, 0) },
		BudgetCooldown: func(id domain.ServiceID) time.Duration {
			return f.store.Duration(tuning.KnobWSBudgetCooldown, id, DefaultBudgetCooldown)
		},
	})
	f.gw = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = f.r.Open(req.Context(), "eth", req, w)
	}))
	t.Cleanup(f.gw.Close)
	return f
}

// subscribe opens a client connection, subscribes to a filtered logs feed and
// reads until the connection ends or goes quiet: the notifications it got and
// how it ended.
func (f *budgetFixture) subscribe(t *testing.T) (notifications int, end error) {
	t.Helper()
	client, _, err := websocket.DefaultDialer.Dial(wsURL(f.gw), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["logs",{"address":"0x55d398326f99059ff775485246999027b3197955"}]}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		_, msg, err := client.ReadMessage()
		if err != nil {
			return notifications, err
		}
		if strings.Contains(string(msg), "eth_subscription") {
			notifications++
		}
	}
}

func isBudgetClose(err error) bool {
	var ce *websocket.CloseError
	return errors.As(err, &ce) && ce.Code == websocket.ClosePolicyViolation && ce.Text == "subscription notification rate over this endpoint's limit"
}

// A connection drawing more than its budget is closed with 1008 after exactly
// the budget's worth, and no supplier is charged for it; the client is then
// refused with a 429 for the cooldown, but only while the service has a
// budget. With none, every notification goes through.
func TestWSOpen_BudgetClosesAndCoolsDown(t *testing.T) {
	f := newBudgetFixture(t, 30, false)

	if got, end := f.subscribe(t); got != 30 || isBudgetClose(end) {
		t.Fatalf("with no budget: %d notifications, ended %v; want all 30 and no close", got, end)
	}

	if err := f.store.Set(tuning.KnobWSMaxNotificationsPerSecond, "eth", "1"); err != nil { // 20 a window
		t.Fatal(err)
	}
	if got, end := f.subscribe(t); got != 20 || !isBudgetClose(end) {
		t.Fatalf("over budget: %d notifications, ended %v; want 20 then the 1008", got, end)
	}
	// The point of it: the supplier's stream is stopped, not just hidden.
	deadline := time.Now().Add(2 * time.Second)
	for f.ended.Load() < f.conns.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if ended, conns := f.ended.Load(), f.conns.Load(); ended != conns {
		t.Fatalf("%d of %d supplier sockets closed after the 1008", ended, conns)
	}
	f.spy.mu.Lock()
	closes := strings.Join(f.spy.budgetClosed, ",")
	f.spy.mu.Unlock()
	if closes != "eth|logs" {
		t.Errorf("budget closes counted %q, want eth|logs", closes)
	}
	for _, reason := range f.rep.of(domain.EndpointAddr("pokt1push-" + wsURL(f.supplier))) {
		if reason != "ws_frame_ok" {
			t.Errorf("supplier charged %s for a client's volume", reason)
		}
	}

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(f.gw), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "60" {
		t.Fatalf("reconnect in the cooldown: %v %+v; want a 429 with Retry-After 60", err, resp)
	}
	f.spy.mu.Lock()
	rejected := strings.Join(f.spy.rejected, ",")
	f.spy.mu.Unlock()
	if rejected != "eth:budget_cooldown" {
		t.Errorf("rejections counted %q, want eth:budget_cooldown", rejected)
	}

	if err := f.store.Set(tuning.KnobWSMaxNotificationsPerSecond, "eth", "0"); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.subscribe(t); got != 30 {
		t.Errorf("budget off, cooldown left: %d notifications, want the connection opened and all 30", got)
	}
}

// The meter is the connection's, not its supplier's: a rebind and the replay
// that follows do not start it over.
func TestWSOpen_BudgetCarriesAcrossARebind(t *testing.T) {
	f := newBudgetFixture(t, 15, true) // 15 a connection, under a budget of 20
	if err := f.store.Set(tuning.KnobWSMaxNotificationsPerSecond, "", "1"); err != nil {
		t.Fatal(err)
	}
	got, end := f.subscribe(t)
	if got != 20 || !isBudgetClose(end) {
		t.Fatalf("%d notifications over %d supplier connections, ended %v; want 20 across the rebind, then the 1008", got, f.conns.Load(), end)
	}
	if n := f.conns.Load(); n != 2 {
		t.Errorf("supplier took %d connections, want 2", n)
	}
}
