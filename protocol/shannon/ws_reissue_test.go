package shannon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
	"github.com/pokt-network/sage/websockets"
)

// A metered plan's quota error as a supplier relayed it on mainnet robinhood
// (2026-10-03), answering request 7.
const quotaAnswer = `{"jsonrpc":"2.0","id":7,"error":{"code":-32029,"message":"rate limit exceeded","data":{"limit":60,"remaining":0,"unit":"cu_per_minute","retry_after_ms":43000}}}`

// perSupplierFullNode answers every frame a supplier signed with that
// supplier's payload.
type perSupplierFullNode struct {
	mockRelayFullNode
	mu      sync.Mutex
	answers map[string]string
}

func (n *perSupplierFullNode) ValidateRelayResponse(supplier string, _ []byte) (*servicetypes.RelayResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &servicetypes.RelayResponse{Payload: []byte(n.answers[supplier])}, nil
}

// echoSupplier is a relay miner that answers every frame with itself, so the
// bridge hands each one to the processor, and counts them.
func echoSupplier(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var got atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			got.Add(1)
			if err := conn.WriteMessage(mt, msg); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

func reissueProcessor(p *Protocol, subs *qos.SubscriptionRegistry, spy *spyWSMetrics, supplier string, canRebind func() bool) *wsMessageProcessor {
	proc := newWSMessageProcessor(context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "robinhood", SessionId: "s-1", SessionEndBlockHeight: 200},
		supplier, domain.EndpointAddr(supplier+"-https://rel."+supplier+".example"),
		&apptypes.Application{Address: "pokt1app"}, nil)
	proc.subs, proc.metrics, proc.canRebind = subs, spy, canRebind
	proc.operator = proc.endpointAddr.Operator()
	return proc
}

// A supplier's quota refusal of a request the client is waiting on never
// reaches the client: the bridge rebinds and the request goes again to the
// next supplier, whose answer is the one the client reads.
func TestWSReissue_QuotaRefusalIsAnsweredByTheNextSupplier(t *testing.T) {
	got, rebinds, secondGot, spy := reissueThroughBridge(t, quotaAnswer,
		`{"jsonrpc":"2.0","id":7,"method":"eth_getBalance","params":["0xabc","latest"]}`, nil)
	if got != `{"jsonrpc":"2.0","id":7,"result":"0x64"}` {
		t.Fatalf("client got %s; want the next supplier's answer, never the quota refusal", got)
	}
	if rebinds != 1 || secondGot != 1 {
		t.Errorf("rebinds %d, frames at the next supplier %d; want 1 and 1", rebinds, secondGot)
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.reissued) != 1 || spy.reissued[0] != "pokt1first.example" {
		t.Errorf("reissues counted %v, want one against pokt1first.example", spy.reissued)
	}
}

// reissueThroughBridge runs a real bridge from a client to a first supplier
// answering request 7 with firstAnswer, and a second answering it "0x64"; it
// returns what the client read, the rebinds taken, the frames the second
// supplier received, and the metrics. staleness, when set, is each
// processor's (nil: none).
func reissueThroughBridge(t *testing.T, firstAnswer, request string, staleness *wsStaleness) (string, int32, int32, *spyWSMetrics) {
	t.Helper()
	first, _ := echoSupplier(t)
	second, secondGot := echoSupplier(t)
	fn := &perSupplierFullNode{answers: map[string]string{
		"pokt1first":  firstAnswer,
		"pokt1second": `{"jsonrpc":"2.0","id":7,"result":"0x64"}`,
	}}
	p := &Protocol{fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(), logger: newTestLogger()}
	subs := qos.NewSubscriptionRegistry(&evm.Plugin{})
	spy := &spyWSMetrics{}

	var bridgeRef atomic.Pointer[websockets.Bridge]
	canRebind := func() bool { b := bridgeRef.Load(); return b != nil && b.CanRebind() }
	proc := func(supplier string) *wsMessageProcessor {
		pr := reissueProcessor(p, subs, spy, supplier, canRebind)
		pr.staleness = staleness
		return pr
	}
	var rebinds atomic.Int32
	lost := func(_ context.Context, _ error) (*websocket.Conn, websockets.MessageProcessor, [][]byte, error) {
		rebinds.Add(1)
		conn, err := websockets.ConnectEndpoint(newTestLogger(), wsURL(second), nil)
		if err != nil {
			return nil, nil, nil, err
		}
		return conn, proc("pokt1second"), subs.Replay().Frames, nil
	}
	up := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := websockets.StartBridge(context.Background(), newTestLogger(), r, w, wsURL(first), nil,
			proc("pokt1first"), websockets.WithEndpointLost(lost))
		if err != nil {
			return
		}
		bridgeRef.Store(b)
		close(up)
		<-b.Done()
	}))
	t.Cleanup(srv.Close)

	client, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	<-up

	if err := client.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, got, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	return string(got), rebinds.Load(), secondGot.Load(), spy
}

// What is not reissued reaches the client as it came: a refusal on a bridge
// with no rebind left (closing it would be worse), a refusal of a request
// nobody tracked, and an error that is the request's own answer.
func TestWSReissue_OtherwiseTheFrameReachesTheClient(t *testing.T) {
	for name, tc := range map[string]struct {
		answer    string
		canRebind bool
		track     bool
	}{
		"no rebind left":    {quotaAnswer, false, true},
		"not in flight":     {quotaAnswer, true, false},
		"chain's own error": {`{"jsonrpc":"2.0","id":7,"error":{"code":3,"message":"execution reverted"}}`, true, true},
	} {
		t.Run(name, func(t *testing.T) {
			fn := &perSupplierFullNode{answers: map[string]string{"pokt1first": tc.answer}}
			p := &Protocol{fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(), logger: newTestLogger()}
			proc := reissueProcessor(p, qos.NewSubscriptionRegistry(&evm.Plugin{}), &spyWSMetrics{}, "pokt1first",
				func() bool { return tc.canRebind })
			if tc.track {
				if _, err := proc.ProcessClientMessage([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_call","params":[]}`)); err != nil {
					t.Fatal(err)
				}
			}
			out, err := proc.ProcessEndpointMessage([]byte(`wire`))
			if err != nil || string(out) != tc.answer {
				t.Fatalf("out %s err %v; want the frame forwarded as it came", out, err)
			}
		})
	}
}
