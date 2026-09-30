package shannon

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/protocol"
)

// passthroughFullNode verifies nothing and hands back the frame the supplier
// sent, so a test supplier's frames reach the probe as written.
type passthroughFullNode struct{ *mockRelayFullNode }

func (passthroughFullNode) ValidateRelayResponse(_ string, bz []byte) (*servicetypes.RelayResponse, error) {
	var r servicetypes.RelayResponse
	return &r, r.Unmarshal(bz)
}

// frameSupplier answers the first frame it reads with frames, each wrapped as
// a RelayResponse, then holds the connection open.
func frameSupplier(t *testing.T, frames ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		for _, f := range frames {
			bz, _ := (&servicetypes.RelayResponse{Payload: []byte(f)}).Marshal()
			if conn.WriteMessage(websocket.BinaryMessage, bz) != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func debugFixture(t *testing.T, supplierURL string) (*WSRelayer, *spyRepSvc) {
	t.Helper()
	r, spy, _ := probeFixture(t, supplierURL, 100, "", map[string]bool{featureflag.FlagWebsocketRelays: true})
	p := r.deps.Protocol
	p.fullNode = passthroughFullNode{p.fullNode.(*mockRelayFullNode)}
	p.sessions = newSessionManager(p.fullNode, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	p.debugLimits = newDebugLimits()
	return r, spy
}

const subscribeHeads = `{"jsonrpc":"2.0","id":7,"method":"eth_subscribe","params":["newHeads"]}`

// A probe subscription records the ack and what each notification names, with
// the signed frame, and changes nothing: no reputation signal, no blacklist.
func TestDebugSubscribe_RecordsFramesUntilTheDuration(t *testing.T) {
	supplier := frameSupplier(t,
		`{"jsonrpc":"2.0","id":7,"result":"0xabc"}`,
		`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"number":"0x10","hash":"0xh16"}}}`,
		`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"blockNumber":"0x11","blockHash":"0xh17","transactionHash":"0xt1","logIndex":"0x2"}}}`,
		`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":"0xpending"}}`,
	)
	r, spy := debugFixture(t, wsURL(supplier))

	res, err := r.DebugSubscribe(context.Background(), "eth", "pokt1owner", []byte(subscribeHeads),
		DebugSubscribeOptions{Duration: 1500 * time.Millisecond, IncludeSigned: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stop != "duration" || res.Error != "" {
		t.Fatalf("stop=%q error=%q, want duration", res.Stop, res.Error)
	}
	if !strings.Contains(res.SubscribeAck, `"0xabc"`) {
		t.Fatalf("ack = %q", res.SubscribeAck)
	}
	if res.Target.Owner != "pokt1owner" || res.Session == nil || res.Session.EndHeight != 110 {
		t.Fatalf("target=%+v session=%+v", res.Target, res.Session)
	}
	if len(res.Events) != 4 {
		t.Fatalf("events = %+v, want 4", res.Events)
	}
	head, log, tx := res.Events[1], res.Events[2], res.Events[3]
	if head.BlockNumber == nil || *head.BlockNumber != 16 || head.BlockHash != "0xh16" || head.TxHash != "" {
		t.Errorf("head = %+v", head)
	}
	if log.BlockNumber == nil || *log.BlockNumber != 17 || log.TxHash != "0xt1" || log.LogIndex == nil || *log.LogIndex != 2 {
		t.Errorf("log = %+v", log)
	}
	if tx.TxHash != "0xpending" || tx.BlockNumber != nil {
		t.Errorf("pending tx = %+v", tx)
	}
	for _, ev := range res.Events {
		if len(ev.Signed) == 0 {
			t.Fatalf("event without its signed frame: %+v", ev)
		}
	}
	if len(spy.calls) != 0 {
		t.Fatalf("a probe recorded reputation signals: %+v", spy.calls)
	}
}

// The probe does not rebind: it stops when the chain passes the session it
// was signed for, and says so.
func TestDebugSubscribe_StopsAtSessionEnd(t *testing.T) {
	r, _ := debugFixture(t, wsURL(frameSupplier(t, `{"jsonrpc":"2.0","id":7,"result":"0xabc"}`)))
	r.deps.Protocol.sessions.latestBlockHeight.Store(111)
	res, err := r.DebugSubscribe(context.Background(), "eth", "pokt1supplier", []byte(subscribeHeads), DebugSubscribeOptions{Duration: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stop != "session_ended" {
		t.Fatalf("stop = %q, want session_ended", res.Stop)
	}
}

// A target outside the session is a not-found that lists who is there, and
// the concurrency cap refuses the sixth subscription.
func TestDebugSubscribe_UnknownTargetAndCap(t *testing.T) {
	r, _ := debugFixture(t, wsURL(frameSupplier(t)))
	_, err := r.DebugSubscribe(context.Background(), "eth", "nobody.example", []byte(subscribeHeads), DebugSubscribeOptions{})
	if !errors.Is(err, protocol.ErrDebugTargetNotFound) || !strings.Contains(err.Error(), "operators in the session") {
		t.Fatalf("err = %v", err)
	}
	if _, err := r.DebugSubscribe(context.Background(), "eth", "x", []byte(`{}`), DebugSubscribeOptions{}); !errors.Is(err, protocol.ErrDebugBadRequest) {
		t.Fatalf("no method: err = %v", err)
	}
	for i := 0; i < debugMaxSubscriptions; i++ {
		if _, ok := r.deps.Protocol.debugLimits.takeSubscription(); !ok {
			t.Fatal("slot refused below the cap")
		}
	}
	if _, err := r.DebugSubscribe(context.Background(), "eth", "pokt1owner", []byte(subscribeHeads), DebugSubscribeOptions{}); !errors.Is(err, protocol.ErrDebugBusy) {
		t.Fatalf("over the cap: err = %v", err)
	}
}

// A relay whose response fails verification is reported with the bytes the
// supplier sent, and the supplier is not blacklisted. The rate cap refuses a
// second relay inside 100ms.
func TestDebugRelay_ReportsAFailedVerificationWithoutActing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("forged"))
	}))
	t.Cleanup(server.Close)
	session := buildRelayTestSession("pokt1supplier", server.URL)
	fn := &mockRelayFullNode{session: session, app: session.Application, validateErr: errors.New("bad signature")}
	p := &Protocol{
		fullNode: fn, sessions: newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger()),
		signer: &mockSigner{}, bl: newBlacklist(), ownedApps: map[domain.ServiceID][]string{"eth": {"pokt1app"}},
		httpClient: server.Client(), metrics: noopSupplierMetrics{}, logger: newTestLogger(), debugLimits: newDebugLimits(),
	}
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

	res, err := p.DebugRelay(context.Background(), "eth", server.URL, domain.RPCTypeJSONRPC, body)
	if err != nil {
		t.Fatal(err)
	}
	if res.Error == "" || string(res.SignedResponse) != "forged" || len(res.SignedRequest) == 0 || res.HTTPStatus != http.StatusOK {
		t.Fatalf("result = %+v", res)
	}
	if p.bl.IsBlacklisted("eth", "pokt1supplier") {
		t.Fatal("a probe blacklisted the supplier")
	}
	if _, err := p.DebugRelay(context.Background(), "eth", server.URL, domain.RPCTypeJSONRPC, body); !errors.Is(err, protocol.ErrDebugBusy) {
		t.Fatalf("second relay inside the interval: err = %v", err)
	}
}

// "reference" posts to the configured URL, and an unconfigured service says
// which key is missing without naming any URL.
func TestDebugRelay_Reference(t *testing.T) {
	ref := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
	}))
	t.Cleanup(ref.Close)
	p := &Protocol{logger: newTestLogger(), debugLimits: newDebugLimits()}
	p.SetDebugReferences(map[domain.ServiceID]string{"eth": ref.URL}, nil)
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)

	res, err := p.DebugRelay(context.Background(), "eth", "reference", domain.RPCTypeJSONRPC, body)
	if err != nil || res.Body != `{"jsonrpc":"2.0","id":1,"result":"0x10"}` || res.Target.Endpoint != "reference" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	p.debugLimits = newDebugLimits() // past the rate cap the first relay took
	if _, err := p.DebugRelay(context.Background(), "base", "reference", domain.RPCTypeJSONRPC, body); !errors.Is(err, protocol.ErrDebugTargetNotFound) {
		t.Fatalf("unconfigured: err = %v", err)
	}
}
