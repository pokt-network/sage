package shannon

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"
	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
)

// headAt100 is an EVM plugin whose chain head is block 100 (0x64), stale past
// two blocks: an eth_blockNumber answer or a pushed head below 98 is stale.
type headAt100 struct{ *evm.Plugin }

func (headAt100) lag(h uint64) (uint64, bool, bool) {
	if h >= 100 {
		return 0, false, true
	}
	return 100 - h, 100-h > 2, true
}

func (l headAt100) HeadLag(payload domain.Payload, response []byte, _ time.Time) (uint64, bool, bool) {
	if payload.Method() != "eth_blockNumber" {
		return 0, false, false
	}
	h, err := strconv.ParseUint(strings.TrimPrefix(gjson.GetBytes(response, "result").String(), "0x"), 16, 64)
	if err != nil {
		return 0, false, false
	}
	return l.lag(h)
}

func (l headAt100) HeightLag(h uint64, _ time.Time) (uint64, bool, bool) { return l.lag(h) }

// penalties records what a staleness check charged.
type penalties struct {
	mu      sync.Mutex
	reasons []string
}

func (p *penalties) add(reason string) { p.mu.Lock(); p.reasons = append(p.reasons, reason); p.mu.Unlock() }

func (p *penalties) all() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.reasons...)
}

func staleCheck(on bool, pen *penalties) *wsStaleness {
	l := headAt100{&evm.Plugin{}}
	return &wsStaleness{enabled: func() bool { return on }, answers: l, heights: l, penalize: pen.add}
}

// A head answer the chain has moved past is charged ws_stale_response and goes
// again to the next supplier: the client reads the current head, never the
// stale one.
func TestWSStale_StaleAnswerIsAnsweredByTheNextSupplier(t *testing.T) {
	pen := &penalties{}
	got, rebinds, _, spy := reissueThroughBridge(t, `{"jsonrpc":"2.0","id":7,"result":"0x10"}`,
		`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`, staleCheck(true, pen))
	if got != `{"jsonrpc":"2.0","id":7,"result":"0x64"}` || rebinds != 1 {
		t.Fatalf("client got %s after %d rebinds; want the next supplier's answer after one", got, rebinds)
	}
	if r := pen.all(); len(r) != 1 || r[0] != reasonWSStaleResponse {
		t.Errorf("charged %v, want one %s", r, reasonWSStaleResponse)
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.reissued) != 1 {
		t.Errorf("reissues counted %v, want one", spy.reissued)
	}
}

// With stale_response off, or a current answer, nothing is charged and the
// answer goes to the client as it came.
func TestWSStale_OffOrCurrentIsForwarded(t *testing.T) {
	for name, tc := range map[string]struct {
		on     bool
		answer string
	}{
		"flag off":       {false, `{"jsonrpc":"2.0","id":7,"result":"0x10"}`},
		"current answer": {true, `{"jsonrpc":"2.0","id":7,"result":"0x63"}`},
	} {
		t.Run(name, func(t *testing.T) {
			pen := &penalties{}
			fn := &perSupplierFullNode{answers: map[string]string{"pokt1first": tc.answer}}
			p := &Protocol{fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(), logger: newTestLogger()}
			proc := reissueProcessor(p, qos.NewSubscriptionRegistry(&evm.Plugin{}), &spyWSMetrics{}, "pokt1first", func() bool { return true })
			proc.staleness = staleCheck(tc.on, pen)
			if _, err := proc.ProcessClientMessage([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`)); err != nil {
				t.Fatal(err)
			}
			out, err := proc.ProcessEndpointMessage([]byte(`wire`))
			if err != nil || string(out) != tc.answer || len(pen.all()) != 0 {
				t.Fatalf("out %s err %v charged %v; want the answer forwarded, nothing charged", out, err, pen.all())
			}
		})
	}
}

// A run of wsStaleHeadsToRebind stale newHeads pushes is charged once and
// moves the connection; a current push in the middle starts the run over, and
// with the flag off nothing is charged or moved.
func TestWSStale_StaleHeadsMoveTheConnection(t *testing.T) {
	head := func(n uint64) string {
		return `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsub","result":{"number":"0x` +
			strconv.FormatUint(n, 16) + `","hash":"0x` + strconv.FormatUint(n, 16) + `aa"}}}`
	}
	setup := func(on bool) (*wsMessageProcessor, *mockRelayFullNode, *penalties) {
		fn := &mockRelayFullNode{}
		p := &Protocol{fullNode: fn, signer: &countingSigner{}, bl: newBlacklist(), logger: newTestLogger()}
		proc := newWSMessageProcessor(context.Background(), p,
			&sessiontypes.SessionHeader{ServiceId: "robinhood", SessionEndBlockHeight: 200},
			"pokt1first", "pokt1first-https://rel.pokt1first.example", &apptypes.Application{Address: "pokt1app"}, nil)
		pen := &penalties{}
		proc.subs, proc.canRebind, proc.staleness = qos.NewSubscriptionRegistry(&evm.Plugin{}), func() bool { return true }, staleCheck(on, pen)
		if _, err := proc.ProcessClientMessage([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
			t.Fatal(err)
		}
		fn.validateResponse = &servicetypes.RelayResponse{Payload: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xsub"}`)}
		if _, err := proc.ProcessEndpointMessage([]byte(`wire`)); err != nil {
			t.Fatal(err)
		}
		return proc, fn, pen
	}
	push := func(proc *wsMessageProcessor, fn *mockRelayFullNode, n uint64) error {
		fn.validateResponse = &servicetypes.RelayResponse{Payload: []byte(head(n))}
		_, err := proc.ProcessEndpointMessage([]byte(`wire`))
		return err
	}

	proc, fn, pen := setup(true)
	for i, n := range []uint64{10, 11, 99, 12, 13} { // the current 99 breaks the run
		if err := push(proc, fn, n); err != nil {
			t.Fatalf("push %d (block %d) moved the connection: %v", i, n, err)
		}
	}
	if err := push(proc, fn, 14); !errors.Is(err, errSupplierBehind) {
		t.Fatalf("the third stale push in a row: err %v, want errSupplierBehind", err)
	}
	if r := pen.all(); len(r) != 1 || r[0] != reasonWSStaleHead {
		t.Errorf("charged %v, want one %s for the run", r, reasonWSStaleHead)
	}

	proc, fn, pen = setup(false)
	for _, n := range []uint64{10, 11, 12, 13} {
		if err := push(proc, fn, n); err != nil || len(pen.all()) != 0 {
			t.Fatalf("flag off: err %v charged %v", err, pen.all())
		}
	}
}

// A probe whose eth_blockNumber answer is stale is behind, graded major; a
// current one is ok. Only with stale_response on.
func TestWSProbe_BehindIsAFailure(t *testing.T) {
	supplier := newEchoSupplier(t)
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true, featureflag.FlagStaleResponse: true}
	for _, tc := range []struct {
		answer, want string
	}{
		{`{"jsonrpc":"2.0","id":1,"result":"0x10"}`, wsProbeBehind},
		{`{"jsonrpc":"2.0","id":1,"result":"0x64"}`, wsProbeOK},
	} {
		r, _, m := probeFixture(t, wsURL(supplier), 40, tc.answer, enabled)
		reg := qos.NewRegistry()
		if err := reg.Register("eth", headAt100{&evm.Plugin{}}); err != nil {
			t.Fatal(err)
		}
		r.deps.QoS = reg
		r.probeCycle(context.Background(), []domain.ServiceID{"eth"})
		if len(m.probes) != 1 || m.probes[0] != tc.want {
			t.Errorf("answer %s: probe results %v, want [%s]", tc.answer, m.probes, tc.want)
		}
	}
}
