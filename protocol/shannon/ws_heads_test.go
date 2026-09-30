package shannon

import (
	"context"
	"reflect"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
)

var (
	opA = wsHeadPusher{"a.example", "oa"}
	opB = wsHeadPusher{"b.example", "ob"}
	opC = wsHeadPusher{"c.example", "oc"}
)

// An operator's repeat of a block it already pushed says nothing new; the
// first push of a block has no delay reading until a second operator pushes
// it; lag is measured against the higher of the consensus and the newest
// pushed head.
func TestWSHeadTracker_LagDelayAndDedupe(t *testing.T) {
	tr := newWSHeadTracker()
	t0 := time.Now()
	r, ok := tr.observe("eth", opA, 100, "0xh100", t0, 90)
	if !ok || r.lag != 0 || r.delayKnown {
		t.Fatalf("first push: %+v ok=%v", r, ok)
	}
	if _, ok := tr.observe("eth", opA, 100, "0xh100", t0.Add(time.Second), 90); ok {
		t.Fatal("an operator's repeat of a block must not be a reading")
	}
	r, ok = tr.observe("eth", opB, 100, "0xh100", t0.Add(300*time.Millisecond), 90)
	if !ok || !r.delayKnown || r.delay != 300*time.Millisecond || r.lag != 0 {
		t.Fatalf("second operator: %+v", r)
	}
	r, _ = tr.observe("eth", opA, 101, "0xh101", t0, 105)
	if r.lag != 4 {
		t.Fatalf("lag behind a consensus ahead of the feed = %d, want 4", r.lag)
	}
	r, _ = tr.observe("eth", opB, 97, "0xh97", t0, 0)
	if r.lag != 4 {
		t.Fatalf("lag behind the newest pushed head = %d, want 4", r.lag)
	}
}

// A block's hash is the one most operators pushed, judged 8 blocks on: the
// operator that pushed another is counted even when it pushed first.
func TestWSHeadTracker_MismatchByMajority(t *testing.T) {
	tr := newWSHeadTracker()
	now := time.Now()
	tr.observe("eth", opA, 100, "0xbad", now, 0)
	tr.observe("eth", opB, 100, "0xgood", now, 0)
	tr.observe("eth", opC, 100, "0xgood", now, 0)
	var wrong []wsHeadPusher
	for n := uint64(101); n <= 108; n++ {
		r, _ := tr.observe("eth", opB, n, "0xh", now, 0)
		wrong = append(wrong, r.mismatched...)
	}
	if !reflect.DeepEqual(wrong, []wsHeadPusher{opA}) {
		t.Fatalf("mismatched = %v, want [%v]", wrong, opA)
	}
	// Old blocks are forgotten.
	tr.observe("eth", opB, 200, "0xh", now, 0)
	if n := len(tr.services["eth"].blocks); n > wsHeadKeepDepth+1 {
		t.Fatalf("%d blocks kept, want at most %d", n, wsHeadKeepDepth+1)
	}
}

// A newHeads notification through the processor reaches the metrics once
// per block; a duplicate frame does not count again.
func TestWSProcessor_FeedsHeadSignals(t *testing.T) {
	p, _, _, fn := buildProcessorFixture()
	proc := newWSMessageProcessor(context.Background(), p,
		&sessiontypes.SessionHeader{ServiceId: "eth", SessionId: "s-1", SessionEndBlockHeight: 200},
		"pokt1supplier", "pokt1supplier-https://rel001.op-alpha.example",
		&apptypes.Application{Address: "pokt1app"}, nil)
	spy := &spyWSMetrics{}
	proc.withSubscriptions(qos.NewSubscriptionRegistry(&evm.Plugin{})).withSupplier(spy, "pokt1owner").
		withHeads(newWSHeadTracker(), func() uint64 { return 18 })
	endpoint := func(payload string) {
		t.Helper()
		fn.validateResponse = &servicetypes.RelayResponse{Payload: []byte(payload)}
		if _, err := proc.ProcessEndpointMessage([]byte(`wire`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := proc.ProcessClientMessage([]byte(`{"jsonrpc":"2.0","id":9,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
		t.Fatal(err)
	}
	endpoint(`{"jsonrpc":"2.0","id":9,"result":"0xsub"}`)
	head := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsub","result":{"number":"0x10","hash":"0xh16"}}}`
	endpoint(head)
	endpoint(head)

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if !reflect.DeepEqual(spy.heads, []string{"op-alpha.example|2|false"}) {
		t.Fatalf("heads = %v", spy.heads)
	}
}

// A push far past consensus is dropped, so it cannot pin the newest head.
func TestWSHeadTracker_IgnoresAHeadFarPastConsensus(t *testing.T) {
	tr := newWSHeadTracker()
	t0 := time.Now()
	if _, ok := tr.observe("eth", opA, 1_000_000, "0xliar", t0, 100); ok {
		t.Fatal("a head far past consensus must not be a reading")
	}
	r, ok := tr.observe("eth", opB, 100, "0xh100", t0, 100)
	if !ok || r.lag != 0 {
		t.Fatalf("honest push after the liar: %+v ok=%v, want lag 0", r, ok)
	}
}
