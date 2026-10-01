package evm

import (
	"fmt"
	"testing"
	"time"

	"github.com/pokt-network/sage/qos"
)

// Frames as geth emits them.
func TestSubscriptions_EVMRoundTrip(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_subscribe","params":["newHeads"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":7,"result":"0xcd0c3e8af590364c09d0fa6a1210faf5"}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != `"0xcd0c3e8af590364c09d0fa6a1210faf5"` || a[0].Method != "eth_subscribe" || a[0].Topic != "newHeads" {
		t.Fatalf("Active = %+v", a)
	}
	head := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcd0c3e8af590364c09d0fa6a1210faf5","result":{"number":"0x1"}}}`)
	if _, _, n := r.TranslateEndpointFrame(head); n.Kind != qos.NotificationOK || n.Topic != "newHeads" {
		t.Fatalf("first head = %+v, want ok on newHeads", n)
	}
	if _, _, n := r.TranslateEndpointFrame(head); n.Kind != qos.NotificationDuplicate {
		t.Fatalf("the same head again = %+v, want duplicate", n)
	}
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":8,"method":"eth_unsubscribe","params":["0xcd0c3e8af590364c09d0fa6a1210faf5"]}`))
	if len(r.Active()) != 0 {
		t.Fatal("eth_unsubscribe must close the subscription")
	}
}

func TestSubscriptions_EVMErrorAndPlainCalls(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["bogus"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid subscription"}}`))
	if len(r.Active()) != 0 {
		t.Fatal("a rejected subscribe is not a subscription")
	}
	// Ordinary request/response on the same socket: nothing to track.
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":2,"result":"0x10"}`))
	if len(r.Active()) != 0 {
		t.Fatal("a plain call's response must not open a subscription")
	}
}

// Across a rebind: the replay carries a gateway id, its ack is consumed, the
// new supplier's id is rewritten to the one the client holds, and the
// client's unsubscribe reaches the supplier under the supplier's id.
func TestSubscriptions_EVMRebindTranslation(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":7,"method":"eth_subscribe","params":["newHeads"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":7,"result":"0xold"}`))

	frames := r.ReplayFrames()
	if len(frames) != 1 || string(frames[0]) != `{"jsonrpc":"2.0","id":"sage-replay-1","method":"eth_subscribe","params":["newHeads"]}` {
		t.Fatalf("replay = %q", frames)
	}
	if out, fwd, _ := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":"sage-replay-1","result":"0xnew"}`)); fwd {
		t.Fatalf("replay ack must be consumed, got %q", out)
	}
	out, fwd, _ := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xnew","result":{"number":"0x2"}}}`))
	if !fwd || string(out) != `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xold","result":{"number":"0x2"}}}` {
		t.Fatalf("notification = %q", out)
	}
	if got := r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":8,"method":"eth_unsubscribe","params":["0xold"]}`)); string(got) != `{"jsonrpc":"2.0","id":8,"method":"eth_unsubscribe","params":["0xnew"]}` {
		t.Fatalf("unsubscribe = %q", got)
	}
}

// newHeads delivers on every block; logs and pending transactions deliver
// only what matches.
func TestSubscriptions_EVMPeriodicTopics(t *testing.T) {
	p := &Plugin{}
	for topic, want := range map[string]bool{"newHeads": true, "logs": false, "newPendingTransactions": false} {
		info := p.ClassifyClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["` + topic + `"]}`))
		if info.Topic != topic || info.Periodic != want {
			t.Errorf("%s: topic=%q periodic=%v, want periodic=%v", topic, info.Topic, info.Periodic, want)
		}
	}
}

// Mainnet bsc and robinhood, 2026-09-26: ~3% of notifications graded
// unsolicited, always alongside an ok stream of the same topic from the same
// supplier at the same rate — one feed under two subscription ids, one of them
// untracked. A reused request id and a subscribe inside a batch both left a
// live subscription out of the registry: graded unsolicited, and not replayed
// when the bridge rebinds, so the client lost that feed on a supplier swap.
func TestSubscriptions_EVMEverySubscribeIsTracked(t *testing.T) {
	grade := func(r *qos.SubscriptionRegistry, id string) qos.NotificationKind {
		_, _, n := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"` + id + `","result":"0x1"}}`))
		return n.Kind
	}

	reused := qos.NewSubscriptionRegistry(&Plugin{})
	reused.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}`))
	reused.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["logs",{}]}`))
	reused.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xa"}`))
	reused.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xb"}`))
	if a, b := grade(reused, "0xa"), grade(reused, "0xb"); a != qos.NotificationOK || b != qos.NotificationOK {
		t.Fatalf("reused request id: grades = %v, %v, want both ok", a, b)
	}
	if n := len(reused.ReplayFrames()); n != 2 {
		t.Fatalf("reused request id: %d subscriptions replayed on rebind, want 2", n)
	}

	batch := qos.NewSubscriptionRegistry(&Plugin{})
	batch.TranslateClientFrame([]byte(`[{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`))
	batch.TranslateEndpointFrame([]byte(`[{"jsonrpc":"2.0","id":1,"result":"0xa"},{"jsonrpc":"2.0","id":2,"result":"0x10"}]`))
	if g := grade(batch, "0xa"); g != qos.NotificationOK {
		t.Fatalf("subscribe inside a batch: grade = %v, want ok", g)
	}
	if n := len(batch.ReplayFrames()); n != 1 {
		t.Fatalf("subscribe inside a batch: %d replayed on rebind, want 1", n)
	}
}

// The duplicate grade looks back 8 notifications; the gap looks back 64 and
// 10s, so a copy that arrives after a burst of others is still measured.
func TestSubscriptions_EVMDuplicateGapSeesPastTheGradeWindow(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xa"}`))
	frame := func(tx string) []byte {
		return []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xa","result":"` + tx + `"}}`)
	}
	if _, _, n := r.TranslateEndpointFrame(frame("0x1")); n.Gap != 0 {
		t.Fatalf("first sight: gap %v, want 0", n.Gap)
	}
	for i := 0; i < 20; i++ {
		r.TranslateEndpointFrame(frame(fmt.Sprintf("0x%x", 100+i)))
	}
	// A pause before each repeat: two calls inside one clock tick measure a
	// gap of exactly 0, which reads as "first sight".
	time.Sleep(time.Millisecond)
	_, _, late := r.TranslateEndpointFrame(frame("0x1"))
	if late.Kind != qos.NotificationOK || late.Gap <= 0 {
		t.Fatalf("repeat after 20 others: %+v, want graded ok with a gap", late)
	}
	time.Sleep(time.Millisecond)
	_, _, again := r.TranslateEndpointFrame(frame("0x1"))
	if again.Kind != qos.NotificationDuplicate || again.Gap <= 0 || again.Gap > time.Second {
		t.Fatalf("immediate repeat: %+v, want duplicate with a small gap", again)
	}
}
