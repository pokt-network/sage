package evm

import (
	"testing"

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
	if _, _, n := r.TranslateEndpointFrameNote(head); n.Kind != qos.NotificationOK || n.Topic != "newHeads" {
		t.Fatalf("first head = %+v, want ok on newHeads", n)
	}
	if r.LastData().IsZero() {
		t.Fatal("newHeads notification must count as data")
	}
	if _, _, n := r.TranslateEndpointFrameNote(head); n.Kind != qos.NotificationDuplicate {
		t.Fatalf("the same head again = %+v, want duplicate", n)
	}
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":8,"method":"eth_unsubscribe","params":["0xcd0c3e8af590364c09d0fa6a1210faf5"]}`))
	if r.HasActive() {
		t.Fatal("eth_unsubscribe must close the subscription")
	}
}

func TestSubscriptions_EVMErrorAndPlainCalls(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["bogus"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid subscription"}}`))
	if r.HasActive() {
		t.Fatal("a rejected subscribe is not a subscription")
	}
	// Ordinary request/response on the same socket: nothing to track.
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":2,"result":"0x10"}`))
	if r.HasActive() {
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
	if out, fwd := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":"sage-replay-1","result":"0xnew"}`)); fwd {
		t.Fatalf("replay ack must be consumed, got %q", out)
	}
	out, fwd := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xnew","result":{"number":"0x2"}}}`))
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
