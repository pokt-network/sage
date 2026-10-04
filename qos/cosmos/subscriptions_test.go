package cosmos

import (
	"testing"

	"github.com/pokt-network/sage/qos"
)

// Frames exactly as the Pocket beta chain's CometBFT sent them through SAGE
// on 2026-08-31: the ack is {"result":{}}, and every event reuses id 1.
func TestSubscriptions_CometBFTRoundTrip(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != "1" || a[0].Method != "subscribe" {
		t.Fatalf("Active = %+v", a)
	}
	if _, _, n := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{}}}}`)); n.Kind != qos.NotificationOK {
		t.Fatalf("an event with the subscribe's id must count as data, got %+v", n)
	}
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"unsubscribe","params":{"query":"tm.event='NewBlock'"}}`))
	if len(r.Active()) != 0 {
		t.Fatal("unsubscribe must clear the subscription")
	}
}

func TestSubscriptions_CometBFTEmptyResultToPlainCallOpensNothing(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":5,"method":"health"}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":5,"result":{}}`))
	if len(r.Active()) != 0 {
		t.Fatal("health's empty result must not open a subscription")
	}
}

// CometBFT after a rebind: events arrive under the replay id and must go to
// the client under the id it subscribed with.
func TestSubscriptions_CometBFTRebindRewritesEventID(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	frames := r.Replay().Frames
	if len(frames) != 1 || string(frames[0]) != `{"jsonrpc":"2.0","id":"sage-replay-1","method":"subscribe","params":{"query":"tm.event='NewBlock'"}}` {
		t.Fatalf("replay = %q", frames)
	}
	if _, fwd, _ := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":"sage-replay-1","result":{}}`)); fwd {
		t.Fatal("replay ack must be consumed")
	}
	out, fwd, _ := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":"sage-replay-1","result":{"query":"tm.event='NewBlock'","data":{"type":"x","value":{}}}}`))
	if !fwd || string(out) != `{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"x","value":{}}}}` {
		t.Fatalf("event = %q", out)
	}
}

// Only the block events fire on every block; a Tx query fires on matches and
// its silence proves nothing about the supplier.
func TestSubscriptions_CometBFTPeriodicTopics(t *testing.T) {
	p := &Plugin{}
	for frame, want := range map[string]struct {
		topic    string
		periodic bool
	}{
		`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`:                       {"NewBlock", true},
		`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":["tm.event = 'NewBlockHeader'"]}`:                       {"NewBlockHeader", true},
		`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='Tx' AND message.sender='pokt1x'"}}`: {"Tx", false},
		`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"message.sender='pokt1x'"}}`:                   {"", false},
	} {
		info := p.ClassifyClientFrame([]byte(frame))
		if info.Topic != want.topic || info.Periodic != want.periodic {
			t.Errorf("%s: topic=%q periodic=%v, want %q %v", frame, info.Topic, info.Periodic, want.topic, want.periodic)
		}
	}
}
