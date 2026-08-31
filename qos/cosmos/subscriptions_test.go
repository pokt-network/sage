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
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{}}}}`))
	if r.LastData().IsZero() {
		t.Fatal("an event with the subscribe's id must count as data")
	}
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"unsubscribe","params":{"query":"tm.event='NewBlock'"}}`))
	if r.HasActive() {
		t.Fatal("unsubscribe must clear the subscription")
	}
}

func TestSubscriptions_CometBFTEmptyResultToPlainCallOpensNothing(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":5,"method":"health"}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":5,"result":{}}`))
	if r.HasActive() {
		t.Fatal("health's empty result must not open a subscription")
	}
}

// CometBFT after a rebind: events arrive under the replay id and must go to
// the client under the id it subscribed with.
func TestSubscriptions_CometBFTRebindRewritesEventID(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	frames := r.ReplayFrames()
	if len(frames) != 1 || string(frames[0]) != `{"jsonrpc":"2.0","id":"sage-replay-1","method":"subscribe","params":{"query":"tm.event='NewBlock'"}}` {
		t.Fatalf("replay = %q", frames)
	}
	if _, fwd := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":"sage-replay-1","result":{}}`)); fwd {
		t.Fatal("replay ack must be consumed")
	}
	out, fwd := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":"sage-replay-1","result":{"query":"tm.event='NewBlock'","data":{"type":"x","value":{}}}}`))
	if !fwd || string(out) != `{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"x","value":{}}}}` {
		t.Fatalf("event = %q", out)
	}
}
