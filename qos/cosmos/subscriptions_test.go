package cosmos

import (
	"testing"

	"github.com/pokt-network/sage/qos"
)

// Frames exactly as the Pocket beta chain's CometBFT sent them through SAGE
// on 2026-08-31: the ack is {"result":{}}, and every event reuses id 1.
func TestSubscriptions_CometBFTRoundTrip(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":{"query":"tm.event='NewBlock'"}}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != "1" || a[0].Method != "subscribe" {
		t.Fatalf("Active = %+v", a)
	}
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{}}}}`))
	if r.LastData().IsZero() {
		t.Fatal("an event with the subscribe's id must count as data")
	}
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"unsubscribe","params":{"query":"tm.event='NewBlock'"}}`))
	if r.HasActive() {
		t.Fatal("unsubscribe must clear the subscription")
	}
}

func TestSubscriptions_CometBFTEmptyResultToPlainCallOpensNothing(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":5,"method":"health"}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","id":5,"result":{}}`))
	if r.HasActive() {
		t.Fatal("health's empty result must not open a subscription")
	}
}
