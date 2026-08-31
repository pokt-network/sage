package solana

import (
	"testing"

	"github.com/pokt-network/sage/qos"
)

// Frames as the Solana RPC emits them: integer subscription ids.
func TestSubscriptions_SolanaRoundTrip(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","result":23784,"id":1}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != "23784" || a[0].Method != "slotSubscribe" {
		t.Fatalf("Active = %+v", a)
	}
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"slotNotification","params":{"result":{"slot":75},"subscription":23784}}`))
	if r.LastData().IsZero() {
		t.Fatal("slotNotification must count as data")
	}
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"slotUnsubscribe","params":[23784]}`))
	if r.HasActive() {
		t.Fatal("slotUnsubscribe must close the subscription")
	}
}

func TestSubscriptions_SolanaPlainCallIgnored(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.ObserveClientFrame([]byte(`{"jsonrpc":"2.0","id":3,"method":"getSlot"}`))
	r.ObserveEndpointFrame([]byte(`{"jsonrpc":"2.0","result":1234,"id":3}`))
	if r.HasActive() {
		t.Fatal("getSlot's numeric result is not a subscription id")
	}
}
