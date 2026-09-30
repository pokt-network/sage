package solana

import (
	"testing"

	"github.com/pokt-network/sage/qos"
)

// Frames as the Solana RPC emits them: integer subscription ids.
func TestSubscriptions_SolanaRoundTrip(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","result":23784,"id":1}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != "23784" || a[0].Method != "slotSubscribe" || a[0].Topic != "slotSubscribe" {
		t.Fatalf("Active = %+v", a)
	}
	if _, _, n := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"slotNotification","params":{"result":{"slot":75},"subscription":23784}}`)); n.Kind != qos.NotificationOK {
		t.Fatalf("slotNotification must count as data, got %+v", n)
	}
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"slotUnsubscribe","params":[23784]}`))
	if len(r.Active()) != 0 {
		t.Fatal("slotUnsubscribe must close the subscription")
	}
}

func TestSubscriptions_SolanaPlainCallIgnored(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":3,"method":"getSlot"}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","result":1234,"id":3}`))
	if len(r.Active()) != 0 {
		t.Fatal("getSlot's numeric result is not a subscription id")
	}
}

func TestSubscriptions_SolanaRebindTranslation(t *testing.T) {
	r := qos.NewSubscriptionRegistry(&Plugin{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","result":10,"id":1}`))
	frames := r.ReplayFrames()
	if len(frames) != 1 {
		t.Fatalf("replay = %q", frames)
	}
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","result":99,"id":"sage-replay-1"}`))
	out, _, _ := r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","method":"slotNotification","params":{"result":{"slot":75},"subscription":99}}`))
	if string(out) != `{"jsonrpc":"2.0","method":"slotNotification","params":{"result":{"slot":75},"subscription":10}}` {
		t.Fatalf("notification = %q", out)
	}
	if got := r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":2,"method":"slotUnsubscribe","params":[10]}`)); string(got) != `{"jsonrpc":"2.0","id":2,"method":"slotUnsubscribe","params":[99]}` {
		t.Fatalf("unsubscribe = %q", got)
	}
}

func TestSubscriptions_SolanaPeriodicTopics(t *testing.T) {
	p := &Plugin{}
	for method, want := range map[string]bool{"slotSubscribe": true, "rootSubscribe": true, "logsSubscribe": false, "accountSubscribe": false} {
		if info := p.ClassifyClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"` + method + `"}`)); info.Periodic != want {
			t.Errorf("%s: periodic=%v, want %v", method, info.Periodic, want)
		}
	}
}
