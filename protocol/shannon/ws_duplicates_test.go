package shannon

import (
	"context"
	"testing"
	"time"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/qos"
	"github.com/pokt-network/sage/qos/evm"
)

// A supplier's repeat of a notification it already sent is kept from the
// client behind ws_drop_duplicates; off, it is forwarded as before.
func TestWSProcessor_DropsDuplicateNotifications(t *testing.T) {
	for _, drop := range []bool{true, false} {
		p, _, _, fn := buildProcessorFixture()
		proc := newWSMessageProcessor(context.Background(), p,
			&sessiontypes.SessionHeader{ServiceId: "eth", SessionId: "s-1", SessionEndBlockHeight: 200},
			"pokt1supplier", "pokt1supplier-https://rel001.op-alpha.example",
			&apptypes.Application{Address: "pokt1app"}, nil)
		proc.subs = qos.NewSubscriptionRegistry(&evm.Plugin{})
		proc.duplicates = &wsDuplicates{drop: func() bool { return drop }, penalize: func() bool { return false }, record: func() {}}
		endpoint := func(payload string) []byte {
			t.Helper()
			fn.validateResponse = &servicetypes.RelayResponse{Payload: []byte(payload)}
			out, err := proc.ProcessEndpointMessage([]byte(`wire`))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
		if _, err := proc.ProcessClientMessage([]byte(`{"jsonrpc":"2.0","id":9,"method":"eth_subscribe","params":["newPendingTransactions"]}`)); err != nil {
			t.Fatal(err)
		}
		endpoint(`{"jsonrpc":"2.0","id":9,"result":"0xsub"}`)
		tx := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsub","result":"0xabc"}}`
		if out := endpoint(tx); out == nil {
			t.Fatalf("drop=%v: the first copy was not forwarded", drop)
		}
		if out := endpoint(tx); (out == nil) != drop {
			t.Fatalf("drop=%v: the repeat forwarded=%v", drop, out != nil)
		}
	}
}

// A supplier is charged once per period in which more than 1% of at least
// 500 notifications were repeats, and only behind ws_duplicate_penalty.
func TestWSDuplicates_PenalizesARepeatingSupplier(t *testing.T) {
	for name, tc := range map[string]struct {
		notes, dups int
		on          bool
		want        int
	}{
		"2% of 600":      {600, 12, true, 1},
		"0.5% of 600":    {600, 3, true, 0},
		"2% of 400":      {400, 8, true, 0},
		"2% of 600, off": {600, 12, false, 0},
	} {
		charged := 0
		d := &wsDuplicates{drop: func() bool { return false }, penalize: func() bool { return tc.on }, record: func() { charged++ }}
		start := time.Unix(0, 0)
		for i := 0; i < tc.notes; i++ {
			d.observe(i < tc.dups, start.Add(time.Duration(i)*time.Millisecond))
		}
		d.observe(false, start.Add(dupPeriod+time.Second)) // closes the period
		if charged != tc.want {
			t.Errorf("%s: charged %d, want %d", name, charged, tc.want)
		}
	}
}
