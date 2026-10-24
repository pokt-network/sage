package shannon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/featureflag"
)

// A subscribe to a feed whose flag is on is answered by the gateway with an
// error on the client's own id, and no supplier is sent anything for it, nor
// for a batch holding one; each flag refuses only its own feed, a filtered
// logs subscribe is sent on, and with both flags off every subscribe goes to
// the supplier as before.
func TestWSOpen_DeniedSubscribesNeverReachASupplier(t *testing.T) {
	const (
		pending    = `{"jsonrpc":"2.0","id":11,"method":"eth_subscribe","params":["newPendingTransactions"]}`
		allLogs    = `{"jsonrpc":"2.0","id":"a","method":"eth_subscribe","params":["logs",{}]}`
		someLogs   = `{"jsonrpc":"2.0","id":13,"method":"eth_subscribe","params":["logs",{"address":"0x55d398326f99059ff775485246999027b3197955"}]}`
		batch      = `[{"jsonrpc":"2.0","id":21,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":22,"method":"eth_subscribe","params":["newPendingTransactions"]}]`
		plainBatch = `[{"jsonrpc":"2.0","id":31,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":32,"method":"eth_subscribe","params":["newHeads"]}]`
		pendingErr = `{"jsonrpc":"2.0","id":11,"error":{"code":-32601,"message":"pending transaction subscriptions are not available on this endpoint"}}`
		allLogsErr = `{"jsonrpc":"2.0","id":"a","error":{"code":-32602,"message":"logs subscriptions need an address or topics filter on this endpoint"}}`
		batchErr   = `[{"jsonrpc":"2.0","id":21,"error":{"code":-32600,"message":"batch not sent: it holds a subscription this endpoint does not serve"}},` +
			`{"jsonrpc":"2.0","id":22,"error":{"code":-32601,"message":"pending transaction subscriptions are not available on this endpoint"}}]`
	)
	type answered struct{ frame, answer string }
	for _, tc := range []struct {
		name    string
		flags   map[string]bool
		denied  []answered
		sentOn  []string
		metrics []string
	}{
		{"pending transactions", map[string]bool{featureflag.FlagWSDenyPendingTransactions: true},
			[]answered{{pending, pendingErr}, {batch, batchErr}}, []string{allLogs, someLogs, plainBatch},
			[]string{"eth|newPendingTransactions", "eth|newPendingTransactions"}},
		{"unfiltered logs", map[string]bool{featureflag.FlagWSDenyUnfilteredLogs: true},
			[]answered{{allLogs, allLogsErr}}, []string{pending, someLogs, batch}, []string{"eth|logs"}},
		{"flags off", nil, nil, []string{pending, allLogs, someLogs, batch}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTwoSuppliers(t, tc.flags)
			client := f.dial()
			send := func(frame string) {
				if err := client.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
					t.Fatal(err)
				}
			}
			for _, d := range tc.denied {
				send(d.frame)
				_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
				_, got, err := client.ReadMessage()
				if err != nil {
					t.Fatalf("no answer to the denied subscribe: %v", err)
				}
				if string(got) != d.answer {
					t.Errorf("answered %s, want %s", got, d.answer)
				}
			}
			// The answers above came back after their frames were routed, so
			// anything sent upstream for them would already be counted.
			if n := f.spy.clientFrames(); n != 0 {
				t.Fatalf("%d frames sent to a supplier for denied subscribes, want 0", n)
			}
			for _, frame := range tc.sentOn {
				send(frame)
			}
			deadline := time.Now().Add(3 * time.Second)
			for f.spy.clientFrames() < len(tc.sentOn) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if n := f.spy.clientFrames(); n < len(tc.sentOn) {
				t.Errorf("%d frames sent to a supplier, want at least %d", n, len(tc.sentOn))
			}
			f.spy.mu.Lock()
			defer f.spy.mu.Unlock()
			if strings.Join(f.spy.denied, ",") != strings.Join(tc.metrics, ",") {
				t.Errorf("denials counted %v, want %v", f.spy.denied, tc.metrics)
			}
		})
	}
}

// A flag turned on after a client subscribed reaches the subscription at the
// next rebind: the client is closed with 1008 and no supplier is sent the
// replay. With only another feed's flag on, the rebind goes on as before.
func TestWSOpen_RebindClosesASubscriptionNowDenied(t *testing.T) {
	const (
		subscribe = `{"jsonrpc":"2.0","id":9,"method":"eth_subscribe","params":["newPendingTransactions"]}`
		read      = `{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`
	)
	for _, tc := range []struct {
		name   string
		flip   string
		closed bool
	}{
		{"its flag turned on", featureflag.FlagWSDenyPendingTransactions, true},
		{"another feed's flag turned on", featureflag.FlagWSDenyUnfilteredLogs, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTwoSuppliers(t, nil)
			client := f.dial()
			for _, frame := range []string{subscribe, read} {
				if err := client.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(3 * time.Second)
			for f.spy.clientFrames() < 2 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if err := f.flags.Set(context.Background(), tc.flip, true); err != nil {
				t.Fatal(err)
			}
			// The silent supplier never answers the read: past the relay
			// timeout the connection rebinds.
			_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, msg, err := client.ReadMessage()

			f.spy.mu.Lock()
			bound := strings.Join(f.spy.bound, ",")
			f.spy.mu.Unlock()
			toEcho := strings.Contains(bound, f.echoEP.Operator())
			if !tc.closed {
				if err != nil || string(msg) != `{"jsonrpc":"2.0","id":7,"result":"0x1"}` || !toEcho {
					t.Fatalf("read %s (%v), bound %s; want the next supplier's answer after a rebind", msg, err, bound)
				}
				return
			}
			var closeErr *websocket.CloseError
			if !errors.As(err, &closeErr) || closeErr.Code != websocket.ClosePolicyViolation || closeErr.Text != "subscription no longer served on this endpoint" {
				t.Fatalf("read %s (%v); want a 1008 close", msg, err)
			}
			if n := f.spy.clientFrames(); n != 2 || toEcho {
				t.Errorf("%d frames sent to suppliers, bound %s; want the 2 first ones and no rebind", n, bound)
			}
		})
	}
}
