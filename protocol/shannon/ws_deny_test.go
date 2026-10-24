package shannon

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
	"github.com/pokt-network/sage/tuning"
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
		name    string
		flip    string
		message string // the service's deny message, "" for unset
		closed  bool
		reason  string
	}{
		{"its flag turned on", featureflag.FlagWSDenyPendingTransactions, "", true, "subscription no longer served on this endpoint"},
		{"its flag turned on, a message set", featureflag.FlagWSDenyPendingTransactions, "Service limit reached, please redirect request to https://example.com/v1/eth", true,
			"Service limit reached, please redirect request to https://example.com/v1/eth"},
		{"another feed's flag turned on", featureflag.FlagWSDenyUnfilteredLogs, "", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTwoSuppliers(t, nil)
			f.r.deps.DenyMessage = func(domain.ServiceID) string { return tc.message }
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
			if !errors.As(err, &closeErr) || closeErr.Code != websocket.ClosePolicyViolation || closeErr.Text != tc.reason {
				t.Fatalf("read %s (%v); want a 1008 close with %q", msg, err, tc.reason)
			}
			if n := f.spy.clientFrames(); n != 2 || toEcho {
				t.Errorf("%d frames sent to suppliers, bound %s; want the 2 first ones and no rebind", n, bound)
			}
		})
	}
}

// The deny message is the service's override over the global one over the
// built-in wording, for both feeds' errors (whose codes stay) and the close
// reason; it is escaped into the JSON-RPC message and cut to 123 bytes on a
// character boundary for the close.
func TestWSDenyMessage(t *testing.T) {
	f := newTwoSuppliers(t, map[string]bool{featureflag.FlagWSDenyPendingTransactions: true, featureflag.FlagWSDenyUnfilteredLogs: true})
	store := tuning.NewStore()
	f.r.deps.DenyMessage = func(id domain.ServiceID) string { return store.String(tuning.KnobWSDenyMessage, id, "") }
	deny := f.r.subscribeDenial("eth")
	const (
		pending = `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}`
		allLogs = `{"jsonrpc":"2.0","id":2,"method":"eth_subscribe","params":["logs"]}`
	)
	answer := func(frame string) (code int, message string) {
		t.Helper()
		var r struct {
			Error struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(deny([]byte(frame)), &r); err != nil {
			t.Fatalf("answer to %s is not JSON: %v", frame, err)
		}
		return r.Error.Code, r.Error.Message
	}
	check := func(wantPending, wantLogs, wantClose string) {
		t.Helper()
		if code, msg := answer(pending); code != -32601 || msg != wantPending {
			t.Errorf("pending: %d %q, want -32601 %q", code, msg, wantPending)
		}
		if code, msg := answer(allLogs); code != -32602 || msg != wantLogs {
			t.Errorf("logs: %d %q, want -32602 %q", code, msg, wantLogs)
		}
		if got := f.r.denyCloseReason("eth"); got != wantClose {
			t.Errorf("close reason %q, want %q", got, wantClose)
		}
	}

	check("pending transaction subscriptions are not available on this endpoint",
		"logs subscriptions need an address or topics filter on this endpoint",
		"subscription no longer served on this endpoint")

	if err := store.Set(tuning.KnobWSDenyMessage, "", "use another endpoint"); err != nil {
		t.Fatal(err)
	}
	check("use another endpoint", "use another endpoint", "use another endpoint")

	escaped := "say \"hi\" \\ <a&b>\n\tfin é\x7f"
	if err := store.Set(tuning.KnobWSDenyMessage, "eth", escaped); err != nil {
		t.Fatal(err)
	}
	check(escaped, escaped, escaped)
	if got := f.r.denyCloseReason("other"); got != "use another endpoint" {
		t.Errorf("another service's close reason %q, want the global message", got)
	}

	long := strings.Repeat("é", 100) // 200 bytes
	if err := store.Set(tuning.KnobWSDenyMessage, "eth", long); err != nil {
		t.Fatal(err)
	}
	if got := f.r.denyCloseReason("eth"); len(got) > maxCloseReason || !utf8.ValidString(got) || !strings.HasPrefix(long, got) || len(got) < maxCloseReason-1 {
		t.Errorf("close reason is %d bytes (valid UTF-8 %v), want the longest prefix up to %d", len(got), utf8.ValidString(got), maxCloseReason)
	}
	if _, msg := answer(pending); msg != long {
		t.Errorf("the JSON-RPC message was cut: %d bytes, want %d", len(msg), len(long))
	}
}
