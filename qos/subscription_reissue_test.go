package qos

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// rpcClassifier reads plain JSON-RPC: method "subscribe" opens a
// subscription, any other request is one the client waits on, and an
// endpoint frame with an id answers one.
type rpcClassifier struct{}

func (rpcClassifier) ClassifyClientFrame(data []byte) ClientFrameInfo {
	if JSONRPCMethod(data) == "subscribe" {
		return ClientFrameInfo{Action: SubscriptionSubscribe, RequestID: JSONRPCRequestID(data), Method: "subscribe"}
	}
	return ClientFrameInfo{}
}

func (rpcClassifier) ClassifyEndpointFrame(data []byte) EndpointFrameInfo {
	id := JSONRPCRequestID(data)
	if id == "" {
		return EndpointFrameInfo{}
	}
	return EndpointFrameInfo{Kind: EndpointFrameResponse, RequestID: id, SubscriptionID: JSONRPCResultScalar(data), IsError: JSONRPCHasError(data)}
}

func refusal(id string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":` + id + `,"error":{"code":-32029,"message":"rate limit exceeded"}}`)
}

// A refused request goes again to the next supplier after the
// subscriptions, as the client sent it, and only a request still in flight:
// one answered, one never sent, and one refused twice are not. A request the
// replaced supplier was still holding goes too, after the refused one, and is
// counted lost.
func TestSubscriptionRegistry_ReissueReplaysARefusedRequestAfterTheSubscriptions(t *testing.T) {
	r := NewSubscriptionRegistry(rpcClassifier{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":["newHeads"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xs"}`))

	call := `{"jsonrpc":"2.0","id":7,"method":"eth_call","params":[]}`
	held := `{"jsonrpc":"2.0","id":9,"method":"eth_call","params":[]}`
	r.TranslateClientFrame([]byte(call))
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":8,"method":"eth_call","params":[]}`))
	r.TranslateClientFrame([]byte(held))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":8,"result":"0x"}`))

	if r.Reissue(refusal("8")) || r.Reissue(refusal("99")) {
		t.Fatal("an answered or unknown request must not be reissued")
	}
	if !r.Reissue(refusal("7")) {
		t.Fatal("the refused request in flight was not taken")
	}
	if r.Reissue(refusal("7")) {
		t.Fatal("a request already taken must not be queued twice")
	}

	rp := r.Replay()
	if len(rp.Frames) != 3 || JSONRPCMethod(rp.Frames[0]) != "subscribe" || string(rp.Frames[1]) != call || string(rp.Frames[2]) != held || rp.Lost != 1 {
		t.Fatalf("replay %q lost %d; want the subscribe, the refused request, then the held one, one lost", rp.Frames, rp.Lost)
	}
	if r.Reissue(refusal("9")) {
		t.Fatal("a replayed request is in flight again only once it is sent")
	}
	if len(r.Replay().Frames) != 1 {
		t.Fatal("a reissued request must be replayed once")
	}
}

// A rebind owes the client every request in flight: reads go to the next
// supplier oldest first, an unanswered subscribe goes again under the client's
// own id, a write is handed back to answer with an error rather than sent
// twice, and a batch's requests are not taken apart.
func TestSubscriptionRegistry_ReplayOwesEveryRequestInFlight(t *testing.T) {
	r := NewSubscriptionRegistry(rpcClassifier{})
	first := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xa","latest"]}`
	write := `{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0xf8"]}`
	sub := `{"jsonrpc":"2.0","id":3,"method":"subscribe","params":["logs"]}`
	second := `{"jsonrpc":"2.0","id":4,"method":"eth_call","params":[]}`
	r.TranslateClientFrame([]byte(first))
	time.Sleep(time.Millisecond)
	r.TranslateClientFrame([]byte(write))
	r.TranslateClientFrame([]byte(sub))
	time.Sleep(time.Millisecond)
	r.TranslateClientFrame([]byte(second))
	r.TranslateClientFrame([]byte(`[{"jsonrpc":"2.0","id":5,"method":"eth_chainId"},{"jsonrpc":"2.0","id":6,"method":"eth_blockNumber"}]`))
	if r.OldestInFlight().IsZero() {
		t.Fatal("requests in flight must report when the oldest went out")
	}

	rp := r.Replay()
	want := []string{sub, first, second}
	if len(rp.Frames) != len(want) || rp.Lost != 2 {
		t.Fatalf("replay %q lost %d; want %q, two lost", rp.Frames, rp.Lost, want)
	}
	for i := range want {
		if string(rp.Frames[i]) != want[i] {
			t.Fatalf("replay[%d] = %s, want %s", i, rp.Frames[i], want[i])
		}
	}
	if len(rp.Abandoned) != 1 || string(rp.Abandoned[0]) != write {
		t.Fatalf("abandoned %q, want the write alone", rp.Abandoned)
	}
	if !r.OldestInFlight().IsZero() {
		t.Fatal("nothing is in flight on the next supplier until it is sent")
	}

	// Sent again, the subscribe is pending once, and its ack opens it.
	for _, f := range rp.Frames {
		r.TranslateClientFrame(f)
	}
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":3,"result":"0xnew"}`))
	if a := r.Active(); len(a) != 1 || a[0].ID != `"0xnew"` {
		t.Fatalf("active %+v; the replayed subscribe's ack must open it", a)
	}
}

// Past either cap a request is not remembered, and its refusal goes to the
// client.
func TestSubscriptionRegistry_ReissueIsBounded(t *testing.T) {
	r := NewSubscriptionRegistry(rpcClassifier{})
	for i := range maxInflightRequests + 1 {
		r.TranslateClientFrame([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_call"}`, i)))
	}
	if r.Reissue(refusal(fmt.Sprint(maxInflightRequests))) {
		t.Error("a request past the count cap was remembered")
	}

	r = NewSubscriptionRegistry(rpcClassifier{})
	big := `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x` + strings.Repeat("ab", maxInflightBytes) + `"]}`
	r.TranslateClientFrame([]byte(big))
	if r.Reissue(refusal("1")) {
		t.Error("a request past the byte cap was remembered")
	}
}
