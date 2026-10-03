package qos

import (
	"fmt"
	"strings"
	"testing"
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
// one answered, one never sent, one refused twice, and one the replaced
// supplier was still holding at the rebind are not.
func TestSubscriptionRegistry_ReissueReplaysARefusedRequestAfterTheSubscriptions(t *testing.T) {
	r := NewSubscriptionRegistry(rpcClassifier{})
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":1,"method":"subscribe","params":["newHeads"]}`))
	r.TranslateEndpointFrame([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xs"}`))

	call := `{"jsonrpc":"2.0","id":7,"method":"eth_call","params":[]}`
	r.TranslateClientFrame([]byte(call))
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":8,"method":"eth_call","params":[]}`))
	r.TranslateClientFrame([]byte(`{"jsonrpc":"2.0","id":9,"method":"eth_call","params":[]}`))
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

	frames := r.ReplayFrames()
	if len(frames) != 2 || JSONRPCMethod(frames[0]) != "subscribe" || string(frames[1]) != call {
		t.Fatalf("replay %q; want the subscribe, then the refused request as sent", frames)
	}
	if r.Reissue(refusal("9")) {
		t.Fatal("a request the replaced supplier held is not in flight on the next one")
	}
	if len(r.ReplayFrames()) != 1 {
		t.Fatal("a reissued request must be replayed once")
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
