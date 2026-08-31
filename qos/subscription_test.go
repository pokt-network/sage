package qos

import (
	"fmt"
	"strings"
	"testing"
)

// fakeClassifier speaks a tiny dialect: client frames "sub:<id>",
// "unsub:<subid>", "unsuball"; endpoint frames "ok:<reqid>:<subid>",
// "err:<reqid>", "data:<subid>".
type fakeClassifier struct{}

func (fakeClassifier) ClassifyClientFrame(data []byte) ClientFrameInfo {
	f := string(data)
	switch {
	case strings.HasPrefix(f, "sub:"):
		return ClientFrameInfo{Action: SubscriptionSubscribe, RequestID: f[4:], Method: "subscribe"}
	case strings.HasPrefix(f, "unsub:"):
		return ClientFrameInfo{Action: SubscriptionUnsubscribe, SubscriptionID: f[6:]}
	case f == "unsuball":
		return ClientFrameInfo{Action: SubscriptionUnsubscribeAll}
	}
	return ClientFrameInfo{}
}

// EncodeReplayID: the fake dialect writes ids bare, not as JSON strings.
func (fakeClassifier) EncodeReplayID(id string) string { return id }

func (fakeClassifier) ClassifyEndpointFrame(data []byte) EndpointFrameInfo {
	parts := strings.Split(string(data), ":")
	switch {
	case parts[0] == "ok" && len(parts) == 3:
		return EndpointFrameInfo{Kind: EndpointFrameResponse, RequestID: parts[1], SubscriptionID: parts[2]}
	case parts[0] == "err" && len(parts) == 2:
		return EndpointFrameInfo{Kind: EndpointFrameResponse, RequestID: parts[1], IsError: true}
	case parts[0] == "data" && len(parts) == 2:
		return EndpointFrameInfo{Kind: EndpointFrameNotification, SubscriptionID: parts[1]}
	}
	return EndpointFrameInfo{}
}

func TestSubscriptionRegistry_SubscribeBecomesActiveOnResponse(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	r.TranslateClientFrame([]byte("sub:1"))
	if r.HasActive() {
		t.Fatal("a subscribe is not active until the endpoint answers")
	}
	r.TranslateEndpointFrame([]byte("ok:1:0xabc"))
	if !r.HasActive() {
		t.Fatal("a successful subscribe response must make the subscription active")
	}
	active := r.Active()
	if len(active) != 1 || active[0].ID != "0xabc" || active[0].Method != "subscribe" || string(active[0].Request) != "sub:1" {
		t.Fatalf("Active = %+v; want the original request kept for replay", active)
	}
}

func TestSubscriptionRegistry_ErrorResponseOpensNothing(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("err:1"))
	if r.HasActive() {
		t.Fatal("a failed subscribe must not be active")
	}
	// And the pending entry is gone: a later unrelated response with the
	// same id is not a subscribe answer.
	r.TranslateEndpointFrame([]byte("ok:1:late"))
	if r.HasActive() {
		t.Fatal("a response after the subscribe failed must not resurrect it")
	}
}

func TestSubscriptionRegistry_UnsubscribeAndUnsubscribeAll(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	for i := 1; i <= 3; i++ {
		r.TranslateClientFrame([]byte(fmt.Sprintf("sub:%d", i)))
		r.TranslateEndpointFrame([]byte(fmt.Sprintf("ok:%d:s%d", i, i)))
	}
	r.TranslateClientFrame([]byte("unsub:s2"))
	if n := len(r.Active()); n != 2 {
		t.Fatalf("after one unsubscribe: %d active, want 2", n)
	}
	r.TranslateClientFrame([]byte("unsuball"))
	if r.HasActive() {
		t.Fatal("unsubscribe_all must clear every subscription")
	}
}

func TestSubscriptionRegistry_NotificationMarksData(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	r.TranslateEndpointFrame([]byte("data:ghost"))
	if !r.LastData().IsZero() {
		t.Fatal("data for a subscription that was never established is not data")
	}
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("ok:1:s1"))
	r.TranslateEndpointFrame([]byte("data:s1"))
	if r.LastData().IsZero() {
		t.Fatal("a notification for a live subscription must update LastData")
	}
}

func TestSubscriptionRegistry_Bounded(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	for i := 0; i < maxTrackedSubscriptions+10; i++ {
		r.TranslateClientFrame([]byte(fmt.Sprintf("sub:%d", i)))
	}
	if len(r.pending) != maxTrackedSubscriptions {
		t.Fatalf("pending = %d, want the cap %d", len(r.pending), maxTrackedSubscriptions)
	}
	if r.Dropped() != 10 {
		t.Fatalf("dropped = %d, want 10", r.Dropped())
	}
}

func TestSubscriptionRegistry_NilIsInert(t *testing.T) {
	var r *SubscriptionRegistry
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("ok:1:s1"))
	if r.HasActive() || r.Active() != nil || !r.LastData().IsZero() {
		t.Fatal("a nil registry must observe nothing and report nothing")
	}
	inert := NewSubscriptionRegistry(nil)
	inert.TranslateClientFrame([]byte("sub:1"))
	inert.TranslateEndpointFrame([]byte("ok:1:s1"))
	if inert.HasActive() {
		t.Fatal("a registry with no classifier must stay empty")
	}
}

func TestJSONRPCRequestID_RawKeepsStringAndNumberDistinct(t *testing.T) {
	if a, b := JSONRPCRequestID([]byte(`{"id":1}`)), JSONRPCRequestID([]byte(`{"id":"1"}`)); a == b {
		t.Fatalf("numeric and string ids must differ, both %q", a)
	}
	if got := JSONRPCRequestID([]byte(`{"id":null}`)); got != "" {
		t.Fatalf("null id = %q, want empty", got)
	}
	if got := JSONRPCRequestID([]byte(`{"method":"x"}`)); got != "" {
		t.Fatalf("missing id = %q, want empty", got)
	}
}
