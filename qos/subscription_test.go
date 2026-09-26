package qos

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// fakeClassifier speaks a tiny dialect: client frames "sub:<id>",
// "unsub:<subid>", "unsuball"; endpoint frames "ok:<reqid>:<subid>",
// "err:<reqid>", "data:<subid>".
type fakeClassifier struct{}

func (fakeClassifier) ClassifyClientFrame(data []byte) ClientFrameInfo {
	f := string(data)
	switch {
	case strings.HasPrefix(f, "sub:"):
		return ClientFrameInfo{Action: SubscriptionSubscribe, RequestID: f[4:], Method: "subscribe", Topic: "heads"}
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
	case parts[0] == "data" && (len(parts) == 2 || len(parts) == 3):
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

// LastActivity is what a stall watchdog measures from: the later of the last
// notification and the last subscribe acknowledgement, so a subscription that
// was just established is not "stalled" before its first event could arrive.
func TestSubscriptionRegistry_LastActivity(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	if !r.LastActivity().IsZero() {
		t.Fatal("nothing has happened yet")
	}
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("ok:1:s1"))
	established := r.LastActivity()
	if established.IsZero() {
		t.Fatal("an ack is activity")
	}
	time.Sleep(2 * time.Millisecond)
	r.TranslateEndpointFrame([]byte("data:s1"))
	if !r.LastActivity().After(established) {
		t.Fatal("a notification must move LastActivity forward")
	}
}

// Frames are "data:<subid>:<payload>"; the payload is what makes two
// notifications for one subscription the same or different.
func TestSubscriptionRegistry_GradesNotifications(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("ok:1:s1"))

	grade := func(frame string) Notification {
		t.Helper()
		out, fwd, note := r.TranslateEndpointFrameNote([]byte(frame))
		if !fwd || string(out) != frame {
			t.Fatalf("%q: a graded notification must still be forwarded unchanged, got %q fwd=%v", frame, out, fwd)
		}
		return note
	}

	if n := grade("data:s1:block100"); n.Kind != NotificationOK || n.Topic != "heads" {
		t.Errorf("first notification = %+v, want ok with the subscription's topic", n)
	}
	if n := grade("data:s1:block101"); n.Kind != NotificationOK {
		t.Errorf("a new payload = %+v, want ok", n)
	}
	if n := grade("data:s1:block100"); n.Kind != NotificationDuplicate {
		t.Errorf("a repeated payload = %+v, want duplicate", n)
	}
	if n := grade("data:nobody:block100"); n.Kind != NotificationUnsolicited {
		t.Errorf("a notification for a subscription never opened = %+v, want unsolicited", n)
	}
	if _, _, n := r.TranslateEndpointFrameNote([]byte("ok:9:s9")); n.Kind != NotificationNone {
		t.Errorf("a response is not a notification, got %+v", n)
	}

	// A frame still in flight when the client unsubscribed is the client's
	// timing, not the supplier pushing something unasked.
	r.TranslateClientFrame([]byte("unsub:s1"))
	if n := grade("data:s1:block102"); n.Kind != NotificationNone {
		t.Errorf("a notification after unsubscribe = %+v, want none (not judged)", n)
	}
}

// After a rebind the new supplier re-sends current state, and on chains
// with small integer ids that can be the old supplier's last frame byte for
// byte. That is not padding.
func TestSubscriptionRegistry_DuplicateWindowResetsOnReplay(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("ok:1:s1"))
	r.TranslateEndpointFrameNote([]byte("data:s1:block100"))

	replay := r.ReplayFrames()
	if len(replay) != 1 {
		t.Fatalf("ReplayFrames = %d frames, want 1", len(replay))
	}
	replayID := strings.TrimPrefix(string(replay[0]), "sub:")
	r.TranslateEndpointFrame([]byte("ok:" + replayID + ":s1"))

	if _, _, n := r.TranslateEndpointFrameNote([]byte("data:s1:block100")); n.Kind != NotificationOK {
		t.Errorf("the new supplier's first frame = %+v, want ok", n)
	}
}

// A subscription past the tracking cap is forwarded but unknown to the
// registry; its notifications must not read as unsolicited.
func TestSubscriptionRegistry_UntrackedSubscriptionsAreNotUnsolicited(t *testing.T) {
	r := NewSubscriptionRegistry(fakeClassifier{})
	for i := 0; i <= maxTrackedSubscriptions; i++ {
		r.TranslateClientFrame([]byte(fmt.Sprintf("sub:%d", i)))
	}
	if r.Dropped() == 0 {
		t.Fatal("expected the pending table to overflow")
	}
	if _, _, n := r.TranslateEndpointFrameNote([]byte("data:untracked:x")); n.Kind != NotificationNone {
		t.Errorf("notification on a connection past the cap = %+v, want none", n)
	}
}

// periodicClassifier is fakeClassifier whose "psub:<id>" subscribes are
// periodic.
type periodicClassifier struct{ fakeClassifier }

func (periodicClassifier) ClassifyClientFrame(data []byte) ClientFrameInfo {
	if f := string(data); strings.HasPrefix(f, "psub:") {
		return ClientFrameInfo{Action: SubscriptionSubscribe, RequestID: f[5:], Method: "subscribe", Topic: "heads", Periodic: true}
	}
	return fakeClassifier{}.ClassifyClientFrame(data)
}

// A stall watchdog may judge only a periodic feed. A connection holding only
// filtered feeds has no heartbeat, however long it has been silent.
func TestSubscriptionRegistry_Heartbeat(t *testing.T) {
	r := NewSubscriptionRegistry(periodicClassifier{})
	r.TranslateClientFrame([]byte("sub:1"))
	r.TranslateEndpointFrame([]byte("ok:1:logs"))
	r.TranslateEndpointFrame([]byte("data:logs:x"))
	if periodic, _ := r.Heartbeat(); periodic {
		t.Fatal("a connection with only a filtered feed must have no heartbeat")
	}

	r.TranslateClientFrame([]byte("psub:2"))
	r.TranslateEndpointFrame([]byte("ok:2:heads"))
	periodic, acked := r.Heartbeat()
	if !periodic || acked.IsZero() {
		t.Fatalf("after a periodic subscribe: periodic=%v last=%v, want the ack as the heartbeat", periodic, acked)
	}

	time.Sleep(time.Millisecond)
	r.TranslateEndpointFrame([]byte("data:logs:y"))
	if _, last := r.Heartbeat(); last != acked {
		t.Fatal("a filtered feed's notification must not move the heartbeat")
	}
	r.TranslateEndpointFrame([]byte("data:heads:h1"))
	if _, last := r.Heartbeat(); !last.After(acked) {
		t.Fatal("a periodic notification must move the heartbeat")
	}

	r.TranslateClientFrame([]byte("unsub:heads"))
	if periodic, _ := r.Heartbeat(); periodic {
		t.Fatal("after the periodic feed is unsubscribed the connection has no heartbeat")
	}
}
