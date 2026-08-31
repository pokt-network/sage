package qos

import (
	"strings"
	"testing"
)

// spanClassifier is fakeClassifier with spans: frames are
// "sub:<id>" / "unsub:<subid>" / "ok:<req>:<sub>" / "data:<sub>", and the
// span of the id is where it sits in the text, so a rewrite can be checked
// by reading the frame back.
type spanClassifier struct{ fakeClassifier }

func (spanClassifier) ClassifyClientFrame(data []byte) ClientFrameInfo {
	info := fakeClassifier{}.ClassifyClientFrame(data)
	if info.Action == SubscriptionUnsubscribe {
		info.SubscriptionIDSpan = Span{Index: 6, Len: len(data) - 6}
	}
	return info
}

func (spanClassifier) ClassifyEndpointFrame(data []byte) EndpointFrameInfo {
	info := fakeClassifier{}.ClassifyEndpointFrame(data)
	if info.Kind == EndpointFrameNotification {
		info.SubscriptionIDSpan = Span{Index: 5, Len: len(data) - 5}
	}
	return info
}

// RequestIDSpan for the fake dialect: "sub:<id>" → the id after the colon.
func (spanClassifier) RequestIDSpan(data []byte) Span {
	if strings.HasPrefix(string(data), "sub:") {
		return Span{Index: 4, Len: len(data) - 4}
	}
	return Span{}
}

func establish(t *testing.T, r *SubscriptionRegistry, req, sub string) {
	t.Helper()
	r.TranslateClientFrame([]byte("sub:" + req))
	if _, fwd := r.TranslateEndpointFrame([]byte("ok:" + req + ":" + sub)); !fwd {
		t.Fatal("the client's own subscribe ack must be forwarded")
	}
}

func TestSubscriptionRegistry_ReplayFramesCarryFreshIDs(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "s1")
	establish(t, r, "2", "s2")

	frames := r.ReplayFrames()
	if len(frames) != 2 {
		t.Fatalf("ReplayFrames = %d frames, want 2", len(frames))
	}
	for _, f := range frames {
		if !strings.HasPrefix(string(f), "sub:sage-replay-") {
			t.Fatalf("replay frame %q must carry a gateway-owned request id", f)
		}
	}
	// The client's view is unchanged while the replay is in flight.
	if n := len(r.Active()); n != 2 {
		t.Fatalf("active during replay = %d, want 2", n)
	}
}

func TestSubscriptionRegistry_ReplayAckIsConsumedAndIDRemapped(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	frames := r.ReplayFrames()
	replayID := strings.TrimPrefix(string(frames[0]), "sub:")

	// The new supplier acks with a new subscription id.
	out, fwd := r.TranslateEndpointFrame([]byte("ok:" + replayID + ":new"))
	if fwd {
		t.Fatalf("a replay ack must not reach the client (got %q)", out)
	}
	// Data on the new id is delivered under the id the client knows.
	out, fwd = r.TranslateEndpointFrame([]byte("data:new"))
	if !fwd || string(out) != "data:old" {
		t.Fatalf("notification = %q forward=%v, want data:old forwarded", out, fwd)
	}
	// The client unsubscribes with its own id; the supplier must hear its id.
	if got := r.TranslateClientFrame([]byte("unsub:old")); string(got) != "unsub:new" {
		t.Fatalf("unsubscribe = %q, want unsub:new", got)
	}
	if r.HasActive() {
		t.Fatal("unsubscribe must still clear the subscription")
	}
}

func TestSubscriptionRegistry_ReplayErrorDropsSubscription(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	frames := r.ReplayFrames()
	replayID := strings.TrimPrefix(string(frames[0]), "sub:")
	if _, fwd := r.TranslateEndpointFrame([]byte("err:" + replayID)); fwd {
		t.Fatal("a failed replay ack must not reach the client either")
	}
	if r.HasActive() {
		t.Fatal("a subscription the new supplier refused is no longer live")
	}
}

func TestSubscriptionRegistry_SameIDAcrossSuppliersNeedsNoRewrite(t *testing.T) {
	// CometBFT-shaped: the replay is acked under the replay id, and events
	// then carry the replay id — which is exactly the remap case too.
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "7", "7")
	frames := r.ReplayFrames()
	replayID := strings.TrimPrefix(string(frames[0]), "sub:")
	r.TranslateEndpointFrame([]byte("ok:" + replayID + ":" + replayID))
	out, fwd := r.TranslateEndpointFrame([]byte("data:" + replayID))
	if !fwd || string(out) != "data:7" {
		t.Fatalf("event = %q forward=%v, want data:7", out, fwd)
	}
}

func TestSubscriptionRegistry_ReplayTwiceChainsRemaps(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	f1 := r.ReplayFrames()
	r.TranslateEndpointFrame([]byte("ok:" + strings.TrimPrefix(string(f1[0]), "sub:") + ":second"))
	f2 := r.ReplayFrames()
	r.TranslateEndpointFrame([]byte("ok:" + strings.TrimPrefix(string(f2[0]), "sub:") + ":third"))
	if out, _ := r.TranslateEndpointFrame([]byte("data:third")); string(out) != "data:old" {
		t.Fatalf("after two rebinds the client must still see its id, got %q", out)
	}
	if out, _ := r.TranslateEndpointFrame([]byte("data:second")); string(out) != "data:second" {
		t.Fatalf("a stale id from the previous supplier must not be remapped, got %q", out)
	}
}
