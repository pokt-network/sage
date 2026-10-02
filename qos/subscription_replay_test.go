package qos

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// spanClassifier is fakeClassifier with spans and JSON subscribes: a
// subscribe is {"id":<id>}, so the registry finds and rewrites its request
// id the way it does for every real dialect; the other frames are
// "unsub:<subid>" / "ok:<req>:<sub>" / "data:<sub>", and the span of the id
// is where it sits in the text, so a rewrite can be checked by reading the
// frame back.
type spanClassifier struct{}

func (spanClassifier) ClassifyClientFrame(data []byte) ClientFrameInfo {
	if id := gjson.GetBytes(data, "id"); id.Exists() {
		return ClientFrameInfo{Action: SubscriptionSubscribe, RequestID: id.Raw, Method: "subscribe", Topic: "heads"}
	}
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

// subFrame is a spanClassifier subscribe with the given raw request id.
func subFrame(id string) []byte { return []byte(`{"id":` + id + `}`) }

// replayID is the raw request id a replay frame carries.
func replayID(frame []byte) string { return gjson.GetBytes(frame, "id").Raw }

func establish(t *testing.T, r *SubscriptionRegistry, req, sub string) {
	t.Helper()
	r.TranslateClientFrame(subFrame(req))
	if _, fwd, _ := r.TranslateEndpointFrame([]byte("ok:" + req + ":" + sub)); !fwd {
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
		if !strings.HasPrefix(replayID(f), `"sage-replay-`) {
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
	id := replayID(frames[0])

	// The new supplier acks with a new subscription id.
	out, fwd, _ := r.TranslateEndpointFrame([]byte("ok:" + id + ":new"))
	if fwd {
		t.Fatalf("a replay ack must not reach the client (got %q)", out)
	}
	// Data on the new id is delivered under the id the client knows.
	out, fwd, _ = r.TranslateEndpointFrame([]byte("data:new"))
	if !fwd || string(out) != "data:old" {
		t.Fatalf("notification = %q forward=%v, want data:old forwarded", out, fwd)
	}
	// The client unsubscribes with its own id; the supplier must hear its id.
	if got := r.TranslateClientFrame([]byte("unsub:old")); string(got) != "unsub:new" {
		t.Fatalf("unsubscribe = %q, want unsub:new", got)
	}
	if len(r.Active()) != 0 {
		t.Fatal("unsubscribe must still clear the subscription")
	}
}

func TestSubscriptionRegistry_ReplayErrorDropsSubscription(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	frames := r.ReplayFrames()
	id := replayID(frames[0])
	if _, fwd, _ := r.TranslateEndpointFrame([]byte("err:" + id)); fwd {
		t.Fatal("a failed replay ack must not reach the client either")
	}
	if len(r.Active()) != 0 {
		t.Fatal("a subscription the new supplier refused is no longer live")
	}
}

func TestSubscriptionRegistry_SameIDAcrossSuppliersNeedsNoRewrite(t *testing.T) {
	// CometBFT-shaped: the replay is acked under the replay id, and events
	// then carry the replay id — which is exactly the remap case too.
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "7", "7")
	frames := r.ReplayFrames()
	id := replayID(frames[0])
	r.TranslateEndpointFrame([]byte("ok:" + id + ":" + id))
	out, fwd, _ := r.TranslateEndpointFrame([]byte("data:" + id))
	if !fwd || string(out) != "data:7" {
		t.Fatalf("event = %q forward=%v, want data:7", out, fwd)
	}
}

func TestSubscriptionRegistry_ReplayTwiceChainsRemaps(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	f1 := r.ReplayFrames()
	r.TranslateEndpointFrame([]byte("ok:" + replayID(f1[0]) + ":second"))
	f2 := r.ReplayFrames()
	r.TranslateEndpointFrame([]byte("ok:" + replayID(f2[0]) + ":third"))
	if out, _, _ := r.TranslateEndpointFrame([]byte("data:third")); string(out) != "data:old" {
		t.Fatalf("after two rebinds the client must still see its id, got %q", out)
	}
	if out, _, _ := r.TranslateEndpointFrame([]byte("data:second")); string(out) != "data:second" {
		t.Fatalf("a stale id from the previous supplier must not be remapped, got %q", out)
	}
}
