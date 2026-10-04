package qos

import (
	"testing"
	"time"

	"github.com/pokt-network/sage/domain"
)

// WS-3. A rebind's replay frames reach the new supplier through
// ProcessClientMessage, which runs them through TranslateClientFrame like any
// client frame (websockets.Bridge.rebind). That parks each one in pending under
// its gateway-owned id; the ack is then consumed by the replay table and the
// pending entry is never removed. Every rebind leaks one per subscription, and
// at the cap new subscribes go untracked and unsolicited grading switches off.
func TestAudit2_ReplayThroughClientPathLeavesNoPending(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	frames := r.Replay().Frames
	if len(frames) != 1 {
		t.Fatalf("precondition: ReplayFrames = %d frames, want 1", len(frames))
	}
	r.TranslateClientFrame(frames[0])
	if _, fwd, _ := r.TranslateEndpointFrame([]byte("ok:" + replayID(frames[0]) + ":new")); fwd {
		t.Fatal("precondition: a replay ack must be consumed")
	}

	r.mu.Lock()
	n, entries := r.npending, len(r.pending)
	r.mu.Unlock()
	if n != 0 || entries != 0 {
		t.Fatalf("after a replay acked by the new supplier: npending=%d, pending ids=%d; want 0 and 0", n, entries)
	}
}

// WS-4. The client unsubscribes while a replay is in flight. The unsubscribe
// goes out under the old supplier's id (the new one has none yet), so the new
// supplier never hears it; completeReplay then finds the subscription gone and
// returns, leaving a live subscription on the new supplier nobody tracks. Its
// notifications reach a client that asked for them to stop.
func TestAudit2_UnsubscribeDuringReplayDoesNotOrphan(t *testing.T) {
	r := NewSubscriptionRegistry(spanClassifier{})
	establish(t, r, "1", "old")
	frames := r.Replay().Frames
	if len(frames) != 1 {
		t.Fatalf("precondition: ReplayFrames = %d frames, want 1", len(frames))
	}
	r.TranslateClientFrame(frames[0]) // as Bridge.rebind sends it
	r.TranslateClientFrame([]byte("unsub:old"))
	r.TranslateEndpointFrame([]byte("ok:" + replayID(frames[0]) + ":new"))

	out, fwd, note := r.TranslateEndpointFrame([]byte("data:new"))
	if fwd {
		t.Fatalf("notification %q for the subscription the client closed mid-replay was forwarded (grade %v); want it dropped",
			out, note.Kind)
	}
}

// HC-4. A booting replica replays minutes of probe results within
// milliseconds. Every AddObservation is stamped now, so twenty eth blocks
// (four minutes of chain time) read as twenty blocks in a millisecond.
func TestAudit2_ReplayBurstDoesNotInflateBlockRate(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	for h := uint64(1000); h <= 1020; h++ {
		bc.AddObservation(domain.EndpointAddr("s1-https://a.example.com"), h)
	}
	if rate, ok := bc.BlockRate(); ok && rate > 100 {
		t.Fatalf("block rate after a replay burst of 21 eth heights = %.0f blocks/s; want unknown or <= 100 (eth is ~0.083)", rate)
	}
}

// HC-4. The same burst, then an answer exactly at the perceived head 11 s
// after the head last moved — under one eth block. At the burst's rate the
// expected head runs away and the honest answer reads stale: a major penalty
// and a retry for a supplier that was right.
func TestAudit2_HonestHeadNotStaleAfterReplayBurst(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	for h := uint64(1000); h <= 1020; h++ {
		bc.AddObservation(domain.EndpointAddr("s1-https://a.example.com"), h)
	}
	lag, stale, ok := bc.AnswerLag(1020, time.Now().Add(11*time.Second))
	if !ok {
		t.Fatal("precondition: there is a head to compare with")
	}
	if stale {
		t.Fatalf("an answer at the perceived head, 11s after it moved, graded stale (lag=%d) after a replay burst", lag)
	}
}

// HC-5. AnswerLag takes the time the answer was produced (gradeHead passes
// ProbedAt+latency) but compares it with the CURRENT head without projecting
// back: an answer that named the head four minutes ago reads 240 blocks
// behind. A replayed or peer result is graded exactly that way.
func TestAudit2_AnswerLagProjectsBackToPastAt(t *testing.T) {
	bc := NewBlockConsensus(nil, 5)
	bc.AddObservation(domain.EndpointAddr("s1-https://a.example.com"), 1100)
	now := time.Now()
	bc.mu.Lock()
	bc.rateSamples = []rateSample{{height: 1000, at: now.Add(-100 * time.Second)}, {height: 1100, at: now}} // 1 block/s
	bc.mu.Unlock()

	answeredAt := now.Add(-240 * time.Second)
	headThen := uint64(1100 - 240)
	lag, stale, ok := bc.AnswerLag(headThen, answeredAt)
	if !ok {
		t.Fatal("precondition: there is a head to compare with")
	}
	if stale {
		t.Fatalf("an answer at the head when it was given (240s ago, 1 block/s) graded stale: lag=%d", lag)
	}
}
