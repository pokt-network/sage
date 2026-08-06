package circuitbreaker

import (
	"testing"
	"time"
)

// firstErrorBreaker disables the failure-rate gate so a single MarkBroken
// trips the domain. Used by tests that exercise the break/expiry/clear
// mechanics rather than the gate itself; the gate has its own tests below.
func firstErrorBreaker() *Breaker {
	return New(WithFailureRateGate(time.Minute, 1, 0))
}

func TestBreaker_MarkBroken_And_IsBroken(t *testing.T) {
	b := firstErrorBreaker()

	// Initially not broken.
	if b.IsBroken("eth", "example.com") {
		t.Error("should not be broken initially")
	}

	// Mark broken.
	b.MarkBroken("eth", "example.com", "test reason")

	if !b.IsBroken("eth", "example.com") {
		t.Error("should be broken after MarkBroken")
	}

	// Different domain not affected.
	if b.IsBroken("eth", "other.com") {
		t.Error("different domain should not be broken")
	}

	// Different service not affected.
	if b.IsBroken("poly", "example.com") {
		t.Error("different service should not be broken")
	}
}

func TestBreaker_Expiry(t *testing.T) {
	b := New()

	// Manually set an expired state.
	b.mu.Lock()
	b.broken["eth"] = map[string]BrokenState{
		"example.com": {
			Expiry:   time.Now().Add(-1 * time.Second),
			HitCount: 1,
			Reason:   "expired",
		},
	}
	b.mu.Unlock()

	if b.IsBroken("eth", "example.com") {
		t.Error("expired entry should not be considered broken")
	}
}

func TestBreaker_EscalatingTTL(t *testing.T) {
	b := New()

	// First mark: 1m TTL.
	ttl0 := b.escalateTTL(0)
	if ttl0 != 1*time.Minute {
		t.Errorf("hit 0: TTL = %v, want 1m", ttl0)
	}

	// Second mark: 2m TTL.
	ttl1 := b.escalateTTL(1)
	if ttl1 != 2*time.Minute {
		t.Errorf("hit 1: TTL = %v, want 2m", ttl1)
	}

	// Third: 4m.
	ttl2 := b.escalateTTL(2)
	if ttl2 != 4*time.Minute {
		t.Errorf("hit 2: TTL = %v, want 4m", ttl2)
	}

	// Fourth: 8m.
	ttl3 := b.escalateTTL(3)
	if ttl3 != 8*time.Minute {
		t.Errorf("hit 3: TTL = %v, want 8m", ttl3)
	}

	// Fifth: 16m.
	ttl4 := b.escalateTTL(4)
	if ttl4 != 16*time.Minute {
		t.Errorf("hit 4: TTL = %v, want 16m", ttl4)
	}

	// Sixth: 30m cap.
	ttl5 := b.escalateTTL(5)
	if ttl5 != 30*time.Minute {
		t.Errorf("hit 5: TTL = %v, want 30m (cap)", ttl5)
	}

	// Beyond cap stays at 30m.
	ttl10 := b.escalateTTL(10)
	if ttl10 != 30*time.Minute {
		t.Errorf("hit 10: TTL = %v, want 30m (cap)", ttl10)
	}
}

// Escalation means "broke AGAIN after we let it back in", not "was marked
// twice during one incident".
func TestBreaker_EscalatingTTL_Integration(t *testing.T) {
	b := firstErrorBreaker()

	if !b.MarkBroken("eth", "example.com", "reason 1") {
		t.Fatal("first mark should break the domain")
	}
	b.mu.RLock()
	state1 := b.broken["eth"]["example.com"]
	b.mu.RUnlock()
	if state1.HitCount != 1 {
		t.Errorf("after first mark: HitCount = %d, want 1", state1.HitCount)
	}

	// Let it back in, then break it again — that is a second episode.
	expireBreak(t, b, "eth", "example.com")
	if !b.MarkBroken("eth", "example.com", "reason 2") {
		t.Fatal("second episode should break the domain again")
	}
	b.mu.RLock()
	state2 := b.broken["eth"]["example.com"]
	b.mu.RUnlock()
	if state2.HitCount != 2 {
		t.Errorf("after second episode: HitCount = %d, want 2", state2.HitCount)
	}

	// Verify the expiry is further out for the second episode.
	if !state2.Expiry.After(state1.Expiry) {
		t.Error("second episode should have a later expiry than the first")
	}
}

// One incident produces many failures — batch sub-relays and hedge arms all
// fail at once. They belong to the episode already in effect and must not
// escalate its TTL or extend its expiry.
func TestBreaker_DuplicateMarksWithinEpisodeDoNotEscalate(t *testing.T) {
	b := firstErrorBreaker()

	if !b.MarkBroken("eth", "example.com", "first") {
		t.Fatal("first mark should break the domain")
	}
	b.mu.RLock()
	first := b.broken["eth"]["example.com"]
	b.mu.RUnlock()

	for i := 0; i < 50; i++ {
		if b.MarkBroken("eth", "example.com", "concurrent burst") {
			t.Fatal("a mark during an active break must report no new break")
		}
	}

	b.mu.RLock()
	after := b.broken["eth"]["example.com"]
	b.mu.RUnlock()
	if after.HitCount != 1 {
		t.Errorf("HitCount = %d after 50 duplicate marks, want 1", after.HitCount)
	}
	if !after.Expiry.Equal(first.Expiry) {
		t.Error("duplicate marks must not extend the expiry")
	}
}

// expireBreak backdates a domain's break so it reads as expired, simulating the
// TTL elapsing without waiting for it.
func expireBreak(t *testing.T, b *Breaker, serviceID, dom string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.broken[serviceID][dom]
	if !ok {
		t.Fatalf("%s/%s is not broken", serviceID, dom)
	}
	state.Expiry = time.Now().Add(-time.Second)
	b.broken[serviceID][dom] = state
}

func TestBreaker_Clear(t *testing.T) {
	b := firstErrorBreaker()

	b.MarkBroken("eth", "example.com", "reason 1")
	b.MarkBroken("eth", "other.com", "reason 2")
	b.MarkBroken("poly", "example.com", "reason 3")

	count := b.Clear("eth")
	if count != 2 {
		t.Errorf("Clear returned %d, want 2", count)
	}

	if b.IsBroken("eth", "example.com") {
		t.Error("eth/example.com should not be broken after Clear")
	}
	if b.IsBroken("eth", "other.com") {
		t.Error("eth/other.com should not be broken after Clear")
	}

	// poly not affected.
	if !b.IsBroken("poly", "example.com") {
		t.Error("poly/example.com should still be broken")
	}
}

func TestBreaker_Clear_EmptyService(t *testing.T) {
	b := New()
	count := b.Clear("nonexistent")
	if count != 0 {
		t.Errorf("Clear of nonexistent service returned %d, want 0", count)
	}
}

func TestBreaker_GetBroken(t *testing.T) {
	b := firstErrorBreaker()

	b.MarkBroken("eth", "a.com", "reason a")
	b.MarkBroken("eth", "b.com", "reason b")

	// Add an expired one manually.
	b.mu.Lock()
	b.broken["eth"]["expired.com"] = BrokenState{
		Expiry:   time.Now().Add(-1 * time.Second),
		HitCount: 1,
		Reason:   "old",
	}
	b.mu.Unlock()

	result := b.GetBroken("eth")
	if len(result) != 2 {
		t.Errorf("GetBroken returned %d entries, want 2", len(result))
	}
	if _, ok := result["a.com"]; !ok {
		t.Error("a.com missing from GetBroken")
	}
	if _, ok := result["b.com"]; !ok {
		t.Error("b.com missing from GetBroken")
	}
	if _, ok := result["expired.com"]; ok {
		t.Error("expired.com should not appear in GetBroken")
	}
}

func TestBreaker_GetBroken_EmptyService(t *testing.T) {
	b := New()
	result := b.GetBroken("nonexistent")
	if result != nil {
		t.Errorf("GetBroken of nonexistent service returned %v, want nil", result)
	}
}

func TestBreaker_LocalOnlyMode(t *testing.T) {
	// Without Redis, should still work.
	b := firstErrorBreaker()

	b.MarkBroken("eth", "example.com", "test")
	if !b.IsBroken("eth", "example.com") {
		t.Error("should be broken in local-only mode")
	}

	b.Clear("eth")
	if b.IsBroken("eth", "example.com") {
		t.Error("should not be broken after clear in local-only mode")
	}
}

func TestBrokenState_IsExpired(t *testing.T) {
	expired := BrokenState{Expiry: time.Now().Add(-1 * time.Second)}
	if !expired.IsExpired() {
		t.Error("should be expired")
	}

	active := BrokenState{Expiry: time.Now().Add(1 * time.Minute)}
	if active.IsExpired() {
		t.Error("should not be expired")
	}
}

func TestBreaker_Options(t *testing.T) {
	b := New(
		WithKeyPrefix("custom:prefix:"),
		WithCacheTTL(10*time.Second),
	)

	if b.keyPrefix != "custom:prefix:" {
		t.Errorf("keyPrefix = %q, want custom:prefix:", b.keyPrefix)
	}
	if b.cacheTTL != 10*time.Second {
		t.Errorf("cacheTTL = %v, want 10s", b.cacheTTL)
	}
}

// --- failure-rate gate --- //

// The bug this replaces: one failed relay removed an entire hostname, and every
// endpoint behind it, from the pool.
func TestBreaker_SingleFailureDoesNotBreak(t *testing.T) {
	b := New()

	if b.MarkBroken("eth", "example.com", "one bad relay") {
		t.Error("a single failure must not break a domain")
	}
	if b.IsBroken("eth", "example.com") {
		t.Error("domain must stay in the pool after one failure")
	}
}

// minFailures is the floor: below it, no rate is trusted, however bad it looks.
func TestBreaker_BreaksOnlyAfterMinFailures(t *testing.T) {
	b := New() // minFailures 5, threshold 0.20

	for i := 1; i < defaultMinFailures; i++ {
		if b.MarkBroken("eth", "example.com", "failing") {
			t.Fatalf("broke after %d failures, want at least %d", i, defaultMinFailures)
		}
	}
	if !b.MarkBroken("eth", "example.com", "failing") {
		t.Fatalf("should break on failure %d", defaultMinFailures)
	}
	if !b.IsBroken("eth", "example.com") {
		t.Error("domain should be broken once the gate passes")
	}
}

// A high-volume domain with a low error rate is never removed, no matter how
// many raw failures its volume produces. This is the production case: an
// operator sustaining >99% success used to be broken repeatedly purely because
// it served the most traffic and so hit its first error soonest.
func TestBreaker_HighVolumeLowRateNeverBreaks(t *testing.T) {
	b := New()

	// 1% failure rate over a large sample — 20 failures, far past minFailures.
	for i := 0; i < 2000; i++ {
		if i%100 == 0 {
			if b.MarkBroken("eth", "busy.example.com", "occasional error") {
				t.Fatalf("broke a 1%%-failure-rate domain at iteration %d", i)
			}
			continue
		}
		b.RecordSuccess("eth", "busy.example.com")
	}
	if b.IsBroken("eth", "busy.example.com") {
		t.Error("a domain failing 1% of the time must stay in the pool")
	}
}

// Successes are the denominator; without them any failure reads as 100%.
func TestBreaker_SuccessesDiluteTheRate(t *testing.T) {
	b := New()

	// 5 failures against 45 successes = 10%, under the 20% threshold.
	for i := 0; i < 45; i++ {
		b.RecordSuccess("eth", "example.com")
	}
	for i := 0; i < defaultMinFailures; i++ {
		if b.MarkBroken("eth", "example.com", "failing") {
			t.Fatal("10% failure rate must not break the domain")
		}
	}

	// Push the rate over the threshold with more failures.
	broke := false
	for i := 0; i < 20 && !broke; i++ {
		broke = b.MarkBroken("eth", "example.com", "failing")
	}
	if !broke {
		t.Error("a sustained failure rate above the threshold should break the domain")
	}
}

// The window is a sliding one: failures spread thinly over time never
// accumulate into a break.
func TestBreaker_WindowRollResetsCounts(t *testing.T) {
	b := New(WithFailureRateGate(20*time.Millisecond, defaultMinFailures, defaultFailureRateThreshold))

	for i := 0; i < 10; i++ {
		if b.MarkBroken("eth", "example.com", "sparse failure") {
			t.Fatalf("failures in separate windows must not accumulate (iteration %d)", i)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if b.IsBroken("eth", "example.com") {
		t.Error("sparse failures should never break a domain")
	}
}

// Clear must drop the gate history too, or an operator undoing a false-positive
// lockout gets the domain re-broken on the next few failures and escalated as a
// repeat offender.
func TestBreaker_ClearResetsRateGate(t *testing.T) {
	b := firstErrorBreaker()

	b.MarkBroken("eth", "example.com", "first episode")
	b.Clear("eth")

	if !b.MarkBroken("eth", "example.com", "after clear") {
		t.Fatal("should break again after clear")
	}
	b.mu.RLock()
	state := b.broken["eth"]["example.com"]
	b.mu.RUnlock()
	if state.HitCount != 1 {
		t.Errorf("HitCount = %d after Clear, want 1 (history dropped)", state.HitCount)
	}
}
