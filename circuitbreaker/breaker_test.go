package circuitbreaker

import (
	"testing"
	"time"
)

func TestBreaker_MarkBroken_And_IsBroken(t *testing.T) {
	b := New()

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

func TestBreaker_EscalatingTTL_Integration(t *testing.T) {
	b := New()

	// Mark broken multiple times and verify hit count increments.
	b.MarkBroken("eth", "example.com", "reason 1")
	b.mu.RLock()
	state1 := b.broken["eth"]["example.com"]
	b.mu.RUnlock()
	if state1.HitCount != 1 {
		t.Errorf("after first mark: HitCount = %d, want 1", state1.HitCount)
	}

	b.MarkBroken("eth", "example.com", "reason 2")
	b.mu.RLock()
	state2 := b.broken["eth"]["example.com"]
	b.mu.RUnlock()
	if state2.HitCount != 2 {
		t.Errorf("after second mark: HitCount = %d, want 2", state2.HitCount)
	}

	// Verify the expiry is further out for the second mark.
	if !state2.Expiry.After(state1.Expiry) {
		t.Error("second mark should have a later expiry than first")
	}
}

func TestBreaker_Clear(t *testing.T) {
	b := New()

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
	b := New()

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
	b := New()

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
