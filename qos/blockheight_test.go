package qos

import (
	"math"
	"testing"
)

func TestValidateBlockHeight_Zero(t *testing.T) {
	_, err := ValidateBlockHeight(0, 100, 5)
	if err == nil {
		t.Fatal("expected error for zero height")
	}
}

func TestValidateBlockHeight_Normal(t *testing.T) {
	h, err := ValidateBlockHeight(100, 105, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != 100 {
		t.Fatalf("expected 100, got %d", h)
	}
}

func TestValidateBlockHeight_TooHigh(t *testing.T) {
	_, err := ValidateBlockHeight(2_000_000, 100, 5)
	if err == nil {
		t.Fatal("expected error for height too far ahead")
	}
}

func TestValidateBlockHeight_ExactDelta(t *testing.T) {
	// Exactly at the delta boundary should pass.
	h, err := ValidateBlockHeight(100+maxBlockHeightDelta, 100, 5)
	if err != nil {
		t.Fatalf("unexpected error at exact delta: %v", err)
	}
	if h != 100+maxBlockHeightDelta {
		t.Fatalf("unexpected height: %d", h)
	}
}

func TestValidateBlockHeight_OneOverDelta(t *testing.T) {
	_, err := ValidateBlockHeight(100+maxBlockHeightDelta+1, 100, 5)
	if err == nil {
		t.Fatal("expected error for one over delta")
	}
}

func TestValidateBlockHeight_PerceivedZero(t *testing.T) {
	// Cold start: perceived is 0, any non-zero height should pass.
	h, err := ValidateBlockHeight(999999, 0, 5)
	if err != nil {
		t.Fatalf("unexpected error during cold start: %v", err)
	}
	if h != 999999 {
		t.Fatalf("expected 999999, got %d", h)
	}
}

func TestIsPlausibleBlockHeight(t *testing.T) {
	cases := []struct {
		height uint64
		want   bool
	}{
		{0, false},                           // "unknown", not a height
		{1, true},                            // genesis-ish
		{20_000_000, true},                   // ethereum
		{250_000_000, true},                  // solana slots
		{MaxPlausibleBlockHeight, true},      // the ceiling is inclusive
		{MaxPlausibleBlockHeight + 1, false}, // one over
		{math.MaxUint64, false},              // the value that used to wrap the cap
	}
	for _, tc := range cases {
		if got := IsPlausibleBlockHeight(tc.height); got != tc.want {
			t.Errorf("IsPlausibleBlockHeight(%d) = %v, want %v", tc.height, got, tc.want)
		}
	}
}

// ValidateBlockHeight compares against perceived+maxBlockHeightDelta, which
// wrapped when perceived sat near the top of the range — turning a ceiling into
// a floor and rejecting every honest height.
func TestValidateBlockHeight_NoWrapNearMaxUint64(t *testing.T) {
	perceived := uint64(math.MaxUint64 - 10)
	h, err := ValidateBlockHeight(perceived, perceived, 5)
	if err != nil {
		t.Fatalf("a height equal to perceived must validate: %v", err)
	}
	if h != perceived {
		t.Errorf("height = %d, want %d", h, perceived)
	}
}

func TestSaturatingAdd(t *testing.T) {
	cases := []struct{ a, b, want uint64 }{
		{1, 2, 3},
		{0, 0, 0},
		{math.MaxUint64, 1, math.MaxUint64},
		{math.MaxUint64 - 5, 10, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64},
	}
	for _, tc := range cases {
		if got := saturatingAdd(tc.a, tc.b); got != tc.want {
			t.Errorf("saturatingAdd(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestSaturatingMul(t *testing.T) {
	cases := []struct{ a, b, want uint64 }{
		{3, 4, 12},
		{0, math.MaxUint64, 0},
		{math.MaxUint64, 0, 0},
		{math.MaxUint64, 2, math.MaxUint64},
		{math.MaxUint64/3 + 1, 3, math.MaxUint64},
	}
	for _, tc := range cases {
		if got := saturatingMul(tc.a, tc.b); got != tc.want {
			t.Errorf("saturatingMul(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
